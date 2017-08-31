/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.group;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.hadoop.metrics.util.MBeanUtil;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Service to support Region Server Grouping (HBase-6721)
 */
@InterfaceAudience.Private
public class GroupAdminServer extends GroupAdmin {
  private static final Log LOG = LogFactory.getLog(GroupAdminServer.class);

  private MasterServices master;
  //List of servers that are being moved from one group to another
  //Key=host:port,Value=targetGroup
  private ConcurrentMap<HostAndPort,String> serversInTransition =
      new ConcurrentHashMap<HostAndPort, String>();
  private GroupInfoManager groupInfoManager;

  public GroupAdminServer(MasterServices master,
                          GroupInfoManager groupInfoManager) throws IOException {
    this.master = master;
    this.groupInfoManager = groupInfoManager;
    registerMBean();
  }

  @Override
  public GroupInfo getGroupInfo(String groupName) throws IOException {
    return getGroupInfoManager().getGroup(groupName);
  }


  @Override
  public GroupInfo getGroupInfoOfTable(TableName tableName) throws IOException {
    String groupName = getGroupInfoManager().getGroupOfTable(tableName);
    if (groupName == null) {
      if(master.getTableDescriptors().get(tableName) == null) {
        throw new ConstraintException("Table "+tableName+" does not exist");
      }
      throw new ConstraintException("Table "+tableName+" has no group");
    }
    return getGroupInfoManager().getGroup(groupName);
  }

  @Override
  public void moveServers(Set<HostAndPort> servers, String targetGroupName)
      throws IOException {
    if (servers == null) {
      throw new DoNotRetryIOException(
          "The list of servers cannot be null.");
    }
    if (StringUtils.isEmpty(targetGroupName)) {
      throw new DoNotRetryIOException("The target group cannot be null.");
    }
    if(servers.size() < 1) {
      return;
    }

    GroupInfo targetGrp = getGroupInfo(targetGroupName);
    GroupInfoManager manager = getGroupInfoManager();
    synchronized (manager) {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preMoveServers(servers, targetGroupName);
      }
      //we only allow a move from a single source group
      //so this should be ok
      GroupInfo srcGrp = manager.getGroupOfServer(servers.iterator().next());
      //only move online servers (from default)
      //or servers from other groups
      //this prevents bogus servers from entering groups
      if(GroupInfo.DEFAULT_GROUP.equals(srcGrp.getName())) {
        Set<HostAndPort> onlineServers = new HashSet<HostAndPort>();
        for(ServerName server: master.getServerManager().getOnlineServers().keySet()) {
          onlineServers.add(server.getHostPort());
        }
        for(HostAndPort el: servers) {
          if(!onlineServers.contains(el)) {
            throw new DoNotRetryIOException(
                "Server "+el+" is not an online server in default group.");
          }
        }
      }

      if(srcGrp.getServers().size() <= servers.size() &&
          srcGrp.getTables().size() > 0) {
        throw new DoNotRetryIOException("Cannot leave a group "+srcGrp.getName()+
            " that contains tables " +"without servers.");
      }

      String sourceGroupName =
          getGroupInfoManager().getGroupOfServer(srcGrp.getServers().iterator().next()).getName();
      if(getGroupInfo(targetGroupName) == null) {
        throw new ConstraintException("Target group does not exist: "+targetGroupName);
      }

      for(HostAndPort server: servers) {
        if (serversInTransition.containsKey(server)) {
          throw new DoNotRetryIOException(
              "Server list contains a server that is already being moved: "+server);
        }
        String tmpGroup = getGroupInfoManager().getGroupOfServer(server).getName();
        if (sourceGroupName != null && !tmpGroup.equals(sourceGroupName)) {
          throw new DoNotRetryIOException(
              "Move server request should only come from one source group. "+
              "Expecting only "+sourceGroupName+" but contains "+tmpGroup);
        }
      }

      if(sourceGroupName.equals(targetGroupName)) {
        throw new ConstraintException(
            "Target group is the same as source group: "+targetGroupName);
      }

      try {
        //update the servers as in transition
        for (HostAndPort server : servers) {
          serversInTransition.put(server, targetGroupName);
        }

        getGroupInfoManager().moveServers(servers, sourceGroupName, targetGroupName);
        boolean found;
        List<HostAndPort> tmpServers = Lists.newArrayList(servers);
        do {
          found = false;
          for (Iterator<HostAndPort> iter = tmpServers.iterator();
               iter.hasNext(); ) {
            HostAndPort rs = iter.next();
            //get online regions
            List<HRegionInfo> regions = new LinkedList<HRegionInfo>();
            for (Map.Entry<HRegionInfo, ServerName> el :
                master.getAssignmentManager().getRegionStates().getRegionAssignments().entrySet()) {
              if (el.getValue().getHostPort().equals(rs)) {
                regions.add(el.getKey());
              }
            }
            for (RegionState state :
                master.getAssignmentManager().getRegionStates().getRegionsInTransition().values()) {
              if (state.getServerName().getHostPort().equals(rs)) {
                regions.add(state.getRegion());
              }
            }

            //unassign regions for a server
            LOG.info("Unassigning " + regions.size() +
                " regions from server " + rs + " for move to " + targetGroupName);
            if (regions.size() > 0) {
              //TODO bulk unassign or throttled unassign?
              for (HRegionInfo region : regions) {
                //regions might get assigned from tables of target group
                //so we need to filter
                if (!targetGrp.containsTable(region.getTable())) {
                  master.getAssignmentManager().unassign(region);
                  found = true;
                }
              }
            }
            if (!found) {
              iter.remove();
            }
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            LOG.warn("Sleep interrupted", e);
            Thread.currentThread().interrupt();
          }
        } while (found);
      } finally {
        //remove from transition
        for (HostAndPort server : servers) {
          serversInTransition.remove(server);
        }
      }
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postMoveServers(servers, targetGroupName);
      }
      LOG.info("Move server done: "+sourceGroupName+"->"+targetGroupName);
    }
  }

  @Override
  public void moveTables(Set<TableName> tables, String targetGroup) throws IOException {
    if (tables == null) {
      throw new ConstraintException(
          "The list of servers cannot be null.");
    }
    if(tables.size() < 1) {
      LOG.debug("moveTables() passed an empty set. Ignoring.");
      return;
    }
    GroupInfoManager manager = getGroupInfoManager();
    synchronized (manager) {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preMoveTables(tables, targetGroup);
      }

      if(targetGroup != null) {
        GroupInfo destGroup = manager.getGroup(targetGroup);
        if(destGroup == null) {
          throw new ConstraintException("Target group does not exist: "+targetGroup);
        }
        if(destGroup.getServers().size() < 1) {
          throw new ConstraintException("Target group must have at least one server.");
        }
      }

      for(TableName table : tables) {
        String srcGroup = manager.getGroupOfTable(table);
        if(srcGroup != null && srcGroup.equals(targetGroup)) {
          throw new ConstraintException("Source group is the same as target group for table "+table+" :"+srcGroup);
        }
      }
      manager.moveTables(tables, targetGroup);
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postMoveTables(tables, targetGroup);
      }
    }
    for(TableName table: tables) {
      TableLock lock = master.getTableLockManager().writeLock(table, "Group: table move");
      try {
        lock.acquire();
        for (HRegionInfo region :
            master.getAssignmentManager().getRegionStates().getRegionsOfTable(table)) {
          master.getAssignmentManager().unassign(region);
        }
      } finally {
        lock.release();
      }
    }
  }

  @Override
  public void addGroup(String name) throws IOException {
    if (master.getMasterCoprocessorHost() != null) {
      master.getMasterCoprocessorHost().preAddGroup(name);
    }
    getGroupInfoManager().addGroup(new GroupInfo(name));
    if (master.getMasterCoprocessorHost() != null) {
      master.getMasterCoprocessorHost().postAddGroup(name);
    }
  }

  @Override
  public void removeGroup(String name) throws IOException {
    GroupInfoManager manager = getGroupInfoManager();
    synchronized (manager) {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preRemoveGroup(name);
      }
      GroupInfo groupInfo = getGroupInfoManager().getGroup(name);
      if(groupInfo == null) {
        throw new DoNotRetryIOException("Group "+name+" does not exist");
      }
      int tableCount = groupInfo.getTables().size();
      if (tableCount > 0) {
        throw new DoNotRetryIOException("Group "+name+" must have no associated tables: "+tableCount);
      }
      int serverCount = groupInfo.getServers().size();
      if(serverCount > 0) {
        throw new DoNotRetryIOException("Group "+name+" must have no associated servers: "+serverCount);
      }
      for(NamespaceDescriptor ns: master.listNamespaceDescriptors()) {
        String nsGroup = ns.getConfigurationValue(GroupInfo.NAMESPACEDESC_PROP_GROUP);
        if(nsGroup != null &&  nsGroup.equals(name)) {
          throw new DoNotRetryIOException("Group "+name+" is referenced by namespace: "+ns.getName());
        }
      }
      manager.removeGroup(name);
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postRemoveGroup(name);
      }
    }
  }

  @Override
  public boolean balanceGroup(String groupName) throws IOException {
    ServerManager serverManager = master.getServerManager();
    AssignmentManager assignmentManager = master.getAssignmentManager();
    LoadBalancer balancer = master.getLoadBalancer();

    boolean balancerRan;
    synchronized (balancer) {
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().preBalanceGroup(groupName);
      }
      // Only allow one balance run at at time.
      Map<String, RegionState> groupRIT = groupGetRegionsInTransition(groupName);
      if (groupRIT.size() > 0) {
        LOG.debug("Not running balancer because " +
          groupRIT.size() +
          " region(s) in transition: " +
          StringUtils.abbreviate(
              master.getAssignmentManager().getRegionStates().getRegionsInTransition().toString(),
              256));
        return false;
      }
      if (serverManager.areDeadServersInProgress()) {
        LOG.debug("Not running balancer because processing dead regionserver(s): " +
            serverManager.getDeadServers());
        return false;
      }

      //We balance per group instead of per table
      List<RegionPlan> plans = new ArrayList<RegionPlan>();
      for(Map.Entry<TableName, Map<ServerName, List<HRegionInfo>>> tableMap:
          getGroupAssignmentsByTable(groupName).entrySet()) {
        LOG.info("Creating partial plan for table "+tableMap.getKey()+": "+tableMap.getValue());
        List<RegionPlan> partialPlans = balancer.balanceCluster(tableMap.getValue());
        LOG.info("Partial plan for table "+tableMap.getKey()+": "+partialPlans);
        if (partialPlans != null) {
          plans.addAll(partialPlans);
        }
      }
      long startTime = System.currentTimeMillis();
      balancerRan = plans != null;
      if (plans != null && !plans.isEmpty()) {
        LOG.info("Group balance "+groupName+" starting with plan count: "+plans.size());
        for (RegionPlan plan: plans) {
          LOG.info("balance " + plan);
          assignmentManager.balance(plan);
        }
        LOG.info("Group balance "+groupName+" completed after "+(System.currentTimeMillis()-startTime)+" seconds");
      }
      if (master.getMasterCoprocessorHost() != null) {
        master.getMasterCoprocessorHost().postBalanceGroup(groupName, balancerRan);
      }
    }
    return balancerRan;
  }

  @Override
  public List<GroupInfo> listGroups() throws IOException {
    return getGroupInfoManager().listGroups();
  }

  @Override
  public GroupInfo getGroupOfServer(HostAndPort hostPort) throws IOException {
    return getGroupInfoManager().getGroupOfServer(hostPort);
  }

  @InterfaceAudience.Private
  public GroupInfoManager getGroupInfoManager() throws IOException {
    return groupInfoManager;
  }

  private Map<String, RegionState> groupGetRegionsInTransition(String groupName)
      throws IOException {
    Map<String, RegionState> rit = Maps.newTreeMap();
    AssignmentManager am = master.getAssignmentManager();
    GroupInfo groupInfo = getGroupInfo(groupName);
    for(TableName tableName : groupInfo.getTables()) {
      for(HRegionInfo regionInfo: am.getRegionStates().getRegionsOfTable(tableName)) {
        RegionState state =
            master.getAssignmentManager().getRegionStates().getRegionTransitionState(regionInfo);
        if(state != null) {
          rit.put(regionInfo.getEncodedName(), state);
        }
      }
    }
    return rit;
  }

  private Map<TableName, Map<ServerName, List<HRegionInfo>>>
      getGroupAssignmentsByTable(String groupName) throws IOException {
    Map<TableName, Map<ServerName, List<HRegionInfo>>> result = Maps.newHashMap();
    GroupInfo groupInfo = getGroupInfo(groupName);
    Map<TableName, Map<ServerName, List<HRegionInfo>>> assignments = Maps.newHashMap();
    for(Map.Entry<HRegionInfo, ServerName> entry:
        master.getAssignmentManager().getRegionStates().getRegionAssignments().entrySet()) {
      TableName currTable = entry.getKey().getTable();
      ServerName currServer = entry.getValue();
      HRegionInfo currRegion = entry.getKey();
      if(groupInfo.getTables().contains(currTable)) {
        if(!assignments.containsKey(entry.getKey().getTable())) {
          assignments.put(currTable, new HashMap<ServerName, List<HRegionInfo>>());
        }
        if(!assignments.get(currTable).containsKey(currServer)) {
          assignments.get(currTable).put(currServer, new ArrayList<HRegionInfo>());
        }
        assignments.get(currTable).get(currServer).add(currRegion);
      }
    }

    Map<ServerName, List<HRegionInfo>> serverMap = Maps.newHashMap();
    for(ServerName serverName: master.getServerManager().getOnlineServers().keySet()) {
      if(groupInfo.getServers().contains(serverName.getHostPort())) {
        serverMap.put(serverName, Collections.EMPTY_LIST);
      }
    }

    //add all tables that are members of the group
    for(TableName tableName : groupInfo.getTables()) {
      if(assignments.containsKey(tableName)) {
        result.put(tableName, new HashMap<ServerName, List<HRegionInfo>>());
        result.get(tableName).putAll(serverMap);
        result.get(tableName).putAll(assignments.get(tableName));
        LOG.debug("Adding assignments for "+tableName+": "+assignments.get(tableName));
      }
    }

    return result;
  }

  void registerMBean() {
    GroupMXBeanImpl mxBeanInfo =
        GroupMXBeanImpl.init(this, master);
    try {
      ObjectName name
          = new ObjectName("Hadoop:service=HBase,name=Master,sub=rsGroup");
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      mbs.registerMBean(mxBeanInfo, name);
    } catch (InstanceAlreadyExistsException var6) {
    } catch (Exception var7) {
      throw new IllegalStateException("Failed to register rsGroup MBean");
    }
    LOG.info("Registered Group MXBean");
  }

  public void prepareGroupForTable(HTableDescriptor desc) throws IOException {
    String groupName =
        master.getNamespaceDescriptor(desc.getTableName().getNamespaceAsString())
                .getConfigurationValue(GroupInfo.NAMESPACEDESC_PROP_GROUP);
    if (groupName == null) {
      groupName = GroupInfo.DEFAULT_GROUP;
    }
    GroupInfo groupInfo = getGroupInfo(groupName);
    if (groupInfo == null) {
      throw new ConstraintException("Group " + groupName + " does not exist.");
    }
    if (!groupInfo.containsTable(desc.getTableName())) {
      LOG.debug("Pre-moving table " + desc.getTableName() + " to group " + groupName);
      moveTables(Sets.newHashSet(desc.getTableName()), groupName);
    }
  }

  public void cleanupGroupForTable(TableName tableName) throws IOException {
    try {
      GroupInfo group = getGroupInfoOfTable(tableName);
      if (group != null) {
        LOG.debug("Removing deleted table from table group " + group.getName());
        moveTables(Sets.newHashSet(tableName), null);
      }
    } catch (ConstraintException ex) {
      LOG.debug("Failed to perform group information cleanup for table: " + tableName, ex);
    } catch (IOException ex) {
      LOG.debug("Failed to perform group information cleanup for table: " + tableName, ex);
    }
  }

  @Override
  public void close() throws IOException {
  }
}