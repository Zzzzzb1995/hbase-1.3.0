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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MetaTableAccessor.DefaultVisitorBase;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableStateManager;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.ServerListener;
import org.apache.hadoop.hbase.master.procedure.CreateTableProcedure;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.GroupProtos;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import org.apache.hadoop.hbase.util.Threads;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.protobuf.ServiceException;

/**
 * This is an implementation of {@link GroupInfoManager}. Which makes
 * use of an HBase table as the persistence store for the group information.
 * It also makes use of zookeeper to store group information needed
 * for bootstrapping during offline mode.
 */
public class GroupInfoManagerImpl implements GroupInfoManager, ServerListener {
  private static final Log LOG = LogFactory.getLog(GroupInfoManagerImpl.class);

  /** Table descriptor for <code>hbase:rsgroup</code> catalog table */
  private final static HTableDescriptor GROUP_TABLE_DESC;
  static {
    GROUP_TABLE_DESC = new HTableDescriptor(GROUP_TABLE_NAME_BYTES);
    GROUP_TABLE_DESC.addFamily(new HColumnDescriptor(META_FAMILY_BYTES));
    GROUP_TABLE_DESC.setRegionSplitPolicyClassName(DisabledRegionSplitPolicy.class.getName());
    try {
      GROUP_TABLE_DESC.addCoprocessor(
          "org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint",
          null, Coprocessor.PRIORITY_SYSTEM, null);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private volatile Map<String, GroupInfo> groupMap;
  private volatile Map<TableName, String> tableMap;
  private MasterServices master;
  private Table groupTable;
  private ClusterConnection conn;
  private ZooKeeperWatcher watcher;
  private GroupStartupWorker groupStartupWorker;
  // contains list of groups that were last flushed to persistent store
  private volatile Set<String> prevGroups;
  private GroupSerDe groupSerDe;
  private DefaultServerUpdater defaultServerUpdater;
  private FailedOpenUpdater failedOpenUpdater;


  public GroupInfoManagerImpl(MasterServices master) throws IOException {
    this.groupMap = Collections.EMPTY_MAP;
    this.tableMap = Collections.EMPTY_MAP;
    groupSerDe = new GroupSerDe();
    this.master = master;
    this.watcher = master.getZooKeeper();
    this.conn = master.getConnection();
    groupStartupWorker = new GroupStartupWorker(this, master, conn);
    prevGroups = new HashSet<String>();
    refresh();
    groupStartupWorker.start();
    defaultServerUpdater = new DefaultServerUpdater(this);
    Threads.setDaemonThreadRunning(defaultServerUpdater);
    failedOpenUpdater = new FailedOpenUpdater(this);
    Threads.setDaemonThreadRunning(failedOpenUpdater);
    master.getServerManager().registerListener(this);

  }

  /**
   * Adds the group.
   *
   * @param groupInfo the group name
   */
  @Override
  public synchronized void addGroup(GroupInfo groupInfo) throws IOException {
    if (groupMap.get(groupInfo.getName()) != null ||
        groupInfo.getName().equals(GroupInfo.DEFAULT_GROUP)) {
      throw new DoNotRetryIOException("Group already exists: "+groupInfo.getName());
    }
    Map<String, GroupInfo> newGroupMap = Maps.newHashMap(groupMap);
    newGroupMap.put(groupInfo.getName(), groupInfo);
    flushConfig(newGroupMap);
  }

  @Override
  public synchronized boolean moveServers(Set<HostAndPort> hostPorts, String srcGroup,
                                          String dstGroup) throws IOException {
    GroupInfo src = new GroupInfo(getGroup(srcGroup));
    GroupInfo dst = new GroupInfo(getGroup(dstGroup));
    boolean foundOne = false;
    for(HostAndPort el: hostPorts) {
      foundOne = src.removeServer(el) || foundOne;
      dst.addServer(el);
    }

    Map<String,GroupInfo> newGroupMap = Maps.newHashMap(groupMap);
    newGroupMap.put(src.getName(), src);
    newGroupMap.put(dst.getName(), dst);

    flushConfig(newGroupMap);
    return foundOne;
  }

  /**
   * Gets the group info of server.
   *
   * @param hostPort the server
   * @return An instance of GroupInfo.
   */
  @Override
  public GroupInfo getGroupOfServer(HostAndPort hostPort) throws IOException {
    for (GroupInfo info : groupMap.values()) {
      if (info.containsServer(hostPort)){
        return info;
      }
    }
    return getGroup(GroupInfo.DEFAULT_GROUP);
  }

  /**
   * Gets the group information.
   *
   * @param groupName
   *          the group name
   * @return An instance of GroupInfo
   */
  @Override
  public GroupInfo getGroup(String groupName) throws IOException {
    GroupInfo groupInfo = groupMap.get(groupName);
    return groupInfo;
  }



  @Override
  public String getGroupOfTable(TableName tableName) throws IOException {
    return tableMap.get(tableName);
  }

  @Override
  public synchronized void moveTables(Set<TableName> tableNames, String groupName) throws IOException {
    if (groupName != null && !groupMap.containsKey(groupName)) {
      throw new DoNotRetryIOException("Group "+groupName+" does not exist or is a special group");
    }
    Map<String,GroupInfo> newGroupMap = Maps.newHashMap(groupMap);
    for(TableName tableName: tableNames) {
      if (tableMap.containsKey(tableName)) {
        GroupInfo src = new GroupInfo(groupMap.get(tableMap.get(tableName)));
        src.removeTable(tableName);
        newGroupMap.put(src.getName(), src);
      }
      if(groupName != null) {
        GroupInfo dst = new GroupInfo(newGroupMap.get(groupName));
        dst.addTable(tableName);
        newGroupMap.put(dst.getName(), dst);
      }
    }

    flushConfig(newGroupMap);
  }


  /**
   * Delete a region server group.
   *
   * @param groupName the group name
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   */
  @Override
  public synchronized void removeGroup(String groupName) throws IOException {
    if (!groupMap.containsKey(groupName) || groupName.equals(GroupInfo.DEFAULT_GROUP)) {
      throw new DoNotRetryIOException("Group "+groupName+" does not exist or is a reserved group");
    }
    Map<String,GroupInfo> newGroupMap = Maps.newHashMap(groupMap);
    newGroupMap.remove(groupName);
    flushConfig(newGroupMap);
  }

  @Override
  public List<GroupInfo> listGroups() throws IOException {
    List<GroupInfo> list = Lists.newLinkedList(groupMap.values());
    return list;
  }

  @Override
  public boolean isOnline() {
    return groupStartupWorker.isOnline();
  }

  @Override
  public synchronized void refresh() throws IOException {
    refresh(false);
  }

  private synchronized void refresh(boolean forceOnline) throws IOException {
    List<GroupInfo> groupList = new LinkedList<GroupInfo>();

    // overwrite anything read from zk, group table is source of truth
    // if online read from GROUP table
    if (forceOnline || isOnline()) {
      LOG.debug("Refreshing in Online mode.");
      if (groupTable == null) {
        groupTable = conn.getTable(GROUP_TABLE_NAME);
      }
      groupList.addAll(groupSerDe.retrieveGroupList(groupTable));
    } else {
      LOG.debug("Refershing in Offline mode.");
      String groupBasePath = ZKUtil.joinZNode(watcher.baseZNode, groupZNode);
      groupList.addAll(groupSerDe.retrieveGroupList(watcher, groupBasePath));
    }

    // refresh default group, prune
    NavigableSet<TableName> orphanTables = new TreeSet<TableName>();
    for(String entry: master.getTableDescriptors().getAll().keySet()) {
      orphanTables.add(TableName.valueOf(entry));
    }

    List<TableName> specialTables;
    if(!master.isInitialized()) {
      specialTables = new ArrayList<TableName>();
      specialTables.add(AccessControlLists.ACL_TABLE_NAME);
      specialTables.add(TableName.META_TABLE_NAME);
      specialTables.add(TableName.NAMESPACE_TABLE_NAME);
      specialTables.add(GroupInfoManager.GROUP_TABLE_NAME);
    } else {
      specialTables =
          master.listTableNamesByNamespace(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR);
    }

    for(TableName table : specialTables) {
      orphanTables.add(table);
    }
    for(GroupInfo group: groupList) {
      if(!group.getName().equals(GroupInfo.DEFAULT_GROUP)) {
        orphanTables.removeAll(group.getTables());
      }
    }

    // This is added to the last of the list
    // so it overwrites the default group loaded
    // from region group table or zk
    groupList.add(new GroupInfo(GroupInfo.DEFAULT_GROUP,
        Sets.newHashSet(getDefaultServers()),
        orphanTables));


    // populate the data
    HashMap<String, GroupInfo> newGroupMap = Maps.newHashMap();
    HashMap<TableName, String> newTableMap = Maps.newHashMap();
    for (GroupInfo group : groupList) {
      newGroupMap.put(group.getName(), group);
      for(TableName table: group.getTables()) {
        newTableMap.put(table, group.getName());
      }
    }
    groupMap = Collections.unmodifiableMap(newGroupMap);
    tableMap = Collections.unmodifiableMap(newTableMap);

    prevGroups.clear();
    prevGroups.addAll(groupMap.keySet());
  }

  private synchronized Map<TableName,String> flushConfigTable(Map<String,GroupInfo> newGroupMap)
      throws IOException {
    Map<TableName,String> newTableMap = Maps.newHashMap();
    List<Mutation> mutations = Lists.newArrayList();

    // populate deletes
    for(String groupName : prevGroups) {
      if(!newGroupMap.containsKey(groupName)) {
        Delete d = new Delete(Bytes.toBytes(groupName));
        mutations.add(d);
      }
    }

    // populate puts
    for(GroupInfo groupInfo : newGroupMap.values()) {
      GroupProtos.GroupInfo proto = ProtobufUtil.toProtoGroupInfo(groupInfo);
      Put p = new Put(Bytes.toBytes(groupInfo.getName()));
      p.add(META_FAMILY_BYTES,
          META_QUALIFIER_BYTES,
          proto.toByteArray());
      mutations.add(p);
      for(TableName entry: groupInfo.getTables()) {
        newTableMap.put(entry, groupInfo.getName());
      }
    }

    if(mutations.size() > 0) {
      multiMutate(mutations);
    }
    return newTableMap;
  }

  private synchronized void flushConfig(Map<String, GroupInfo> newGroupMap) throws IOException {
    Map<TableName, String> newTableMap;

    // For offline mode persistence is still unavailable
    // We're refreshing in-memory state but only for default servers
    if (!isOnline()) {
      Map<String, GroupInfo> m = Maps.newHashMap(groupMap);
      GroupInfo oldDefaultGroup = m.remove(GroupInfo.DEFAULT_GROUP);
      GroupInfo newDefaultGroup = newGroupMap.remove(GroupInfo.DEFAULT_GROUP);
      if (!m.equals(newGroupMap) ||
          !oldDefaultGroup.getTables().equals(newDefaultGroup.getTables())) {
        throw new IOException("Only default servers can be updated during offline mode");
      }
      newGroupMap.put(GroupInfo.DEFAULT_GROUP, newDefaultGroup);
      groupMap = newGroupMap;
      return;
    }

    newTableMap = flushConfigTable(newGroupMap);

    // make changes visible since it has been
    // persisted in the source of truth
    groupMap = Collections.unmodifiableMap(newGroupMap);
    tableMap = Collections.unmodifiableMap(newTableMap);


    try {
      String groupBasePath = ZKUtil.joinZNode(watcher.baseZNode, groupZNode);
      ZKUtil.createAndFailSilent(watcher, groupBasePath, ProtobufUtil.PB_MAGIC);

      List<ZKUtil.ZKUtilOp> zkOps = new ArrayList<ZKUtil.ZKUtilOp>(newGroupMap.size());
      for(String groupName : prevGroups) {
        if(!newGroupMap.containsKey(groupName)) {
          String znode = ZKUtil.joinZNode(groupBasePath, groupName);
          zkOps.add(ZKUtil.ZKUtilOp.deleteNodeFailSilent(znode));
        }
      }


      for(GroupInfo groupInfo : newGroupMap.values()) {
        String znode = ZKUtil.joinZNode(groupBasePath, groupInfo.getName());
        GroupProtos.GroupInfo proto = ProtobufUtil.toProtoGroupInfo(groupInfo);
        LOG.debug("Updating znode: "+znode);
        ZKUtil.createAndFailSilent(watcher, znode);
        zkOps.add(ZKUtil.ZKUtilOp.deleteNodeFailSilent(znode));
        zkOps.add(ZKUtil.ZKUtilOp.createAndFailSilent(znode,
            ProtobufUtil.prependPBMagic(proto.toByteArray())));
      }
      LOG.debug("Writing ZK GroupInfo count: " + zkOps.size());

      ZKUtil.multiOrSequential(watcher, zkOps, false);
    } catch (KeeperException e) {
      LOG.error("Failed to write to groupZNode", e);
      master.abort("Failed to write to groupZNode", e);
      throw new IOException("Failed to write to groupZNode",e);
    }

    prevGroups.clear();
    prevGroups.addAll(newGroupMap.keySet());
  }

  private List<ServerName> getOnlineRS() throws IOException {
    if (master != null) {
      return master.getServerManager().getOnlineServersList();
    }
    try {
      LOG.debug("Reading online RS from zookeeper");
      List<ServerName> servers = new LinkedList<ServerName>();
      for (String el: ZKUtil.listChildrenNoWatch(watcher, watcher.rsZNode)) {
        servers.add(ServerName.parseServerName(el));
      }
      return servers;
    } catch (KeeperException e) {
      throw new IOException("Failed to retrieve server list from zookeeper", e);
    }
  }

  private List<HostAndPort> getDefaultServers() throws IOException {
    List<HostAndPort> defaultServers = new LinkedList<HostAndPort>();
    for(ServerName server : getOnlineRS()) {
      HostAndPort hostPort = HostAndPort.fromParts(server.getHostname(), server.getPort());
      boolean found = false;
      for(GroupInfo groupInfo : groupMap.values()) {
        if(!GroupInfo.DEFAULT_GROUP.equals(groupInfo.getName()) &&
            groupInfo.containsServer(hostPort)) {
          found = true;
          break;
        }
      }
      if(!found) {
        defaultServers.add(hostPort);
      }
    }
    return defaultServers;
  }

  private synchronized void updateDefaultServers(
      Set<HostAndPort> hostPort) throws IOException {
    GroupInfo info = groupMap.get(GroupInfo.DEFAULT_GROUP);
    GroupInfo newInfo = new GroupInfo(info.getName(), hostPort, info.getTables());
    HashMap<String, GroupInfo> newGroupMap = Maps.newHashMap(groupMap);
    newGroupMap.put(newInfo.getName(), newInfo);
    flushConfig(newGroupMap);
  }

  @Override
  public void serverAdded(ServerName serverName) {
    defaultServerUpdater.serverChanged();
    failedOpenUpdater.serverChanged();
  }

  @Override
  public void serverRemoved(ServerName serverName) {
    defaultServerUpdater.serverChanged();
  }

  private static class DefaultServerUpdater extends Thread {
    private static final Log LOG = LogFactory.getLog(DefaultServerUpdater.class);
    private GroupInfoManagerImpl mgr;
    private volatile boolean hasChanged = false;

    public DefaultServerUpdater(GroupInfoManagerImpl mgr) {
      this.mgr = mgr;
      setName(DefaultServerUpdater.class.getName()+"-" + mgr.master.getServerName());
      setDaemon(true);
    }

    public void run() {
      List<HostAndPort> prevDefaultServers = new LinkedList<HostAndPort>();
      while (!mgr.master.isAborted() && !mgr.master.isStopped()) {
        try {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Updating default servers");
          }
          List<HostAndPort> servers = mgr.getDefaultServers();
          Collections.sort(servers, new Comparator<HostAndPort>() {
            @Override
            public int compare(HostAndPort o1, HostAndPort o2) {
              int diff = o1.getHostText().compareTo(o2.getHostText());
              if (diff != 0) {
                return diff;
              }
              return o1.getPort() - o2.getPort();
            }
          });
          if(!servers.equals(prevDefaultServers)) {
            mgr.updateDefaultServers(Sets.<HostAndPort>newHashSet(servers));
            prevDefaultServers = servers;
            LOG.info("Updated with servers: "+servers.size());
          }
          try {
            synchronized (this) {
              while (!hasChanged) {
                wait();
              }
              hasChanged = false;
            }
          } catch (InterruptedException e) {
            LOG.warn("Interrupted", e);
          }
        } catch (IOException e) {
          LOG.warn("Failed to update default servers", e);
        }
      }
    }

    // Called for both server additions and removals.
    public void serverChanged() {
      synchronized (this) {
        hasChanged = true;
        this.notify();
      }
    }
  }

  private static class FailedOpenUpdater extends Thread {
    private static final Log LOG = LogFactory.getLog(FailedOpenUpdater.class);

    private final GroupInfoManagerImpl mgr;
    private final long waitInterval;
    private volatile boolean hasChanged = false;

    public FailedOpenUpdater(GroupInfoManagerImpl mgr) {
      this.mgr = mgr;
      this.waitInterval = mgr.master.getConfiguration().getLong(REASSIGN_WAIT_INTERVAL_KEY,
              DEFAULT_REASSIGN_WAIT_INTERVAL);
      setName(FailedOpenUpdater.class.getName() + "-" + mgr.master.getServerName());
      setDaemon(true);
    }

    @Override
    public void run() {
      while (!mgr.master.isAborted() && !mgr.master.isStopped()) {
        boolean interrupted = false;
        try {
          synchronized (this) {
            while (!hasChanged) {
              wait();
            }
            hasChanged = false;
          }
        } catch (InterruptedException e) {
          LOG.warn("Interrupted", e);
          interrupted = true;
        }
        if (mgr.master.isAborted() || mgr.master.isStopped() || interrupted) {
          continue;
        }

        // First, wait a while in case more servers are about to rejoin the cluster
        try {
          Thread.sleep(waitInterval);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted", e);
        }
        if (mgr.master.isAborted() || mgr.master.isStopped()) {
          continue;
        }

        // Kick all regions in FAILED_OPEN state
        List<HRegionInfo> failedAssignments = Lists.newArrayList();
        List<RegionState> regionStates = new ArrayList<RegionState>
                (mgr.master.getAssignmentManager().getRegionStates().getRegionsInTransition().values());
        for (RegionState state : regionStates) {
          if (state.isFailedOpen()) {
            failedAssignments.add(state.getRegion());
          }
        }
        for (HRegionInfo region : failedAssignments) {
          LOG.info("Retrying assignment of " + region);
          mgr.master.getAssignmentManager().unassign(region);
        }
      }
    }

    // Only called for server additions
    public void serverChanged() {
      synchronized (this) {
        hasChanged = true;
      }
    }
  }


  private static class GroupStartupWorker extends Thread {
    private static final Log LOG = LogFactory.getLog(GroupStartupWorker.class);

    private Configuration conf;
    private volatile boolean isOnline = false;
    private MasterServices masterServices;
    private GroupInfoManagerImpl groupInfoManager;
    private ClusterConnection conn;

    public GroupStartupWorker(GroupInfoManagerImpl groupInfoManager,
                              MasterServices masterServices,
                              ClusterConnection conn) {
      this.conf = masterServices.getConfiguration();
      this.masterServices = masterServices;
      this.groupInfoManager = groupInfoManager;
      this.conn = conn;
      setName(GroupStartupWorker.class.getName()+"-"+masterServices.getServerName());
      setDaemon(true);
    }

    @Override
    public void run() {
      if(waitForGroupTableOnline()) {
        LOG.info("GroupBasedLoadBalancer is now online");
      }
    }

    public boolean waitForGroupTableOnline() {
      final List<HRegionInfo> foundRegions = new LinkedList<HRegionInfo>();
      final List<HRegionInfo> assignedRegions = new LinkedList<HRegionInfo>();
      final AtomicBoolean found = new AtomicBoolean(false);

      boolean createSent = false;
      while (!found.get() && isMasterRunning()) {
        try {
          final TableStateManager tsm
            = masterServices.getCoordinatedStateManager().getTableStateManager();
          foundRegions.clear();
          assignedRegions.clear();
          found.set(true);

          final Table nsTable = conn.getTable(TableName.NAMESPACE_TABLE_NAME);
          final Table groupTable = conn.getTable(GroupInfoManager.GROUP_TABLE_NAME);
          boolean rootMetaFound =
              masterServices.getMetaTableLocator().verifyMetaRegionLocation(
                  conn,
                  masterServices.getZooKeeper(),
                  1);
          final AtomicBoolean nsFound = new AtomicBoolean(false);
          if (rootMetaFound) {

            MetaTableAccessor.Visitor visitor = new DefaultVisitorBase() {
              @Override
              public boolean visitInternal(Result row) throws IOException {
                HRegionInfo info = HRegionInfo.getHRegionInfo(row);
                if (info != null) {
                  Cell serverCell =
                      row.getColumnLatestCell(HConstants.CATALOG_FAMILY,
                          HConstants.SERVER_QUALIFIER);
                  if (GROUP_TABLE_NAME.equals(info.getTable()) && serverCell != null) {
                    ServerName sn =
                        ServerName.parseVersionedServerName(CellUtil.cloneValue(serverCell));
                    if (sn == null) {
                      found.set(false);
                    } else if (tsm.isTableState(GROUP_TABLE_NAME, ZooKeeperProtos.Table.State.ENABLED)) {
                      try {
                        ClientProtos.ClientService.BlockingInterface rs = conn.getClient(sn);
                        ClientProtos.GetRequest request =
                            RequestConverter.buildGetRequest(info.getRegionName(),
                                new Get(ROW_KEY));
                        rs.get(null, request);
                        assignedRegions.add(info);
                      } catch(Exception ex) {
                        LOG.debug("Caught exception while verifying group region", ex);
                      }
                    }
                    foundRegions.add(info);
                  }
                  if (TableName.NAMESPACE_TABLE_NAME.equals(info.getTable())) {
                    Cell cell = row.getColumnLatestCell(HConstants.CATALOG_FAMILY,
                        HConstants.SERVER_QUALIFIER);
                    ServerName sn = null;
                    if(cell != null) {
                      sn = ServerName.parseVersionedServerName(CellUtil.cloneValue(cell));
                    }
                    if (tsm.isTableState(TableName.NAMESPACE_TABLE_NAME,
                        ZooKeeperProtos.Table.State.ENABLED)) {
                      try {
                        ClientProtos.ClientService.BlockingInterface rs = conn.getClient(sn);
                        ClientProtos.GetRequest request =
                            RequestConverter.buildGetRequest(info.getRegionName(),
                                new Get(ROW_KEY));
                        rs.get(null, request);
                        nsFound.set(true);
                      } catch(Exception ex) {
                        LOG.debug("Caught exception while verifying group region", ex);
                      }
                    }
                  }
                }
                return true;
              }
            };
            MetaTableAccessor.fullScan(conn, visitor);
            // if no regions in meta then we have to create the table
            if (foundRegions.size() < 1 && rootMetaFound && !createSent && nsFound.get()) {
              groupInfoManager.createGroupTable(masterServices);
              createSent = true;
            }
            LOG.info("Group table: " + GROUP_TABLE_NAME + " isOnline: " + found.get()
                + ", regionCount: " + foundRegions.size() + ", assignCount: "
                + assignedRegions.size() + ", rootMetaFound: "+rootMetaFound);
            found.set(found.get() && assignedRegions.size() == foundRegions.size()
                && foundRegions.size() > 0);
          } else {
            LOG.info("Waiting for catalog tables to come online");
            found.set(false);
          }
          if (found.get()) {
            LOG.debug("With group table online, refreshing cached information.");
            groupInfoManager.refresh(true);
            isOnline = true;
            //flush any inconsistencies between ZK and HTable
            groupInfoManager.flushConfig(groupInfoManager.groupMap);
          }
        } catch(Exception e) {
          found.set(false);
          LOG.warn("Failed to perform check", e);
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          LOG.info("Sleep interrupted", e);
        }
      }
      return found.get();
    }

    public boolean isOnline() {
      return isOnline;
    }

    private boolean isMasterRunning() {
      return !masterServices.isAborted() && !masterServices.isStopped();
    }
  }

  private void createGroupTable(MasterServices masterServices) throws IOException {
    HRegionInfo[] newRegions =
        ModifyRegionUtils.createHRegionInfos(GROUP_TABLE_DESC, null);
    ProcedurePrepareLatch latch = ProcedurePrepareLatch.createLatch();
    masterServices.getMasterProcedureExecutor().submitProcedure(
        new CreateTableProcedure(
            masterServices.getMasterProcedureExecutor().getEnvironment(),
            GROUP_TABLE_DESC,
            newRegions,
            latch));
    latch.await();
    // wait for region to be online
    int tries = 600;
    while(masterServices.getAssignmentManager().getRegionStates()
        .getRegionServerOfRegion(newRegions[0]) == null && tries > 0) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new IOException("Wait interrupted", e);
      }
      tries--;
    }
    if(tries <= 0) {
      throw new IOException("Failed to create group table.");
    }
  }

  private void multiMutate(List<Mutation> mutations)
      throws IOException {
    CoprocessorRpcChannel channel = groupTable.coprocessorService(ROW_KEY);
    MultiRowMutationProtos.MutateRowsRequest.Builder mmrBuilder
      = MultiRowMutationProtos.MutateRowsRequest.newBuilder();
    for (Mutation mutation : mutations) {
      if (mutation instanceof Put) {
        mmrBuilder.addMutationRequest(ProtobufUtil.toMutation(
          ClientProtos.MutationProto.MutationType.PUT, mutation));
      } else if (mutation instanceof Delete) {
        mmrBuilder.addMutationRequest(ProtobufUtil.toMutation(
          ClientProtos.MutationProto.MutationType.DELETE, mutation));
      } else {
        throw new DoNotRetryIOException("multiMutate doesn't support "
          + mutation.getClass().getName());
      }
    }

    MultiRowMutationProtos.MultiRowMutationService.BlockingInterface service =
      MultiRowMutationProtos.MultiRowMutationService.newBlockingStub(channel);
    try {
      service.mutateRows(null, mmrBuilder.build());
    } catch (ServiceException ex) {
      ProtobufUtil.toIOException(ex);
    }
  }
}