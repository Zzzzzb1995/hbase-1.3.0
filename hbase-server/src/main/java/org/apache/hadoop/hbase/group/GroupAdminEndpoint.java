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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos.AddGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos.AddGroupResponse;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos.BalanceGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos.BalanceGroupResponse;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos.GetGroupInfoOfServerRequest;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos.GetGroupInfoOfServerResponse;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos.GetGroupInfoOfTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos.GetGroupInfoOfTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos.GetGroupInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos.GetGroupInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos.GroupAdminService;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos.ListGroupInfosRequest;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos.ListGroupInfosResponse;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos.MoveServersRequest;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos.MoveServersResponse;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos.MoveTablesRequest;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos.MoveTablesResponse;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos.RemoveGroupRequest;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos.RemoveGroupResponse;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas;

import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

public class GroupAdminEndpoint extends GroupAdminService
    implements CoprocessorService, Coprocessor, MasterObserver {

  private static final Log LOG = LogFactory.getLog(GroupAdminEndpoint.class);
  private MasterServices master = null;

  private static GroupInfoManagerImpl groupInfoManager;
  private GroupAdminServer groupAdminServer;

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    MasterCoprocessorEnvironment menv = (MasterCoprocessorEnvironment)env;
    master = menv.getMasterServices();
    groupInfoManager = new GroupInfoManagerImpl(master);
    groupAdminServer = new GroupAdminServer(master, groupInfoManager);
    Class clazz =
        master.getConfiguration().getClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, null);
    if (!GroupableBalancer.class.isAssignableFrom(clazz)) {
      throw new IOException("Configured balancer is not a GroupableBalancer");
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
  }

  @Override
  public Service getService() {
    return this;
  }

  public GroupInfoManager getGroupInfoManager() {
    return groupInfoManager;
  }

  @Override
  public void getGroupInfo(RpcController controller,
                           GetGroupInfoRequest request,
                           RpcCallback<GetGroupInfoResponse> done) {
    GetGroupInfoResponse response = null;
    try {
      GetGroupInfoResponse.Builder builder =
          GetGroupInfoResponse.newBuilder();
      GroupInfo groupInfo = groupAdminServer.getGroupInfo(request.getGroupName());
      if(groupInfo != null) {
        builder.setGroupInfo(ProtobufUtil.toProtoGroupInfo(groupInfo));
      }
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void getGroupInfoOfTable(RpcController controller,
                                  GetGroupInfoOfTableRequest request,
                                  RpcCallback<GetGroupInfoOfTableResponse> done) {
    GetGroupInfoOfTableResponse response = null;
    try {
      GetGroupInfoOfTableResponse.Builder builder =
          GetGroupInfoOfTableResponse.newBuilder();
      GroupInfo groupInfo =
          groupAdminServer.getGroupInfoOfTable(ProtobufUtil.toTableName(request.getTableName()));
      response = builder.setGroupInfo(ProtobufUtil.toProtoGroupInfo(groupInfo)).build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void moveServers(RpcController controller,
                          MoveServersRequest request,
                          RpcCallback<MoveServersResponse> done) {
    GroupAdminProtos.MoveServersResponse response = null;
    try {
      GroupAdminProtos.MoveServersResponse.Builder builder =
          GroupAdminProtos.MoveServersResponse.newBuilder();
      Set<HostAndPort> hostPorts = Sets.newHashSet();
      for(HBaseProtos.HostPort el: request.getServersList()) {
        hostPorts.add(HostAndPort.fromParts(el.getHostName(), el.getPort()));
      }
      groupAdminServer.moveServers(hostPorts, request.getTargetGroup());
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void moveTables(RpcController controller,
                         MoveTablesRequest request,
                         RpcCallback<MoveTablesResponse> done) {
    MoveTablesResponse response = null;
    try {
      MoveTablesResponse.Builder builder =
          MoveTablesResponse.newBuilder();
      Set<TableName> tables = new HashSet<TableName>(request.getTableNameList().size());
      for(HBaseProtos.TableName tableName: request.getTableNameList()) {
        tables.add(ProtobufUtil.toTableName(tableName));
      }
      groupAdminServer.moveTables(tables, request.getTargetGroup());
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void addGroup(RpcController controller,
                       AddGroupRequest request,
                       RpcCallback<AddGroupResponse> done) {
    AddGroupResponse response = null;
    try {
      AddGroupResponse.Builder builder =
          AddGroupResponse.newBuilder();
      groupAdminServer.addGroup(request.getGroupName());
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void removeGroup(RpcController controller,
                          RemoveGroupRequest request,
                          RpcCallback<RemoveGroupResponse> done) {
    RemoveGroupResponse response = null;
    try {
      RemoveGroupResponse.Builder builder =
          RemoveGroupResponse.newBuilder();
      groupAdminServer.removeGroup(request.getGroupName());
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void balanceGroup(RpcController controller,
                           BalanceGroupRequest request,
                           RpcCallback<BalanceGroupResponse> done) {
    BalanceGroupResponse response = null;
    try {
      BalanceGroupResponse.Builder builder =
          BalanceGroupResponse.newBuilder();
      builder.setBalanceRan(groupAdminServer.balanceGroup(request.getGroupName()));
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void listGroupInfos(RpcController controller,
                             ListGroupInfosRequest request,
                             RpcCallback<ListGroupInfosResponse> done) {
    ListGroupInfosResponse response = null;
    try {
      ListGroupInfosResponse.Builder builder =
          ListGroupInfosResponse.newBuilder();
      for(GroupInfo groupInfo: groupAdminServer.listGroups()) {
        builder.addGroupInfo(ProtobufUtil.toProtoGroupInfo(groupInfo));
      }
      response = builder.build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void getGroupInfoOfServer(RpcController controller,
                                   GetGroupInfoOfServerRequest request,
                                   RpcCallback<GetGroupInfoOfServerResponse> done) {
    GetGroupInfoOfServerResponse response = null;
    try {
      GetGroupInfoOfServerResponse.Builder builder =
          GetGroupInfoOfServerResponse.newBuilder();
      GroupInfo groupInfo = groupAdminServer.getGroupOfServer(
          HostAndPort.fromParts(request.getServer().getHostName(), request.getServer().getPort()));
      response = builder.setGroupInfo(ProtobufUtil.toProtoGroupInfo(groupInfo)).build();
    } catch (IOException e) {
      ResponseConverter.setControllerException(controller, e);
    }
    done.run(response);
  }

  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
    groupAdminServer.prepareGroupForTable(desc);
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              TableName tableName) throws IOException {
    groupAdminServer.cleanupGroupForTable(tableName);
  }

  @Override
  public void preCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 NamespaceDescriptor ns) throws IOException {
    String group = ns.getConfigurationValue(GroupInfo.NAMESPACEDESC_PROP_GROUP);
    if(group != null && groupAdminServer.getGroupInfo(group) == null) {
      throw new ConstraintException("Region server group "+group+" does not exit");
    }
  }

  @Override
  public void preModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 NamespaceDescriptor ns) throws IOException {
    preCreateNamespace(ctx, ns);
  }

  @Override
  public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              HTableDescriptor desc,
                              HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void preCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                    HTableDescriptor desc,
                                    HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void postCreateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                     HTableDescriptor desc,
                                     HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                             TableName tableName) throws IOException {
  }

  @Override
  public void preDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                    TableName tableName) throws IOException {
  }

  @Override
  public void postDeleteTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                     TableName tableName) throws IOException {
  }

  @Override
  public void preTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               TableName tableName) throws IOException {
  }

  @Override
  public void postTruncateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                TableName tableName) throws IOException {
  }

  @Override
  public void preTruncateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                      TableName tableName) throws IOException {
  }

  @Override
  public void postTruncateTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                       TableName tableName) throws IOException {
  }

  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                             TableName tableName,
                             HTableDescriptor htd) throws IOException {
  }

  @Override
  public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              TableName tableName,
                              HTableDescriptor htd) throws IOException {
  }

  @Override
  public void preModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                    TableName tableName,
                                    HTableDescriptor htd) throws IOException {
  }

  @Override
  public void postModifyTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                     TableName tableName,
                                     HTableDescriptor htd) throws IOException {
  }

  @Override
  public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
                           TableName tableName,
                           HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
                            TableName tableName,
                            HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void preAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  TableName tableName,
                                  HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void postAddColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                   TableName tableName,
                                   HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void preModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              TableName tableName,
                              HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               TableName tableName,
                               HColumnDescriptor columnFamily) throws IOException {
  }

  @Override
  public void preModifyColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName, HColumnDescriptor columnFamily) throws IOException {

  }

  @Override
  public void postModifyColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                      TableName tableName, HColumnDescriptor columnFamily) throws
      IOException {

  }

  @Override
  public void preDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName, byte[] columnFamily) throws IOException {

  }

  @Override
  public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName, byte[] columnFamily) throws IOException {

  }

  @Override
  public void preDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName, byte[] columnFamily) throws IOException {

  }

  @Override
  public void postDeleteColumnHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                      TableName tableName, byte[] columnFamily) throws IOException {

  }

  @Override
  public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName) throws IOException {

  }

  @Override
  public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName) throws IOException {

  }

  @Override
  public void preEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName) throws IOException {

  }

  @Override
  public void postEnableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName) throws IOException {

  }

  @Override
  public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName) throws IOException {

  }

  @Override
  public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName) throws IOException {

  }

  @Override
  public void preDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName) throws IOException {

  }

  @Override
  public void postDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                      TableName tableName) throws IOException {

  }

  @Override
  public void preMove(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo region,
                      ServerName srcServer, ServerName destServer) throws IOException {

  }

  @Override
  public void postMove(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo region,
                       ServerName srcServer, ServerName destServer) throws IOException {

  }

  @Override
  public void preAssign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo
      regionInfo) throws IOException {

  }

  @Override
  public void postAssign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo
      regionInfo) throws IOException {

  }

  @Override
  public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo
      regionInfo, boolean force) throws IOException {

  }

  @Override
  public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo
      regionInfo, boolean force) throws IOException {

  }

  @Override
  public void preRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo
      regionInfo) throws IOException {

  }

  @Override
  public void postRegionOffline(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo
      regionInfo) throws IOException {

  }

  @Override
  public void preBalance(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {

  }

  @Override
  public void postBalance(ObserverContext<MasterCoprocessorEnvironment> ctx, List<RegionPlan>
      plans) throws IOException {

  }

  @Override
  public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx, boolean
      newValue) throws IOException {
    return false;
  }

  @Override
  public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx, boolean
      oldValue, boolean newValue) throws IOException {

  }

  @Override
  public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {

  }

  @Override
  public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {

  }

  @Override
  public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws
      IOException {

  }

  @Override
  public void preMasterInitialization(ObserverContext<MasterCoprocessorEnvironment> ctx) throws
      IOException {

  }

  @Override
  public void preSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription
      snapshot, HTableDescriptor hTableDescriptor) throws IOException {

  }

  @Override
  public void postSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx, SnapshotDescription
      snapshot, HTableDescriptor hTableDescriptor) throws IOException {

  }

  @Override
  public void preListSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                              SnapshotDescription snapshot) throws IOException {

  }

  @Override
  public void postListSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               SnapshotDescription snapshot) throws IOException {

  }

  @Override
  public void preCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               SnapshotDescription snapshot, HTableDescriptor hTableDescriptor)
      throws IOException {

  }

  @Override
  public void postCloneSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                SnapshotDescription snapshot, HTableDescriptor hTableDescriptor)
      throws IOException {

  }

  @Override
  public void preRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 SnapshotDescription snapshot, HTableDescriptor hTableDescriptor)
      throws IOException {

  }

  @Override
  public void postRestoreSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  SnapshotDescription snapshot, HTableDescriptor
      hTableDescriptor) throws IOException {

  }

  @Override
  public void preDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                SnapshotDescription snapshot) throws IOException {

  }

  @Override
  public void postDeleteSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 SnapshotDescription snapshot) throws IOException {

  }

  @Override
  public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                     List<TableName> tableNamesList, List<HTableDescriptor>
      descriptors, String regex) throws IOException {

  }

  @Override
  public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                      List<TableName> tableNamesList, List<HTableDescriptor>
      descriptors, String regex) throws IOException {

  }

  @Override
  public void preGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
                               List<HTableDescriptor> descriptors, String regex) throws
      IOException {

  }

  @Override
  public void postGetTableNames(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                List<HTableDescriptor> descriptors, String regex) throws
      IOException {

  }

  @Override
  public void postCreateNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  NamespaceDescriptor ns) throws IOException {

  }

  @Override
  public void preDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, String
      namespace) throws IOException {

  }

  @Override
  public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx, String
      namespace) throws IOException {

  }

  @Override
  public void postModifyNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                  NamespaceDescriptor ns) throws IOException {

  }

  @Override
  public void preGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx, String
      namespace) throws IOException {

  }

  @Override
  public void postGetNamespaceDescriptor(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                         NamespaceDescriptor ns) throws IOException {

  }

  @Override
  public void preListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                          List<NamespaceDescriptor> descriptors) throws
      IOException {

  }

  @Override
  public void postListNamespaceDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                           List<NamespaceDescriptor> descriptors) throws
      IOException {

  }

  @Override
  public void preTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName) throws IOException {

  }

  @Override
  public void postTableFlush(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName) throws IOException {

  }

  @Override
  public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName,
                              Quotas quotas) throws IOException {

  }

  @Override
  public void postSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String
      userName, Quotas quotas) throws IOException {

  }

  @Override
  public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName,
                              TableName tableName, Quotas quotas) throws IOException {

  }

  @Override
  public void postSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String
      userName, TableName tableName, Quotas quotas) throws IOException {

  }

  @Override
  public void preSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String userName,
                              String namespace, Quotas quotas) throws IOException {

  }

  @Override
  public void postSetUserQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String
      userName, String namespace, Quotas quotas) throws IOException {

  }

  @Override
  public void preSetTableQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName, Quotas quotas) throws IOException {

  }

  @Override
  public void postSetTableQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, TableName
      tableName, Quotas quotas) throws IOException {

  }

  @Override
  public void preSetNamespaceQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String
      namespace, Quotas quotas) throws IOException {

  }

  @Override
  public void postSetNamespaceQuota(ObserverContext<MasterCoprocessorEnvironment> ctx, String
      namespace, Quotas quotas) throws IOException {
  }

  @Override
  public void preMoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx, Set<HostAndPort>
      servers, String targetGroup) throws IOException {
  }

  @Override
  public void postMoveServers(ObserverContext<MasterCoprocessorEnvironment> ctx, Set<HostAndPort>
      servers, String targetGroup) throws IOException {
  }

  @Override
  public void preMoveTables(ObserverContext<MasterCoprocessorEnvironment> ctx, Set<TableName>
      tables, String targetGroup) throws IOException {
  }

  @Override
  public void postMoveTables(ObserverContext<MasterCoprocessorEnvironment> ctx,
      Set<TableName> tables, String targetGroup) throws IOException {
  }

  @Override
  public void preAddGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
      throws IOException {
  }

  @Override
  public void postAddGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
      throws IOException {
  }

  @Override
  public void preRemoveGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
      throws IOException {
  }

  @Override
  public void postRemoveGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String name)
      throws IOException {
  }

  @Override
  public void preBalanceGroup(ObserverContext<MasterCoprocessorEnvironment> ctx, String groupName)
      throws IOException {
  }

  @Override
  public void postBalanceGroup(ObserverContext<MasterCoprocessorEnvironment> ctx,
      String groupName, boolean balancerRan) throws IOException {
  }

  @Override
  public void preGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<TableName> tableNamesList, List<HTableDescriptor> descriptors) throws IOException {
  }

  @Override
  public void postGetTableDescriptors(ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<HTableDescriptor> descriptors) throws IOException {
  }

  //empty implementation

  @Override
  public void preAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                ProcedureExecutor<MasterProcedureEnv> procEnv, long procId) throws IOException {

  }

  @Override
  public void postAbortProcedure(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {

  }

  @Override
  public void preListProcedures(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {

  }

  @Override
  public void postListProcedures(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                 List<ProcedureInfo> procInfoList) throws IOException {

  }

  @Override
  public boolean preSetSplitOrMergeEnabled(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                           boolean newValue, Admin.MasterSwitchType switchType) throws IOException {
    return false;
  }

  @Override
  public void postSetSplitOrMergeEnabled(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                         boolean newValue, Admin.MasterSwitchType switchType) throws IOException {

  }

  @Override
  public void preDispatchMerge(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionA,
                               HRegionInfo regionB) throws IOException {

  }

  @Override
  public void postDispatchMerge(ObserverContext<MasterCoprocessorEnvironment> c, HRegionInfo regionA,
                                HRegionInfo regionB) throws IOException {

  }
}