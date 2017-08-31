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

import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.GroupAdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.GroupProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Client used for managing region server group information.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
class GroupAdminClient extends GroupAdmin {
  private GroupAdminProtos.GroupAdminService.BlockingInterface proxy;
  private static final Log LOG = LogFactory.getLog(GroupAdminClient.class);

  public GroupAdminClient(Connection conn) throws IOException {
    proxy = GroupAdminProtos.GroupAdminService.newBlockingStub(
        conn.getAdmin().coprocessorService());
  }

  @Override
  public GroupInfo getGroupInfo(String groupName) throws IOException {
    try {
      GroupAdminProtos.GetGroupInfoResponse resp =
        proxy.getGroupInfo(null,
            GroupAdminProtos.GetGroupInfoRequest.newBuilder().setGroupName(groupName).build());
      if(resp.hasGroupInfo()) {
        return ProtobufUtil.toGroupInfo(resp.getGroupInfo());
      }
      return null;
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public GroupInfo getGroupInfoOfTable(TableName tableName) throws IOException {
    GroupAdminProtos.GetGroupInfoOfTableRequest request =
        GroupAdminProtos.GetGroupInfoOfTableRequest.newBuilder()
            .setTableName(ProtobufUtil.toProtoTableName(tableName)).build();

    try {
      return ProtobufUtil.toGroupInfo(proxy.getGroupInfoOfTable(null, request).getGroupInfo());
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public void moveServers(Set<HostAndPort> servers, String targetGroup) throws IOException {
    Set<HBaseProtos.HostPort> hostPorts = Sets.newHashSet();
    for(HostAndPort el: servers) {
      hostPorts.add(HBaseProtos.HostPort.newBuilder()
        .setHostName(el.getHostText())
        .setPort(el.getPort())
        .build());
    }
    GroupAdminProtos.MoveServersRequest request =
        GroupAdminProtos.MoveServersRequest.newBuilder()
            .setTargetGroup(targetGroup)
            .addAllServers(hostPorts).build();

    try {
      proxy.moveServers(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public void moveTables(Set<TableName> tables, String targetGroup) throws IOException {
    GroupAdminProtos.MoveTablesRequest.Builder builder =
        GroupAdminProtos.MoveTablesRequest.newBuilder()
            .setTargetGroup(targetGroup);
    for(TableName tableName: tables) {
      builder.addTableName(ProtobufUtil.toProtoTableName(tableName));
    }
    try {
      proxy.moveTables(null, builder.build());
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public void addGroup(String groupName) throws IOException {
    GroupAdminProtos.AddGroupRequest request =
        GroupAdminProtos.AddGroupRequest.newBuilder()
            .setGroupName(groupName).build();
    try {
      proxy.addGroup(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public void removeGroup(String name) throws IOException {
    GroupAdminProtos.RemoveGroupRequest request =
        GroupAdminProtos.RemoveGroupRequest.newBuilder()
            .setGroupName(name).build();
    try {
      proxy.removeGroup(null, request);
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public boolean balanceGroup(String name) throws IOException {
    GroupAdminProtos.BalanceGroupRequest request =
        GroupAdminProtos.BalanceGroupRequest.newBuilder()
            .setGroupName(name).build();

    try {
      return proxy.balanceGroup(null, request).getBalanceRan();
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public List<GroupInfo> listGroups() throws IOException {
    try {
      List<GroupProtos.GroupInfo> resp =
          proxy.listGroupInfos(null, GroupAdminProtos.ListGroupInfosRequest.newBuilder().build())
              .getGroupInfoList();
      List<GroupInfo> result = new ArrayList<GroupInfo>(resp.size());
      for(GroupProtos.GroupInfo entry: resp) {
        result.add(ProtobufUtil.toGroupInfo(entry));
      }
      return result;
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public GroupInfo getGroupOfServer(HostAndPort hostPort) throws IOException {
    GroupAdminProtos.GetGroupInfoOfServerRequest request =
        GroupAdminProtos.GetGroupInfoOfServerRequest.newBuilder()
            .setServer(HBaseProtos.HostPort.newBuilder()
                .setHostName(hostPort.getHostText())
                .setPort(hostPort.getPort())
                .build())
            .build();
    try {
      return ProtobufUtil.toGroupInfo(
          proxy.getGroupInfoOfServer(null, request).getGroupInfo());
    } catch (ServiceException e) {
      throw ProtobufUtil.getRemoteException(e);
    }
  }

  @Override
  public void close() throws IOException {
  }
}