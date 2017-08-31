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

import com.google.common.net.HostAndPort;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Interface used to manage GroupInfo storage. An implementation
 * has the option to support offline mode.
 * See {@link GroupBasedLoadBalancer}
 */
public interface GroupInfoManager {
  //Assigned before user tables
  public static final TableName GROUP_TABLE_NAME =
      TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR,"rsgroup");
  public static final byte[] GROUP_TABLE_NAME_BYTES = GROUP_TABLE_NAME.toBytes();
  public static final String groupZNode = "groupInfo";
  public static final byte[] META_FAMILY_BYTES = Bytes.toBytes("m");
  public static final byte[] META_QUALIFIER_BYTES = Bytes.toBytes("i");
  public static final byte[] ROW_KEY = {0};


  /**
   * Adds the group.
   *
   * @param groupInfo the group name
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   */
  void addGroup(GroupInfo groupInfo) throws IOException;

  /**
   * Remove a region server group.
   *
   * @param groupName the group name
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   */
  void removeGroup(String groupName) throws IOException;

  /**
   * move servers to a new group.
   * @param hostPorts list of servers, must be part of the same group
   * @param srcGroup
   * @param dstGroup
   * @return true if move was successful
   * @throws java.io.IOException
   */
  boolean moveServers(Set<HostAndPort> hostPorts,
                      String srcGroup, String dstGroup) throws IOException;

  /**
   * Gets the group info of server.
   *
   * @param hostPort the server
   * @return An instance of GroupInfo
   */
  GroupInfo getGroupOfServer(HostAndPort hostPort) throws IOException;

  /**
   * Gets the group information.
   *
   * @param groupName the group name
   * @return An instance of GroupInfo
   */
  GroupInfo getGroup(String groupName) throws IOException;

  /**
   * Get the group membership of a table
   * @param tableName
   * @return Group name of table
   * @throws java.io.IOException
   */
  String getGroupOfTable(TableName tableName) throws IOException;

  /**
   * Set the group membership of a set of tables
   *
   * @param tableNames
   * @param groupName
   * @throws java.io.IOException
   */
  void moveTables(Set<TableName> tableNames, String groupName) throws IOException;

  /**
   * List the groups
   *
   * @return list of GroupInfo
   * @throws java.io.IOException
   */
  List<GroupInfo> listGroups() throws IOException;

  /**
   * Refresh/reload the group information from
   * the persistent store
   *
   * @throws java.io.IOException
   */
  void refresh() throws IOException;

  /**
   * Whether the manager is able to fully
   * return group metadata
   *
   * @return
   */
  boolean isOnline();
}