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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Group user API interface used between client and server.
 */
@InterfaceAudience.Private
public abstract class GroupAdmin implements Closeable {

  /**
   * Create a new GroupAdmin client
   * @param conn
   * @return a new GroupAdmin client
   * @throws IOException
   */
  public static GroupAdmin newClient(Connection conn) throws IOException {
    return new GroupAdminClient(conn);
  }

  /**
   * Gets the group information.
   *
   * @param groupName the group name
   * @return An instance of GroupInfo
   */
  public abstract GroupInfo getGroupInfo(String groupName) throws IOException;

  /**
   * Gets the group info of table.
   *
   * @param tableName the table name
   * @return An instance of GroupInfo.
   */
  public abstract GroupInfo getGroupInfoOfTable(TableName tableName) throws IOException;

  /**
   * Move a set of serves to another group
   *
   *
   * @param servers set of servers, must be in the form HOST:PORT
   * @param targetGroup the target group
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   */
  public abstract void moveServers(Set<HostAndPort> servers, String targetGroup) throws IOException;

  /**
   * Move tables to a new group.
   * This will unassign all of a table's region so it can be reassigned to the correct group.
   * @param tables list of tables to move
   * @param targetGroup target group
   * @throws java.io.IOException
   */
  public abstract void moveTables(Set<TableName> tables, String targetGroup) throws IOException;

  /**
   * Add a new group
   * @param name name of the group
   * @throws java.io.IOException
   */
  public abstract void addGroup(String name) throws IOException;

  /**
   * Remove a group
   * @param name name of the group
   * @throws java.io.IOException
   */
  public abstract void removeGroup(String name) throws IOException;

  /**
   * Balance the regions in a group
   *
   * @param name the name of the gorup to balance
   * @return
   * @throws java.io.IOException
   */
  public abstract boolean balanceGroup(String name) throws IOException;

  /**
   * Lists the existing groups.
   *
   * @return Collection of GroupInfo.
   */
  public abstract List<GroupInfo> listGroups() throws IOException;

  /**
   * Retrieve the GroupInfo a server is affiliated to
   * @param hostPort
   * @return
   * @throws java.io.IOException
   */
  public abstract GroupInfo getGroupOfServer(HostAndPort hostPort) throws IOException;
}