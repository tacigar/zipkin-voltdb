/*
 * Copyright 2019 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.storage.voltdb.procedure;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import static zipkin2.storage.voltdb.Schema.TABLE_SPAN;

public final class GetServiceNames extends VoltProcedure {

  final SQLStmt serviceNames = new SQLStmt(
      "SELECT distinct(service_name) from " + TABLE_SPAN + " order by service_name");
  final SQLStmt remoteServiceNames = new SQLStmt(
      "SELECT distinct(remote_service_name) from " + TABLE_SPAN + " order by remote_service_name");

  public VoltTable[] run() throws VoltAbortException {
    voltQueueSQL(serviceNames);
    voltQueueSQL(remoteServiceNames);
    return voltExecuteSQL(true);
  }
}
