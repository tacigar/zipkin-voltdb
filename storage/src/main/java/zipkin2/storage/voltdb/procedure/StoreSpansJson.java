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

public final class StoreSpansJson extends VoltProcedure {

  final SQLStmt insertSpan = new SQLStmt("INSERT INTO Span ("
      + "trace_id, id, service_name, remote_service_name, name, ts, duration, is_error, md5, json"
      + ") VALUES (?, ?, ?, ?, ?, TO_TIMESTAMP(Micros, ?), ?, ?, ?, ?)");

  public VoltTable[] run(String trace_id, String id,
      String service_name, String remote_service_name, String name,
      Long ts, Long duration, byte is_error, byte[] md5, byte[] json) throws VoltAbortException {
    voltQueueSQL(insertSpan,
        trace_id, id, service_name, remote_service_name, name, ts, duration, is_error, md5, json);
    return voltExecuteSQL();
  }
}
