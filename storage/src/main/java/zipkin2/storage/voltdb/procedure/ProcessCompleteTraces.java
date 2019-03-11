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
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import static zipkin2.storage.voltdb.Schema.TABLE_COMPLETE_TRACE;
import static zipkin2.storage.voltdb.Schema.TABLE_PENDING_EXPORT;

public class ProcessCompleteTraces extends BaseLinkTrace {
  static final Byte ONE = 1;

  final SQLStmt tracesToProcess = new SQLStmt(
      "SELECT trace_id, should_export from "
          + TABLE_COMPLETE_TRACE
          + " WHERE is_dirty = 1 ORDER BY trace_id LIMIT ?");

  final SQLStmt markProcessed = new SQLStmt(
      "UPDATE " + TABLE_COMPLETE_TRACE + " SET should_export = ?, is_dirty = 0 WHERE trace_id = ?");

  final SQLStmt updatePendingExport = new SQLStmt(
      "UPSERT INTO " + TABLE_PENDING_EXPORT + " VALUES (?, NOW())");

  public VoltTable run(String partitionKey, int maxTraces) {
    if (maxTraces < 1) throw new VoltAbortException("maxTraces < 1");

    voltQueueSQL(tracesToProcess, maxTraces);
    VoltTable pendingTraceIdTable = voltExecuteSQL()[0];

    VoltTable result = new VoltTable(new VoltTable.ColumnInfo("trace_id", VoltType.STRING));
    if (pendingTraceIdTable.getRowCount() == 0) return result; // no rows

    while (pendingTraceIdTable.advanceRow()) {
      String trace_id = pendingTraceIdTable.getString(0);

      // Signal via the pending export table, if a decision to export happened
      Byte should_export = (Byte) pendingTraceIdTable.get(1, VoltType.TINYINT);
      if (ONE.equals(should_export)) {
        voltQueueSQL(updatePendingExport, trace_id);
        voltExecuteSQL(false);
      }

      // Regardless of export decision, we should process or re-process dependency links
      linkTrace(trace_id, false);
      voltQueueSQL(markProcessed, EXPECT_SCALAR_MATCH(1), should_export, trace_id);
      voltExecuteSQL(false);

      // The response includes processed trace IDs, regardless of export decision
      result.addRow(trace_id);
    }

    return result;
  }
}
