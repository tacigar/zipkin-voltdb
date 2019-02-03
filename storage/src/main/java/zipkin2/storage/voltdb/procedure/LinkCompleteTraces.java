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

import java.util.ArrayList;
import java.util.List;
import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import static zipkin2.storage.voltdb.Schema.TABLE_COMPLETE_TRACE;

public class LinkCompleteTraces extends BaseLinkTrace {
  final SQLStmt pendingTraceIds = new SQLStmt(
      "SELECT trace_id from "
          + TABLE_COMPLETE_TRACE
          + " WHERE process_ts IS NULL ORDER BY trace_id LIMIT ?");

  final SQLStmt updateCompleteTrace =
      new SQLStmt("UPDATE " + TABLE_COMPLETE_TRACE + " SET process_ts = NOW WHERE trace_id = ?");

  public VoltTable run(String partitionValue, int chunkSize) {
    if (chunkSize <= 0) throw new VoltAbortException("chunkSize must be > 0");

    voltQueueSQL(pendingTraceIds, chunkSize);
    VoltTable pendingTraceIdTable = voltExecuteSQL()[0];

    VoltTable result = new VoltTable(new VoltTable.ColumnInfo("trace_id", VoltType.STRING));
    if (pendingTraceIdTable.getRowCount() == 0) return result; // no rows

    List<String> traceIds = new ArrayList<>();
    while (pendingTraceIdTable.advanceRow()) {
      traceIds.add(pendingTraceIdTable.getString(0));
    }

    for (String trace_id : traceIds) {
      linkTrace(trace_id, false);
      voltQueueSQL(updateCompleteTrace, EXPECT_SCALAR_MATCH(1), trace_id);
      voltExecuteSQL(false);
      result.addRow(trace_id);
    }

    return result;
  }
}
