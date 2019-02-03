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
import org.voltdb.VoltType;

import static zipkin2.storage.voltdb.Schema.TABLE_SPAN;

public final class GetSpansJson extends VoltProcedure {
  static final String TRACE_IDS_HEADER =
      "SELECT trace_id from " + TABLE_SPAN + " where ";
  static final String TRACE_IDS_FOOTER =
      "ts BETWEEN TO_TIMESTAMP(Millis, ?) AND TO_TIMESTAMP(Millis, ?) ORDER BY trace_id LIMIT ?;";

  // TODO: It seems we need to explicitly make statements, as they have to be declared as final.
  // this means things like tag queries could be brutal to declare (ex 1 tag, 2 tags, 3 tags).
  // there's probably a better way
  final SQLStmt basicStatement = new SQLStmt(TRACE_IDS_HEADER + TRACE_IDS_FOOTER);
  final SQLStmt serviceNameStatement =
      new SQLStmt(TRACE_IDS_HEADER + "service_name = ? AND " + TRACE_IDS_FOOTER);
  final SQLStmt spanNameStatement =
      new SQLStmt(TRACE_IDS_HEADER + "name = ? AND " + TRACE_IDS_FOOTER);
  final SQLStmt serviceNameSpanNameStatement =
      new SQLStmt(TRACE_IDS_HEADER + "service_name = ? AND name = ? AND " + TRACE_IDS_FOOTER);

  final SQLStmt spans = new SQLStmt("SELECT json from " + TABLE_SPAN + " where trace_id in ?;");

  public VoltTable[] run(String serviceName, String spanName, long endTs,
      long lookback, int limit)
      throws VoltAbortException {
    if (serviceName != null && spanName != null) {
      voltQueueSQL(serviceNameSpanNameStatement, serviceName, spanName, endTs - lookback, endTs,
          limit);
    } else if (serviceName != null) {
      voltQueueSQL(serviceNameStatement, serviceName, endTs - lookback, endTs, limit);
    } else if (spanName != null) {
      voltQueueSQL(spanNameStatement, spanName, endTs - lookback, endTs, limit);
    } else {
      voltQueueSQL(basicStatement, endTs - lookback, endTs, limit);
    }
    String[] traceIds = getStrings(voltExecuteSQL());
    if (traceIds.length == 0) return new VoltTable[0];

    voltQueueSQL(spans, (Object) traceIds);
    return voltExecuteSQL(true);
  }

  static String[] getStrings(VoltTable[] response) {
    int length = response.length == 0 ? 0 : response[0].getRowCount();
    if (length == 0) return new String[0];

    VoltTable table = response[0];
    String[] result = new String[length];
    int i = 0;
    while (table.advanceRow()) {
      result[i++] = (String) table.get(0, VoltType.STRING);
    }
    return result;
  }
}
