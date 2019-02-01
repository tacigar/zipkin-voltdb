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
package zipkin2.storage.voltdb;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

/** Test class that just gets json from a trace as an array */
public class GetTrace extends VoltProcedure {

  public final SQLStmt GetTrace = new SQLStmt("SELECT json from Span where trace_id = ?;");

  public VoltTable run(String traceId) throws VoltAbortException {
    VoltTable[] queryresults;

    voltQueueSQL(GetTrace, traceId);

    queryresults = voltExecuteSQL();

    VoltTable table = queryresults[0];
    int length = table.getRowCount();
    if (length == 0) return null;

    StringBuilder jsonArray = new StringBuilder().append('[');
    for (int i = 0; i < length; ) {
      table.advanceRow();
      jsonArray.append((String) table.get(0, VoltType.STRING));
      if (++i < length) jsonArray.append(',');
    }
    VoltTable result = new VoltTable(new VoltTable.ColumnInfo("json", VoltType.STRING));
    result.addRow(jsonArray.append(']').toString());
    return result;
  }
}
