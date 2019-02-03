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
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.internal.DependencyLinker;

/** This uses Zipkin's dependency linker */
public class LinkTrace extends VoltProcedure {
  final SQLStmt getMinimumTimestamp = new SQLStmt("SELECT MIN(ts) from Span where trace_id = ?;");
  final SQLStmt getDependencyLinkFields = new SQLStmt("SELECT"
      + " parent_id, id, kind, service_name, remote_service_name, is_error from Span"
      + " where trace_id = ?;");
  final SQLStmt insertDependencyLink = new SQLStmt("INSERT INTO DependencyLink ("
      + "trace_id, ts, parent, child, call_count, error_count"
      + ") VALUES (?, TO_TIMESTAMP(Micros, ?), ?, ?, ?, ?);");

  public VoltTable[] run(String trace_id) {
    voltQueueSQL(getMinimumTimestamp, EXPECT_ZERO_OR_ONE_ROW, trace_id);
    voltQueueSQL(getDependencyLinkFields, EXPECT_NON_EMPTY, trace_id);
    VoltTable[] tables = voltExecuteSQL();

    VoltTable getMinimumTimestampTable = tables[0];
    if (!getMinimumTimestampTable.advanceRow()) {
      setAppStatusString("no timestamp for trace " + trace_id);
      return new VoltTable[0];
    }
    long ts = getMinimumTimestampTable.getTimestampAsLong(0);
    if (getMinimumTimestampTable.wasNull()) {
      setAppStatusString("null timestamp for trace " + trace_id);
      return new VoltTable[0];
    }

    VoltTable spansTable = tables[1];
    DependencyLinker linker = new DependencyLinker();
    List<Span> spans = new ArrayList<>();
    while (spansTable.advanceRow()) {
      Span.Builder builder = Span.newBuilder()
          .traceId(trace_id)
          .parentId(maybeNull(spansTable, 0))
          .id(spansTable.getString(1));

      String kind = maybeNull(spansTable, 2);
      if (kind != null) builder.kind(Span.Kind.valueOf(kind));

      String service = maybeNull(spansTable, 3);
      if (service != null) {
        builder.localEndpoint(Endpoint.newBuilder().serviceName(service).build());
      }

      String remote_service = maybeNull(spansTable, 4);
      if (remote_service != null) {
        builder.remoteEndpoint(Endpoint.newBuilder().serviceName(remote_service).build());
      }

      if ((byte) spansTable.get(5, VoltType.TINYINT) != 0) {
        builder.putTag("error", "");
      }
      spans.add(builder.build());
    }

    for (DependencyLink link : linker.putTrace(spans).link()) {
      voltQueueSQL(insertDependencyLink, trace_id, ts,
          link.parent(), link.child(), link.callCount(), link.errorCount()
      );
    }
    return voltExecuteSQL(true);
  }

  static String maybeNull(VoltTable table, int index) {
    String result = table.getString(index);
    return table.wasNull() ? null : result;
  }
}
