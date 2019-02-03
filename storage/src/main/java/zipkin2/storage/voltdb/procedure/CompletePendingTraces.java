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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import zipkin2.Span;
import zipkin2.internal.SpanNode;

import static zipkin2.storage.voltdb.Schema.TABLE_COMPLETE_TRACE;
import static zipkin2.storage.voltdb.Schema.TABLE_PENDING_TRACE;
import static zipkin2.storage.voltdb.Schema.TABLE_SPAN;
import static zipkin2.storage.voltdb.procedure.BaseLinkTrace.maybeNull;

public class CompletePendingTraces extends VoltProcedure {
  static final Logger LOG = Logger.getLogger(CompletePendingTraces.class.getName());

  final SQLStmt oldTraceIds = new SQLStmt(
      "SELECT SINCE_EPOCH(SECOND, NOW) - SINCE_EPOCH(SECOND, update_ts) AS age_seconds, trace_id"
          + " FROM " + TABLE_PENDING_TRACE
          // TODO: why can't we use age_seconds in the where clause?
          + " WHERE SINCE_EPOCH(SECOND, NOW) - SINCE_EPOCH(SECOND, update_ts) > ? ORDER BY update_ts LIMIT ?");

  final SQLStmt minimalSpanFields = new SQLStmt(
      "SELECT MAX(parent_id), id, MIN(ts), MIN(duration) FROM " + TABLE_SPAN + " WHERE trace_id = ?"
          + " GROUP BY id");

  final SQLStmt deletePendingTrace = new SQLStmt(
      "DELETE FROM " + TABLE_PENDING_TRACE + " WHERE trace_id = ?;");

  final SQLStmt updateCompleteTrace = new SQLStmt( // unset the process timestamp
      "UPSERT INTO " + TABLE_COMPLETE_TRACE + " VALUES (?, NULL)");

  public VoltTable run(String partitionValue, int chunkSize, long minAgeSeconds,
      long maxAgeSeconds) {
    if (minAgeSeconds <= 0) throw new VoltAbortException("minAgeSeconds must be > 0");
    if (maxAgeSeconds <= 0) throw new VoltAbortException("maxAgeSeconds must be > 0");

    voltQueueSQL(oldTraceIds, minAgeSeconds - 1, chunkSize);
    VoltTable oldTraceIdTable = voltExecuteSQL()[0];

    VoltTable result = new VoltTable(new VoltTable.ColumnInfo("trace_id", VoltType.STRING));
    if (oldTraceIdTable.getRowCount() == 0) return result; // no rows

    SpanNode.Builder nodeBuilder = SpanNode.newBuilder(LOG);

    List<String> traceIds = new ArrayList<>();
    while (oldTraceIdTable.advanceRow()) {
      long age_seconds = oldTraceIdTable.getLong(0);
      String trace_id = oldTraceIdTable.getString(1);

      if (age_seconds < maxAgeSeconds) {
        voltQueueSQL(minimalSpanFields, trace_id);

        VoltTable spansTable = voltExecuteSQL()[0];
        List<Span> spans = new ArrayList<>();
        String missingTimestamp = null, missingDuration = null;
        while (spansTable.advanceRow()) {
          Span span = Span.newBuilder()
              .traceId(trace_id)
              .parentId(maybeNull(spansTable, 0))
              .id(spansTable.getString(1)).build();

          spansTable.getTimestampAsLong(2);
          if (spansTable.wasNull()) {
            missingTimestamp = span.id();
            break;
          }
          spansTable.getLong(3);
          if (spansTable.wasNull()) {
            missingDuration = span.id();
            break;
          }

          spans.add(span);
        }

        if (missingTimestamp != null) {
          if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(trace_id + "/" + missingTimestamp + " is missing its timestamp");
          }
          continue;
        }

        if (missingDuration != null) {
          if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(trace_id + "/" + missingDuration + " is missing its duration");
          }
          continue;
        }

        if (spans.isEmpty()) {
          if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(trace_id + " had no valid spans");
          }
          continue;
        }

        SpanNode node = nodeBuilder.build(spans);

        if (node.span() == null) {
          if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(trace_id + " missing a root span");
          }
          continue;
        }

        String rootSpanId = node.span().id();
        String missingParent = null;
        for (SpanNode child : node.children()) {
          if (!rootSpanId.equals(child.span().parentId())) {
            missingParent = child.span().id();
            break;
          }
        }

        if (missingParent != null) {
          if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(trace_id + "/" + missingParent + " is missing its parent");
          }
          continue;
        }

        // TODO: check for messaging spans as this isn't quite correct, but good enough for now
      }
      traceIds.add(trace_id);
    }
    for (String trace_id : traceIds) {
      voltQueueSQL(deletePendingTrace, EXPECT_SCALAR_MATCH(1), trace_id);
      voltQueueSQL(updateCompleteTrace, EXPECT_SCALAR_MATCH(1), trace_id);
      result.addRow(trace_id);
    }
    voltExecuteSQL(true);
    return result;
  }
}
