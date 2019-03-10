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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
  // temporary as we need to think about who does the sampling logic and where
  static final CountingSampler SAMPLER = CountingSampler.create(0.01f); // export 1% of traces

  final SQLStmt oldTraceIds = new SQLStmt(
      "SELECT SINCE_EPOCH(SECOND, NOW) - SINCE_EPOCH(SECOND, update_ts) AS age_seconds, trace_id"
          + " FROM " + TABLE_PENDING_TRACE
          // TODO: why can't we use age_seconds in the where clause?
          + " WHERE SINCE_EPOCH(SECOND, NOW) - SINCE_EPOCH(SECOND, update_ts) > ? ORDER BY update_ts LIMIT ?");

  final SQLStmt minimalSpanFields = new SQLStmt(
      "SELECT MAX(parent_id), id, MIN(ts), MIN(duration) FROM " + TABLE_SPAN + " WHERE trace_id = ?"
          + " GROUP BY id");

  final SQLStmt selectSampledStatus = new SQLStmt(
      "SELECT trace_id, is_sampled from " + TABLE_COMPLETE_TRACE + " WHERE trace_id in ?");

  final SQLStmt deletePendingTrace = new SQLStmt(
      "DELETE FROM " + TABLE_PENDING_TRACE + " WHERE trace_id = ?;");

  final SQLStmt updateCompleteTrace = new SQLStmt( // 1 is the dirty bit
      "UPSERT INTO " + TABLE_COMPLETE_TRACE + " VALUES (?, 1, ?, ?)");

  public VoltTable run(String partitionKey, int maxTraces, long minAgeSeconds,
      long maxAgeSeconds) {
    if (maxTraces < 1) throw new VoltAbortException("maxTraces < 1");
    if (minAgeSeconds < 0) throw new VoltAbortException("minAgeSeconds < 0");
    if (maxAgeSeconds < minAgeSeconds) {
      throw new VoltAbortException("maxAgeSeconds < minAgeSeconds");
    }

    voltQueueSQL(oldTraceIds, minAgeSeconds - 1, maxTraces);

    VoltTable oldTraceIdTable = voltExecuteSQL()[0];

    VoltTable result = new VoltTable(
        new VoltTable.ColumnInfo("trace_id", VoltType.STRING),
        new VoltTable.ColumnInfo("is_sampled", VoltType.TINYINT)
    );
    if (oldTraceIdTable.getRowCount() == 0) return result; // no rows

    List<String> completeTraceIds = traceIdsToProcess(oldTraceIdTable, maxAgeSeconds);

    // Non-null sampling values are traces with late updates.
    Map<String, Byte> traceToSampledStatus = getSampledStatus(completeTraceIds);

    for (String trace_id : completeTraceIds) {
      voltQueueSQL(deletePendingTrace, EXPECT_SCALAR_MATCH(1), trace_id);
      Byte is_sampled = traceToSampledStatus.get(trace_id);
      if (is_sampled == null) { // then this is the first time we make a decision on this trace
        is_sampled = (byte) (SAMPLER.isSampled() ? 1 : 0);
        // TODO: a real sampling impl could consider the entire trace. We should make one.. perhaps
        // mimicking our SpanStore query logic.
      }
      voltQueueSQL(updateCompleteTrace, EXPECT_SCALAR_MATCH(1), trace_id, is_sampled);
      result.addRow(trace_id, is_sampled);
    }
    voltExecuteSQL(true);
    return result;
  }

  /**
   * This cycles through the {@code oldTraceIdTable}, looking for traces which are either
   * heuristically complete, or which haven't been written since {@code maxAgeSeconds}.
   */
  List<String> traceIdsToProcess(VoltTable oldTraceIdTable, long maxAgeSeconds) {
    SpanNode.Builder nodeBuilder = SpanNode.newBuilder(LOG);
    List<String> result = new ArrayList<>();
    while (oldTraceIdTable.advanceRow()) {
      long age_seconds = oldTraceIdTable.getLong(0);
      String trace_id = oldTraceIdTable.getString(1);

      if (age_seconds >= maxAgeSeconds || heuristicallyComplete(nodeBuilder, trace_id)) {
        result.add(trace_id);
      }
    }
    return result;
  }

  /**
   * This gets minimal span fields in the trace in order to tell if it is topologically complete,
   * and if there is a fair chance remote requests have completed.
   */
  boolean heuristicallyComplete(SpanNode.Builder nodeBuilder, String trace_id) {
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
      return false;
    }

    if (missingDuration != null) {
      if (LOG.isLoggable(Level.FINE)) {
        LOG.fine(trace_id + "/" + missingDuration + " is missing its duration");
      }
      return false;
    }

    if (spans.isEmpty()) {
      if (LOG.isLoggable(Level.FINE)) {
        LOG.fine(trace_id + " had no valid spans");
      }
      return false;
    }

    SpanNode node = nodeBuilder.build(spans);

    if (node.span() == null) {
      if (LOG.isLoggable(Level.FINE)) {
        LOG.fine(trace_id + " missing a root span");
      }
      return false;
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
      return false;
    }

    // TODO: check for messaging spans as this isn't quite correct, but good enough for now
    return true;
  }

  Map<String, Byte> getSampledStatus(List<String> traceIds) {
    voltQueueSQL(selectSampledStatus, (Object) traceIds.toArray(new String[0]));
    VoltTable selectSampledStatusTable = voltExecuteSQL()[0];
    if (selectSampledStatusTable.getRowCount() == 0) return Collections.emptyMap();

    Map<String, Byte> result = new LinkedHashMap<>();
    while (selectSampledStatusTable.advanceRow()) {
      String trace_id = selectSampledStatusTable.getString(0);
      Byte is_sampled = (Byte) selectSampledStatusTable.get(1, VoltType.TINYINT);
      result.put(trace_id, is_sampled);
    }
    return result;
  }
}
