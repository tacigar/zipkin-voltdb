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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponseWithPartitionKey;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.TRACE;
import static zipkin2.storage.voltdb.ITCompletePendingTraces.getStrings;
import static zipkin2.storage.voltdb.ITCompletePendingTraces.getTraceIds;
import static zipkin2.storage.voltdb.ITLinkTrace.assertLinksTableConsistentWith;
import static zipkin2.storage.voltdb.Schema.TABLE_COMPLETE_TRACE;
import static zipkin2.storage.voltdb.Schema.TABLE_DEPENDENCY_LINK;
import static zipkin2.storage.voltdb.Schema.TABLE_PENDING_EXPORT;
import static zipkin2.storage.voltdb.VoltDBStorage.executeAdHoc;

abstract class ITProcessCompleteTraces {
  int maxTraces = 5;

  abstract VoltDBStorage storage();

  @Test public void processesCompleteTrace_doesNotExportUnsampledTrace() throws Exception {
    String traceId = TRACE.get(0).traceId();

    storage().spanConsumer().accept(TRACE).execute();

    markComplete(traceId, false);

    assertThat(callProcessCompleteTraces(maxTraces))
        .flatExtracting(l -> l)
        .containsExactly(traceId);

    // We don't add to the pending export table when not sampled for export.
    assertThat(getTraceIds(storage(), TABLE_PENDING_EXPORT)).isEmpty();
    // Dependency links are derived even when unsampled
    assertLinksTableConsistentWith(storage().client, TRACE);
  }

  @Test public void processesCompleteTrace_updatesPendingExportWhenSampled() throws Exception {
    String traceId = TRACE.get(0).traceId();

    storage().spanConsumer().accept(TRACE).execute();

    markComplete(traceId, true);

    assertThat(callProcessCompleteTraces(maxTraces))
        .flatExtracting(l -> l)
        .containsExactly(traceId);

    assertThat(getTraceIds(storage(), TABLE_PENDING_EXPORT)).containsExactly(traceId);
    assertLinksTableConsistentWith(storage().client, TRACE);
  }

  /** The complete trace table holds trace IDs and if they have been sampled or processed. */
  void markComplete(String traceId, boolean sampled) throws Exception {
    executeAdHoc(client(), String.format(
        "UPSERT INTO %s VALUES ('%s', %s, %s)", TABLE_COMPLETE_TRACE, traceId, 1 /* dirty */,
        sampled ? 1 : 0));
  }

  @Test public void ignoresProcessedTrace() throws Exception {
    String traceId = TRACE.get(0).traceId();

    storage().spanConsumer().accept(TRACE).execute();

    markProcessed(traceId);

    assertThat(callProcessCompleteTraces(maxTraces))
        .flatExtracting(l -> l)
        .isEmpty();

    assertLinksTableConsistentWith(storage().client, Collections.emptyList());
  }

  void markProcessed(String traceId) throws Exception {
    executeAdHoc(client(), "UPSERT INTO " + TABLE_COMPLETE_TRACE
        + " VALUES ('" + traceId + "', 0, 1)");
  }

  @Test public void completePendingTrace_maxTraces() throws Exception {
    int traceCount = 100;
    for (int i = 0; i < traceCount; i++) {
      String traceId = Long.toHexString(-(i + 1));
      List<Span> trace = TRACE.stream().map(s -> s.toBuilder().traceId(traceId).build())
          .collect(Collectors.toList());
      storage().spanConsumer().accept(trace).execute();
      markComplete(traceId, true);
    }

    List<List<String>> partitionToTraceIds = callProcessCompleteTraces(maxTraces);
    // check each partition completed up to the requested chunk size of traces
    assertThat(partitionToTraceIds)
        .allSatisfy(l -> assertThat(l).hasSize(maxTraces));
    int totalProcessed = partitionToTraceIds.stream().mapToInt(Collection::size).sum();

    assertThat(getStrings(
        executeAdHoc(client(), "SELECT DISTINCT trace_id from " + TABLE_DEPENDENCY_LINK)))
        .hasSize(totalProcessed);
    assertThat(getStrings(executeAdHoc(client(), "SELECT trace_id from " + TABLE_COMPLETE_TRACE
        + " WHERE is_dirty = 1")))
        .hasSize(traceCount - totalProcessed);
  }

  List<List<String>> callProcessCompleteTraces(int maxTraces) throws Exception {
    ClientResponseWithPartitionKey[] responses = client().callAllPartitionProcedure(
        Schema.PROCEDURE_PROCESS_COMPLETE_TRACES, maxTraces);
    List<List<String>> result = new ArrayList<>();
    for (ClientResponseWithPartitionKey response : responses) {
      result.add(getStrings(response.response));
    }
    return result;
  }

  Client client() {
    return storage().client;
  }
}
