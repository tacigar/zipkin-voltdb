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
import static zipkin2.storage.voltdb.ITLinkTrace.assertLinksTableConsistentWith;
import static zipkin2.storage.voltdb.Schema.TABLE_COMPLETE_TRACE;
import static zipkin2.storage.voltdb.Schema.TABLE_DEPENDENCY_LINK;
import static zipkin2.storage.voltdb.VoltDBStorage.executeAdHoc;

abstract class ITLinkCompleteTraces {
  int chunkSize = 5;

  abstract VoltDBStorage storage();

  @Test public void linksCompleteTrace() throws Exception {
    String traceId = TRACE.get(0).traceId();

    storage().spanConsumer().accept(TRACE).execute();

    markComplete(traceId);

    assertThat(callLinkCompleteTraces(chunkSize))
        .flatExtracting(l -> l)
        .containsExactly(traceId);

    assertLinksTableConsistentWith(storage().client, TRACE);
  }

  void markComplete(String traceId) throws Exception {
    executeAdHoc(client(), "UPSERT INTO " + TABLE_COMPLETE_TRACE
        + " VALUES ('" + traceId + "', NULL)");
  }

  @Test public void ignoresProcessedTrace() throws Exception {
    String traceId = TRACE.get(0).traceId();

    storage().spanConsumer().accept(TRACE).execute();

    markProcessed(traceId);

    assertThat(callLinkCompleteTraces(chunkSize))
        .flatExtracting(l -> l)
        .isEmpty();

    assertLinksTableConsistentWith(storage().client, Collections.emptyList());
  }

  void markProcessed(String traceId) throws Exception {
    executeAdHoc(client(), "UPSERT INTO " + TABLE_COMPLETE_TRACE
        + " VALUES ('" + traceId + "', NOW)");
  }

  @Test public void completePendingTrace_chunkSize() throws Exception {
    int traceCount = 100;
    for (int i = 0; i < traceCount; i++) {
      String traceId = Long.toHexString(-(i + 1));
      List<Span> trace = TRACE.stream().map(s -> s.toBuilder().traceId(traceId).build())
          .collect(Collectors.toList());
      storage().spanConsumer().accept(trace).execute();
      markComplete(traceId);
    }

    List<List<String>> partitionToTraceIds = callLinkCompleteTraces(chunkSize);
    // check each partition completed up to the requested chunk size of traces
    assertThat(partitionToTraceIds)
        .allSatisfy(l -> assertThat(l).hasSize(chunkSize));
    int totalProcessed = partitionToTraceIds.stream().mapToInt(Collection::size).sum();

    assertThat(getStrings(
        executeAdHoc(client(), "SELECT DISTINCT trace_id from " + TABLE_DEPENDENCY_LINK)))
        .hasSize(totalProcessed);
    assertThat(getStrings(executeAdHoc(client(), "SELECT trace_id from " + TABLE_COMPLETE_TRACE
        + " WHERE process_ts IS NULL")))
        .hasSize(traceCount - totalProcessed);
  }

  List<List<String>> callLinkCompleteTraces(int chunkSize) throws Exception {
    ClientResponseWithPartitionKey[] responses = client().callAllPartitionProcedure(
        Schema.PROCEDURE_LINK_COMPLETE_TRACES, chunkSize);
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
