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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.Test;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientResponseWithPartitionKey;
import org.voltdb.client.ProcCallException;
import zipkin2.Span;

import static java.util.Arrays.asList;
import static java.util.Arrays.copyOfRange;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.LOTS_OF_SPANS;
import static zipkin2.TestObjects.TODAY;
import static zipkin2.TestObjects.TRACE;
import static zipkin2.storage.voltdb.Schema.TABLE_COMPLETE_TRACE;
import static zipkin2.storage.voltdb.Schema.TABLE_PENDING_TRACE;
import static zipkin2.storage.voltdb.VoltDBStorage.executeAdHoc;

abstract class ITCompletePendingTraces {
  int chunkSize = 5, minAgeSeconds = 5, maxAgeSeconds = 30;

  abstract VoltDBStorage storage();

  @Test public void doesntCompleteWhenInUpdateWindow() throws Exception {
    String traceId = TRACE.get(0).traceId();
    storage().spanConsumer().accept(TRACE).execute();

    expectNoopOnCompletePendingTraces(traceId);
  }

  @Test public void completesPendingTraceWhenOutsideWindow() throws Exception {
    String traceId = TRACE.get(0).traceId();
    storage().spanConsumer().accept(TRACE).execute();

    // change so that the trace is older than the check interval
    agePendingTraces(minAgeSeconds);

    expectCompletePendingTraces(traceId);
  }

  @Test public void doesntCompleteUntilAllSpansHaveDuration() throws Exception {
    // root is finished
    Span root = Span.newBuilder().traceId("a").id("a").timestamp(TODAY).duration(10).build();
    // but child is not
    Span child = root.toBuilder().parentId("a").id("b").duration(null).build();

    storage().spanConsumer().accept(asList(root, child)).execute();

    agePendingTraces(minAgeSeconds);

    expectNoopOnCompletePendingTraces(root.traceId());

    // finish child, but only record its duration, not its parent ID
    storage().spanConsumer()
        .accept(asList(Span.newBuilder().traceId("a").id("b").duration(2).build()))
        .execute();

    agePendingTraces(minAgeSeconds);

    expectCompletePendingTraces(root.traceId());
  }

  @Test public void completesIncompleteAfterMaxWindow() throws Exception {
    Span root = Span.newBuilder().traceId("a").id("a").timestamp(TODAY).duration(10L).build();
    List<Span> spans = asList(
        root.toBuilder().parentId("a").id("b").build(),
        root.toBuilder().parentId("b").id("c").build(),
        root.toBuilder().parentId("c").id("d").build());
    storage().spanConsumer().accept(spans).execute();

    agePendingTraces(maxAgeSeconds);

    // reports eventhough missing root
    expectCompletePendingTraces(root.traceId());
  }

  @Test public void doesntCompleteUntilRootIsReported() throws Exception {
    Span root = Span.newBuilder().traceId("a").id("a").timestamp(TODAY).duration(10L).build();
    List<Span> spans = asList(
        root.toBuilder().parentId("a").id("b").build(),
        root.toBuilder().parentId("b").id("c").build(),
        root.toBuilder().parentId("c").id("d").build());
    storage().spanConsumer().accept(spans).execute();

    agePendingTraces(minAgeSeconds);

    expectNoopOnCompletePendingTraces(root.traceId());

    // report the root
    storage().spanConsumer().accept(asList(root)).execute();

    agePendingTraces(minAgeSeconds);

    expectCompletePendingTraces(root.traceId());
  }

  @Test public void resetsProcessingTimeOnLateUpdate() throws Exception {
    Span root = Span.newBuilder().traceId("a").id("a").timestamp(TODAY).duration(10L).build();
    List<Span> spans = asList(
        root,
        root.toBuilder().parentId("a").id("b").build(),
        root.toBuilder().parentId("b").id("c").build(),
        root.toBuilder().parentId("c").id("d").build());
    storage().spanConsumer().accept(spans).execute();

    agePendingTraces(minAgeSeconds);

    expectCompletePendingTraces(root.traceId());

    executeAdHoc(storage().client, "UPDATE " + TABLE_COMPLETE_TRACE
        + " SET process_ts = NOW");

    // report a late span
    storage().spanConsumer()
        .accept(asList(root.toBuilder().parentId("d").id("e").build()))
        .execute();

    agePendingTraces(minAgeSeconds);

    expectCompletePendingTraces(root.traceId());

    // unset processing flag alerts we should look again
    VoltTable table = executeAdHoc(storage().client,
        "SELECT process_ts FROM " + TABLE_COMPLETE_TRACE).getResults()[0];
    assertThat(table.advanceRow()).isTrue();
    table.getTimestampAsLong(0);
    assertThat(table.wasNull()).isTrue();
  }

  @Test public void doesntCompleteUntilOrphanIsReported() throws Exception {
    Span root = Span.newBuilder().traceId("a").id("a").timestamp(TODAY).duration(10L).build();
    Span late = root.toBuilder().parentId("b").id("c").build();
    List<Span> spans = asList(
        root,
        root.toBuilder().parentId("a").id("b").build(),
        root.toBuilder().parentId("c").id("d").build());
    storage().spanConsumer().accept(spans).execute();

    agePendingTraces(minAgeSeconds);

    expectNoopOnCompletePendingTraces(root.traceId());

    // report the late span
    storage().spanConsumer().accept(asList(late)).execute();

    agePendingTraces(minAgeSeconds);

    expectCompletePendingTraces(root.traceId());
  }

  @Test public void completePendingTrace_chunkSize() throws Exception {
    int traceCount = 100;
    storage().spanConsumer().accept(asList(copyOfRange(LOTS_OF_SPANS, 0, traceCount))).execute();

    agePendingTraces(minAgeSeconds);

    List<List<String>> partitionToTraceIds =
        completePendingTraces(chunkSize, minAgeSeconds, maxAgeSeconds);
    // check each partition completed up to the requested chunk size of traces
    assertThat(partitionToTraceIds)
        .allSatisfy(l -> assertThat(l).hasSize(chunkSize));
    int totalProcessed = partitionToTraceIds.stream().mapToInt(Collection::size).sum();

    assertThat(getTraceIds(TABLE_PENDING_TRACE)).hasSize(traceCount - totalProcessed);
    assertThat(getTraceIds(TABLE_COMPLETE_TRACE)).hasSize(totalProcessed);
  }

  void agePendingTraces(int seconds) throws IOException, ProcCallException {
    // change so that the trace is older than the check interval
    executeAdHoc(storage().client, "UPDATE " + TABLE_PENDING_TRACE
        + " SET update_ts = dateadd(second, -" + seconds + ", NOW)");
  }

  void expectCompletePendingTraces(String traceId) throws Exception {
    assertThat(completePendingTraces(chunkSize, minAgeSeconds, maxAgeSeconds))
        .flatExtracting(l -> l)
        .containsExactly(traceId);

    assertThat(getTraceIds(TABLE_PENDING_TRACE)).isEmpty();
    assertThat(getTraceIds(TABLE_COMPLETE_TRACE)).containsExactly(traceId);
  }

  void expectNoopOnCompletePendingTraces(String traceId) throws Exception {
    assertThat(completePendingTraces(chunkSize, minAgeSeconds, maxAgeSeconds))
        .flatExtracting(l -> l)
        .isEmpty();

    assertThat(getTraceIds(TABLE_PENDING_TRACE)).containsExactly(traceId);
    assertThat(getTraceIds(TABLE_COMPLETE_TRACE)).isEmpty();
  }

  List<List<String>> completePendingTraces(int chunkSize, long minAgeSeconds, long maxAgeSeconds)
      throws Exception {
    ClientResponseWithPartitionKey[] responses = storage().client.callAllPartitionProcedure(
        Schema.PROCEDURE_COMPLETE_PENDING_TRACES, chunkSize, minAgeSeconds, maxAgeSeconds);
    List<List<String>> result = new ArrayList<>();
    for (ClientResponseWithPartitionKey response : responses) {
      result.add(getStrings(response.response));
    }
    return result;
  }

  List<String> getTraceIds(String table) throws Exception {
    return getStrings(executeAdHoc(storage().client, "SELECT trace_id from " + table));
  }

  static List<String> getStrings(ClientResponse response) {
    List<String> result = new ArrayList<>();
    assertThat(response.getStatus())
        .withFailMessage(response.getStatusString() + " " + response.getAppStatusString())
        .isEqualTo(ClientResponse.SUCCESS);
    VoltTable table = response.getResults()[0];
    while (table.advanceRow()) {
      result.add(table.getString(0));
    }
    return result;
  }
}
