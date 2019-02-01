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
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.voltdb.client.Client;
import zipkin2.Span;
import zipkin2.TestObjects;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanStore;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.CLIENT_SPAN;
import static zipkin2.TestObjects.DAY;
import static zipkin2.TestObjects.FRONTEND;
import static zipkin2.TestObjects.TODAY;

@RunWith(Enclosed.class)
public class ITVoltDBStorage {

  /** Written intentionally to allow you to run a single nested method via the CLI. See README */
  static VoltDBStorageRule classRule() {
    return new VoltDBStorageRule("voltdb/voltdb-community:8.4");
  }

  public static class ITEnsureSchema extends zipkin2.storage.voltdb.ITEnsureSchema {
    @ClassRule public static VoltDBStorageRule voltdb = classRule();

    @Override Client client() {
      return voltdb.storage.client;
    }
  }

  public static class ITInstallJavaProcedure extends zipkin2.storage.voltdb.ITInstallJavaProcedure {
    @ClassRule public static VoltDBStorageRule voltdb = classRule();

    @Override VoltDBStorage storage() {
      return voltdb.storage;
    }

    @Before public void clear() throws Exception {
      voltdb.clear();
    }
  }

  // This test is temporary playground to be removed later once things are implemented
  public static class ITBabySteps {
    @ClassRule public static VoltDBStorageRule voltdb = classRule();

    @Test public void checkWorks() throws IOException {
      accept(TestObjects.TRACE.toArray(new Span[0]));

      assertThat(store().getTrace(TestObjects.TRACE.get(0).traceId()).execute())
          .containsExactlyInAnyOrderElementsOf(TestObjects.TRACE);
    }

    @Test public void dupesOk() throws IOException {
      accept(TestObjects.TRACE.toArray(new Span[0]));
      accept(TestObjects.TRACE.toArray(new Span[0]));

      assertThat(store().getTrace(TestObjects.TRACE.get(0).traceId()).execute())
          .containsExactlyInAnyOrderElementsOf(TestObjects.TRACE);
    }

    @Test public void checkMinimalSpan() throws IOException {
      Span span = Span.newBuilder().traceId(-1L, 1L).id(1L).build();

      accept(span);

      assertThat(store().getTrace(span.traceId()).execute())
          .containsExactly(span);
    }

    @Test public void getTrace_returnsEmptyOnNotFound() throws IOException {
      assertThat(store().getTrace(CLIENT_SPAN.traceId()).execute())
          .isEmpty();

      accept(CLIENT_SPAN);

      assertThat(store().getTrace(CLIENT_SPAN.traceId()).execute())
          .containsExactly(CLIENT_SPAN);

      assertThat(store().getTrace(CLIENT_SPAN.traceId().substring(16)).execute())
          .isEmpty();
    }

    @Test public void getTraces_groupsTracesTogether() throws IOException {
      Span traceASpan1 = Span.newBuilder()
          .traceId("a")
          .id("1")
          .timestamp((TODAY + 1) * 1000L)
          .localEndpoint(FRONTEND)
          .build();
      Span traceASpan2 = traceASpan1.toBuilder().id("1").timestamp((TODAY + 2) * 1000L).build();
      Span traceBSpan1 = traceASpan1.toBuilder().traceId("b").build();
      Span traceBSpan2 = traceASpan2.toBuilder().traceId("b").build();

      accept(traceASpan1, traceBSpan1, traceASpan2, traceBSpan2);

      assertThat(store().getTraces(requestBuilder().endTs(TODAY + 3).build()).execute())
          .containsExactlyInAnyOrder(asList(traceASpan1, traceASpan2),
              asList(traceBSpan1, traceBSpan2));
    }

    /** Traces whose root span has timestamps between (endTs - lookback) and endTs are returned */
    @Test public void getTraces_endTsAndLookback() throws IOException {
      Span span1 = Span.newBuilder()
          .traceId("a")
          .id("1")
          .timestamp((TODAY + 1) * 1000L)
          .localEndpoint(FRONTEND)
          .build();
      Span span2 = span1.toBuilder().traceId("b").timestamp((TODAY + 2) * 1000L).build();
      accept(span1, span2);

      assertThat(store().getTraces(requestBuilder().endTs(TODAY).build()).execute())
          .isEmpty();
      assertThat(store().getTraces(requestBuilder().endTs(TODAY + 1).build()).execute())
          .extracting(t -> t.get(0).id())
          .containsExactly(span1.id());
      assertThat(store().getTraces(requestBuilder().endTs(TODAY + 2).build()).execute())
          .extracting(t -> t.get(0).id())
          .containsExactlyInAnyOrder(span1.id(), span2.id());
      assertThat(store().getTraces(requestBuilder().endTs(TODAY + 3).build()).execute())
          .extracting(t -> t.get(0).id())
          .containsExactlyInAnyOrder(span1.id(), span2.id());

      assertThat(store().getTraces(requestBuilder().endTs(TODAY).build()).execute())
          .isEmpty();
      assertThat(store().getTraces(requestBuilder().endTs(TODAY + 1).lookback(1).build()).execute())
          .extracting(t -> t.get(0).id())
          .containsExactly(span1.id());
      assertThat(store().getTraces(requestBuilder().endTs(TODAY + 2).lookback(1).build()).execute())
          .extracting(t -> t.get(0).id())
          .containsExactlyInAnyOrder(span1.id(), span2.id());
      assertThat(store().getTraces(requestBuilder().endTs(TODAY + 3).lookback(1).build()).execute())
          .extracting(t -> t.get(0).id())
          .containsExactlyInAnyOrder(span2.id());
    }

    @Before public void clear() throws Exception {
      voltdb.clear();
    }

    static QueryRequest.Builder requestBuilder() {
      return QueryRequest.newBuilder().endTs(TODAY + DAY).lookback(DAY * 2).limit(100);
    }

    void accept(Span... spans) throws IOException {
      voltdb.storage.spanConsumer().accept(asList(spans)).execute();
    }

    SpanStore store() {
      return voltdb.storage.spanStore();
    }
  }
}
