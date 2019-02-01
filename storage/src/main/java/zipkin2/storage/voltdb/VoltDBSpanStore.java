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
import java.util.Collections;
import java.util.List;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.storage.GroupByTraceId;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanStore;

import static zipkin2.storage.voltdb.Schema.PROCEDURE_GET_SPAN;
import static zipkin2.storage.voltdb.Schema.PROCEDURE_GET_SPANS;
import static zipkin2.storage.voltdb.Schema.UTF_8;

final class VoltDBSpanStore implements SpanStore {

  final Client client;
  final boolean searchEnabled = true;

  VoltDBSpanStore(VoltDBStorage storage) {
    client = storage.client;
  }

  @Override public Call<List<List<Span>>> getTraces(QueryRequest request) {
    if (!searchEnabled) return Call.emptyList();
    return new GetSpansJsonCall(client, request.endTs(), request.lookback());
  }

  @Override public Call<List<Span>> getTrace(String hexTraceId) {
    // make sure we have a 16 or 32 character trace ID
    return new GetSpanJsonCall(client, Span.normalizeTraceId(hexTraceId));
  }

  static final class GetSpansJsonCall extends VoltDBCall<List<List<Span>>> {
    final long beginMillis, endMillis;
    final Mapper<List<Span>, List<List<Span>>> groupByTraceId = GroupByTraceId.create(false);

    GetSpansJsonCall(Client client, long beginMillis, long endMillis) {
      super(client, PROCEDURE_GET_SPANS, beginMillis, endMillis);
      this.beginMillis = beginMillis;
      this.endMillis = endMillis;
    }

    @Override List<List<Span>> convert(ClientResponse response) {
      return groupByTraceId.map(decodeSpanJson(response));
    }

    @Override public Call<List<List<Span>>> clone() {
      return new GetSpansJsonCall(client, beginMillis, endMillis);
    }

    @Override public String toString() {
      return "GetSpansJson(" + beginMillis + ", " + endMillis + ")";
    }
  }

  static final class GetSpanJsonCall extends VoltDBCall<List<Span>> {
    final String traceId;

    GetSpanJsonCall(Client client, String traceId) {
      super(client, PROCEDURE_GET_SPAN, traceId);
      this.traceId = traceId;
    }

    @Override List<Span> convert(ClientResponse response) {
      return decodeSpanJson(response);
    }

    @Override public Call<List<Span>> clone() {
      return new GetSpanJsonCall(client, traceId);
    }

    @Override public String toString() {
      return "GetSpanJson(" + traceId + ")";
    }
  }

  @Override public Call<List<String>> getServiceNames() {
    return Call.emptyList(); // search is disabled except trace ID
  }

  @Override public Call<List<String>> getSpanNames(String serviceName) {
    return Call.emptyList(); // search is disabled except trace ID
  }

  @Override public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    return Call.emptyList(); // TODO: aggregate
  }

  static List<Span> decodeSpanJson(ClientResponse response) {
    VoltTable table = response.getResults()[0];
    if (table.getRowCount() == 0) return Collections.emptyList();

    List<Span> spans = new ArrayList<>(table.getRowCount());
    while (table.advanceRow()) {
      String json = (String) table.get(0, VoltType.STRING);
      SpanBytesDecoder.JSON_V2.decode(json.getBytes(UTF_8), spans);
    }
    return spans;
  }
}
