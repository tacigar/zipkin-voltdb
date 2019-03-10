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
import java.util.List;
import java.util.Locale;
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

import static zipkin2.storage.voltdb.Schema.PROCEDURE_GET_DEPENDENCY_LINKS;
import static zipkin2.storage.voltdb.Schema.PROCEDURE_GET_SERVICE_NAMES;
import static zipkin2.storage.voltdb.Schema.PROCEDURE_GET_SPAN;
import static zipkin2.storage.voltdb.Schema.PROCEDURE_GET_SPANS;
import static zipkin2.storage.voltdb.Schema.PROCEDURE_GET_SPAN_NAMES;

final class VoltDBSpanStore implements SpanStore {

  final Client client;
  final boolean searchEnabled = true;

  VoltDBSpanStore(VoltDBStorage storage) {
    client = storage.client;
  }

  @Override public Call<List<List<Span>>> getTraces(QueryRequest request) {
    if (!searchEnabled) return Call.emptyList();
    return new GetSpansJsonCall(client, request);
  }

  static final class GetSpansJsonCall extends VoltDBCall<List<List<Span>>> {
    final QueryRequest request;
    final Mapper<List<Span>, List<List<Span>>> groupByTraceId = GroupByTraceId.create(false);

    GetSpansJsonCall(Client client, QueryRequest request) {
      super(client, PROCEDURE_GET_SPANS, request.serviceName(), request.spanName(),
          request.endTs(), request.lookback(), request.limit());
      this.request = request;
    }

    @Override List<List<Span>> convert(ClientResponse response) {
      return groupByTraceId.map(decodeSpanJson(response));
    }

    @Override public Call<List<List<Span>>> clone() {
      return new GetSpansJsonCall(client, request);
    }

    @Override public String toString() {
      return "GetSpansJson(" + request + ")";
    }
  }

  @Override public Call<List<Span>> getTrace(String hexTraceId) {
    // make sure we have a 16 or 32 character trace ID
    return new GetSpanJsonCall(client, Span.normalizeTraceId(hexTraceId));
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
    if (!searchEnabled) return Call.emptyList();
    return new GetServiceNamesCall(client);
  }

  static final class GetServiceNamesCall extends VoltDBCall<List<String>> {

    GetServiceNamesCall(Client client) {
      super(client, PROCEDURE_GET_SERVICE_NAMES);
    }

    @Override List<String> convert(ClientResponse response) {
      return decodeStrings(response);
    }

    @Override public Call<List<String>> clone() {
      return new GetServiceNamesCall(client);
    }

    @Override public String toString() {
      return "GetServiceNames()";
    }
  }

  @Override public Call<List<String>> getSpanNames(String serviceName) {
    if (!searchEnabled) return Call.emptyList();
    return new GetSpanNamesCall(client, serviceName.toLowerCase(Locale.ROOT));
  }

  static final class GetSpanNamesCall extends VoltDBCall<List<String>> {
    final String serviceName;

    GetSpanNamesCall(Client client, String serviceName) {
      super(client, PROCEDURE_GET_SPAN_NAMES, serviceName, serviceName);
      this.serviceName = serviceName;
    }

    @Override List<String> convert(ClientResponse response) {
      return decodeStrings(response);
    }

    @Override public Call<List<String>> clone() {
      return new GetSpanNamesCall(client, serviceName);
    }

    @Override public String toString() {
      return "GetSpanNames(" + serviceName + ")";
    }
  }

  @Override public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    if (endTs <= 0) throw new IllegalArgumentException("endTs <= 0");
    if (lookback <= 0) throw new IllegalArgumentException("lookback <= 0");

    return new GetDependencyLinksCall(client, endTs, lookback);
  }

  static final class GetDependencyLinksCall extends VoltDBCall<List<DependencyLink>> {
    final long endTs, lookback;

    GetDependencyLinksCall(Client client, long endTs, long lookback) {
      super(client, PROCEDURE_GET_DEPENDENCY_LINKS, endTs - lookback, endTs);
      this.endTs = endTs;
      this.lookback = lookback;
    }

    @Override List<DependencyLink> convert(ClientResponse response) {
      List<DependencyLink> result = new ArrayList<>();
      for (VoltTable table : response.getResults()) {
        while (table.advanceRow()) {
          result.add(DependencyLink.newBuilder()
              .parent(table.getString(0))
              .child(table.getString(1))
              .callCount(table.getLong(2))
              .errorCount(table.getLong(3))
              .build());
        }
      }
      return result;
    }

    @Override public Call<List<DependencyLink>> clone() {
      return new GetDependencyLinksCall(client, endTs, lookback);
    }

    @Override public String toString() {
      return "GetDependencyLinks(" + endTs + ", " + lookback + ")";
    }
  }

  static List<Span> decodeSpanJson(ClientResponse response) {
    List<Span> result = new ArrayList<>();
    for (VoltTable table : response.getResults()) {
      while (table.advanceRow()) {
        byte[] json = table.getStringAsBytes(0);
        SpanBytesDecoder.JSON_V2.decode(json, result);
      }
    }
    return result;
  }

  static List<String> decodeStrings(ClientResponse response) {
    List<String> result = new ArrayList<>();
    for (VoltTable table : response.getResults()) {
      while (table.advanceRow()) {
        String string = (String) table.get(0, VoltType.STRING);
        if (string != null) result.add(string);
      }
    }
    return result;
  }
}
