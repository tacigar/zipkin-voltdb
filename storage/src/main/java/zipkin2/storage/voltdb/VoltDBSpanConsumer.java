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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.voltdb.internal.AggregateCall;

import static zipkin2.storage.voltdb.Schema.PROCEDURE_STORE_SPAN;

final class VoltDBSpanConsumer implements SpanConsumer {
  static final ThreadLocal<MessageDigest> MD5 = new ThreadLocal<MessageDigest>() {
    @Override protected MessageDigest initialValue() {
      try {
        return MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    }
  };

  final Client client;

  VoltDBSpanConsumer(VoltDBStorage storage) {
    client = storage.client;
  }

  @Override public Call<Void> accept(List<Span> spans) {
    if (spans.isEmpty()) return Call.create(null);
    List<Call<Void>> calls = new ArrayList<>();
    for (Span span : spans) calls.add(StoreSpanJsonCall.create(client, span));
    return AggregateCall.create(calls);
  }

  static final class StoreSpanJsonCall extends VoltDBCall<Void> implements Call.ErrorHandler<Void> {
    static Call<Void> create(Client client, Span span) {
      byte[] json = SpanBytesEncoder.JSON_V2.encode(span);
      byte[] md5 = MD5.get().digest(json);
      String kind = span.kind() != null ? span.kind().name() : null;
      StoreSpanJsonCall result = new StoreSpanJsonCall(
          client, span.traceId(), span.parentId(), span.id(), kind,
          span.localServiceName(), span.remoteServiceName(), span.name(),
          span.timestamp(), span.duration(),
          span.tags().containsKey("error") ? 1 : 0,
          md5, json);
      return result.handleError(result);
    }

    StoreSpanJsonCall(Client client, Object... parameters) {
      super(client, PROCEDURE_STORE_SPAN, parameters);
    }

    @Override public void onErrorReturn(Throwable error, Callback<Void> callback) {
      if (error.getMessage().contains("CONSTRAINT VIOLATION")) {
        callback.onSuccess(null); // ignore dupes
      } else {
        callback.onError(error);
      }
    }

    @Override Void convert(ClientResponse response) {
      return null;
    }

    @Override public Call<Void> clone() {
      return new StoreSpanJsonCall(client, procName, parameters);
    }
  }
}
