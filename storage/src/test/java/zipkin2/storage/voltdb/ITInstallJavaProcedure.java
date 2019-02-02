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
import org.junit.Test;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.storage.SpanStore;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.CLIENT_SPAN;
import static zipkin2.TestObjects.UTF_8;

abstract class ITInstallJavaProcedure {

  abstract VoltDBStorage storage();

  @Test public void getTrace_javaProcedure() throws Exception {
    InstallJavaProcedure.installProcedure(storage().client, GetTrace.class,
        "TABLE " + Schema.TABLE_SPAN + " COLUMN trace_id PARAMETER 0");

    assertThat(store().getTrace(CLIENT_SPAN.traceId()).execute())
        .isEmpty();

    assertThat(callGetTrace(CLIENT_SPAN.traceId()).getResults())
        .isEmpty();

    accept(CLIENT_SPAN);

    assertThat(store().getTrace(CLIENT_SPAN.traceId()).execute())
        .containsExactly(CLIENT_SPAN);

    assertThat(callGetTrace(CLIENT_SPAN.traceId()).getResults())
        .hasSize(1)
        .flatExtracting(t -> {
          t.advanceRow();
          String jsonList = t.get(0, VoltType.STRING).toString();
          return SpanBytesDecoder.JSON_V2.decodeList(jsonList.getBytes(UTF_8));
        })
        .containsOnly(CLIENT_SPAN);
  }

  ClientResponse callGetTrace(String traceId) throws IOException, ProcCallException {
    ClientResponse response = storage().client.callProcedure("GetTrace", traceId);
    if (response.getStatus() != ClientResponse.SUCCESS) {
      throw new RuntimeException("GetTrace resulted in " + response.getStatus());
    }
    return response;
  }

  void accept(Span... spans) throws IOException {
    storage().spanConsumer().accept(asList(spans)).execute();
  }

  SpanStore store() {
    return storage().spanStore();
  }
}
