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
import org.junit.Test;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import zipkin2.DependencyLink;
import zipkin2.internal.DependencyLinker;

import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.CLIENT_SPAN;
import static zipkin2.TestObjects.TRACE;
import static zipkin2.storage.voltdb.VoltDBStorage.executeAdHoc;

abstract class ITLinkTrace {

  abstract VoltDBStorage storage();

  @Test public void linkTrace_javaProcedure() throws Exception {
    storage().spanConsumer().accept(TRACE).execute();

    assertThat(callLinkTrace(CLIENT_SPAN.traceId()).getResults())
        .hasSize(2);

    VoltTable table = executeAdHoc(storage().client,
        "SELECT parent, child, call_count, error_count from DependencyLink").getResults()[0];

    List<DependencyLink> links = toDependencyLinks(table);
    assertThat(links).isEqualTo(new DependencyLinker().putTrace(TRACE).link());
  }

  ClientResponse callLinkTrace(String traceId) throws Exception {
    ClientResponse response = storage().client.callProcedure("LinkTrace", traceId);
    if (response.getStatus() != ClientResponse.SUCCESS) {
      throw new RuntimeException("LinkTrace resulted in " + response.getStatus());
    }
    return response;
  }

  List<DependencyLink> toDependencyLinks(VoltTable table) {
    List<DependencyLink> result = new ArrayList<>();
    while (table.advanceRow()) {
      DependencyLink.Builder builder = DependencyLink.newBuilder()
          .parent(table.getString(0))
          .child(table.getString(1))
          .callCount(table.getLong(2))
          .errorCount(table.getLong(3));
      if (table.wasNull()) builder.errorCount(0);
      result.add(builder.build());
    }
    return result;
  }
}
