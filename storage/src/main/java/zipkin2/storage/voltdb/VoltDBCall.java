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
import java.util.Arrays;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import zipkin2.Call;
import zipkin2.Callback;

abstract class VoltDBCall<V> extends Call.Base<V> {
  final Client client;
  final String procName;
  final Object[] parameters;

  VoltDBCall(Client client, String procName, Object... parameters) {
    this.client = client;
    this.procName = procName;
    this.parameters = parameters;
  }

  @Override protected final V doExecute() throws IOException {
    try {
      ClientResponse response = client.callProcedure(procName, parameters);
      if (response.getStatus() != ClientResponse.SUCCESS) {
        throw new RuntimeException(procName + " " +
            Arrays.toString(parameters) + " resulted in " + response.getStatus());
      }
      return convert(response);
    } catch (ProcCallException e) {
      throw new IOException(e);
    }
  }

  abstract V convert(ClientResponse response);

  @Override protected void doEnqueue(Callback<V> callback) {
    class ProcedureCallbackAdapter implements ProcedureCallback {

      @Override public void clientCallback(ClientResponse clientResponse) {
        if (clientResponse.getStatus() == ClientResponse.SUCCESS) {
          callback.onSuccess(convert(clientResponse));
          return;
        }
        callback.onError(new RuntimeException(clientResponse.getStatusString()));
      }
    }

    try {
      if (!client.callProcedure(new ProcedureCallbackAdapter(), procName, parameters)) {
        throw new RuntimeException("procedure not callable");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
