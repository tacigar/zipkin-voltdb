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
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import zipkin2.CheckResult;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;

public final class VoltDBStorage extends StorageComponent {
  static final Logger LOG = Logger.getLogger(VoltDBStorage.class.getName());

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder extends StorageComponent.Builder {
    String host = "localhost:21212";
    boolean ensureSchema = true;

    @Override public Builder strictTraceId(boolean strictTraceId) {
      if (!strictTraceId) throw new IllegalArgumentException("unstrict trace ID not supported");
      return this;
    }

    @Override public Builder searchEnabled(boolean searchEnabled) {
      if (searchEnabled) throw new IllegalArgumentException("search not supported");
      return this;
    }

    @Override public Builder autocompleteKeys(List<String> keys) {
      if (keys == null) throw new NullPointerException("keys == null");
      if (!keys.isEmpty()) throw new IllegalArgumentException("autocomplete not supported");
      return this;
    }

    /**
     * Hostname or IP of the host to connect to including optional port in the hostname:port
     * format.
     */
    public Builder host(String host) {
      if (host == null) throw new NullPointerException("host == null");
      this.host = host;
      return this;
    }

    /**
     * When true executes io.zipkin2:zipkin-storage-voltdb/ddl.sql if needed. Defaults to true.
     */
    public Builder ensureSchema(boolean ensureSchema) {
      this.ensureSchema = ensureSchema;
      return this;
    }

    @Override public VoltDBStorage build() {
      return new VoltDBStorage(this);
    }

    Builder() {
    }
  }

  final Client client;
  final String host;
  final boolean ensureSchema;

  VoltDBStorage(VoltDBStorage.Builder builder) {
    client = ClientFactory.createClient(new ClientConfig());
    host = builder.host;
    ensureSchema = builder.ensureSchema;
  }

  final AtomicBoolean connected = new AtomicBoolean();

  void connect() {
    if (connected.compareAndSet(false, true)) {
      try {
        client.createConnection(host);
      } catch (Exception e) {
        throw new RuntimeException("Unable to establish connection to VoltDB server", e);
      }
      if (ensureSchema) {
        Schema.ensureExists(client, host);
      } else {
        LOG.fine("Skipping schema check as ensureSchema was false");
      }
    }
  }

  @Override public SpanStore spanStore() {
    connect();
    return new VoltDBSpanStore(this);
  }

  @Override public SpanConsumer spanConsumer() {
    connect();
    return new VoltDBSpanConsumer(this);
  }

  @Override public CheckResult check() {
    connect();
    try {
      executeAdHoc(client, "Select count(*) from " + Schema.TABLE_SPAN);
    } catch (Exception e) {
      return CheckResult.failed(e);
    }
    return CheckResult.OK;
  }

  @Override public void close() {
    try {
      // block until all outstanding txns return
      client.drain();
      // close down the client connections
      client.close();
    } catch (Exception | Error e) {
      LOG.log(Level.WARNING, "error closing client " + e.getMessage(), e);
    }
  }

  static ClientResponse executeAdHoc(Client client, Object... parameters)
      throws IOException, ProcCallException {
    ClientResponse response = client.callProcedure("@AdHoc", parameters);
    if (response.getStatus() != ClientResponse.SUCCESS) {
      throw new IllegalStateException("@AdHoc" +
          Arrays.toString(parameters) + " resulted in " + response.getStatus());
    }
    return response;
  }
}
