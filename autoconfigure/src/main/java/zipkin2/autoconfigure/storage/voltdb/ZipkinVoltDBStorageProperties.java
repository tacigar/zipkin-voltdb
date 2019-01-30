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
package zipkin2.autoconfigure.storage.voltdb;

import java.io.Serializable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin2.storage.voltdb.VoltDBStorage;

import static zipkin2.storage.voltdb.VoltDBStorage.Builder;

@ConfigurationProperties("zipkin.storage.voltdb")
class ZipkinVoltDBStorageProperties implements Serializable { // for Spark jobs
  private static final long serialVersionUID = 0L;

  private String host = "localhost";
  private boolean ensureSchema = true;

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public boolean isEnsureSchema() {
    return ensureSchema;
  }

  public void setEnsureSchema(boolean ensureSchema) {
    this.ensureSchema = ensureSchema;
  }

  public Builder toBuilder() {
    return VoltDBStorage.newBuilder().host(host).ensureSchema(ensureSchema);
  }
}
