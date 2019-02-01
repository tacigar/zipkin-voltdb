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

import com.google_voltpatches.common.io.CharStreams;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;

import static zipkin2.storage.voltdb.VoltDBStorage.executeAdHoc;

final class Schema {
  static final Logger LOG = Logger.getLogger(Schema.class.getName());
  static final Charset UTF_8 = Charset.forName("UTF-8");
  static final String SCHEMA_RESOURCE = "/ddl.sql";
  static final String
      TABLE_SPAN = "Span",
      PROCEDURE_STORE_SPAN = "StoreSpanJson",
      PROCEDURE_GET_SPAN = "GetSpanJson",
      PROCEDURE_GET_SPANS = "GetSpansJson";

  static void ensureExists(Client client, String host) {
    try {
      executeAdHoc(client, "Select count(*) from " + Schema.TABLE_SPAN);
    } catch (ProcCallException e) {
      if (e.getMessage().contains("object not found")) {
        LOG.info("Installing schema " + SCHEMA_RESOURCE + " on host " + host);
        applyCqlFile(client, SCHEMA_RESOURCE);
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
    }
  }

  static void applyCqlFile(Client client, String resource) {
    try (Reader reader = new InputStreamReader(Schema.class.getResourceAsStream(resource), UTF_8)) {
      for (String cmd : CharStreams.toString(reader).split(";", -1)) {
        if (cmd.trim().isEmpty()) continue;
        executeAdHoc(client, cmd);
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
    }
  }

  Schema() {
  }
}
