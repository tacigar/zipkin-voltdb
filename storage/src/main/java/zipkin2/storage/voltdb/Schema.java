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
import zipkin2.storage.voltdb.procedure.CompletePendingTraces;
import zipkin2.storage.voltdb.procedure.GetServiceNames;
import zipkin2.storage.voltdb.procedure.GetSpansJson;
import zipkin2.storage.voltdb.procedure.LinkCompleteTraces;
import zipkin2.storage.voltdb.procedure.LinkTrace;
import zipkin2.storage.voltdb.procedure.StoreSpansJson;

import static zipkin2.storage.voltdb.VoltDBStorage.executeAdHoc;

public final class Schema {
  static final Logger LOG = Logger.getLogger(Schema.class.getName());
  static final Charset UTF_8 = Charset.forName("UTF-8");
  static final String SCHEMA_RESOURCE = "/ddl.sql";
  public static final String
      TABLE_SPAN = "Span",
      TABLE_PENDING_TRACE = "PendingTrace",
      TABLE_COMPLETE_TRACE = "CompleteTrace",
      TABLE_DEPENDENCY_LINK = "DependencyLink",
      PROCEDURE_STORE_SPAN = StoreSpansJson.class.getSimpleName(),
      PROCEDURE_GET_SPAN = "GetSpanJson",
      PROCEDURE_GET_SERVICE_NAMES = GetServiceNames.class.getSimpleName(),
      PROCEDURE_GET_SPAN_NAMES = "GetSpanNames",
      PROCEDURE_GET_SPANS = GetSpansJson.class.getSimpleName(),
      PROCEDURE_GET_DEPENDENCY_LINKS = "GetDependencyLinks",
      PROCEDURE_LINK_TRACE = LinkTrace.class.getSimpleName(),
      PROCEDURE_COMPLETE_PENDING_TRACES = CompletePendingTraces.class.getSimpleName(),
      PROCEDURE_LINK_COMPLETE_TRACES = LinkCompleteTraces.class.getSimpleName();

  static void ensureExists(Client client, String host) {
    try {
      executeAdHoc(client, "Select count(*) from " + Schema.TABLE_SPAN);
    } catch (ProcCallException e) {
      if (e.getMessage().contains("object not found")) {
        LOG.info("Installing schema " + SCHEMA_RESOURCE + " on host " + host);
        applySqlFile(client, SCHEMA_RESOURCE);
        try {
          InstallJavaProcedure.installProcedure(client, GetServiceNames.class, null, false);
          InstallJavaProcedure.installProcedure(client, GetSpansJson.class, null, false);
          InstallJavaProcedure.installProcedure(client, StoreSpansJson.class,
              "TABLE " + Schema.TABLE_SPAN + " COLUMN trace_id", false);
          InstallJavaProcedure.installProcedure(client, LinkTrace.class,
              "TABLE " + Schema.TABLE_DEPENDENCY_LINK + " COLUMN trace_id", true);
          InstallJavaProcedure.installProcedure(client, CompletePendingTraces.class,
              "TABLE " + Schema.TABLE_PENDING_TRACE + " COLUMN trace_id", false);
          InstallJavaProcedure.installProcedure(client, LinkCompleteTraces.class,
              "TABLE " + Schema.TABLE_COMPLETE_TRACE + " COLUMN trace_id", false);
        } catch (Exception e1) {
          LOG.log(Level.SEVERE, e.getMessage(), e1);
        }
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
    }
  }

  static void applySqlFile(Client client, String resource) {
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
