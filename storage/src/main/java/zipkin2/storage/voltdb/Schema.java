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

import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;

import static zipkin2.storage.voltdb.VoltDBStorage.executeAdHoc;

public final class Schema {
  static final Logger LOG = Logger.getLogger(Schema.class.getName());
  static final String SCHEMA_RESOURCE = "/ddl.sql";
  static final String
      TABLE_SPAN = "Span",
      TABLE_PENDING_TRACE = "PendingTrace",
      TABLE_COMPLETE_TRACE = "CompleteTrace",
      TABLE_DEPENDENCY_LINK = "DependencyLink",
      PROCEDURE_STORE_SPAN = "StoreSpansJson",
      PROCEDURE_GET_SPAN = "GetSpanJson",
      PROCEDURE_GET_SERVICE_NAMES = "GetServiceNames",
      PROCEDURE_GET_SPAN_NAMES = "GetSpanNames",
      PROCEDURE_GET_SPANS = "GetSpansJson",
      PROCEDURE_GET_DEPENDENCY_LINKS = "GetDependencyLinks",
      PROCEDURE_LINK_TRACE = "LinkTrace",
      PROCEDURE_COMPLETE_PENDING_TRACES = "CompletePendingTraces",
      PROCEDURE_LINK_COMPLETE_TRACES = "LinkCompleteTraces";

  static void ensureExists(Client client, String host) {
    try {
      executeAdHoc(client, "Select count(*) from " + Schema.TABLE_SPAN);
    } catch (ProcCallException e) {
      if (e.getMessage().contains("object not found")) {
        LOG.info("Installing schema " + SCHEMA_RESOURCE + " on host " + host);
        try {
          applySqlFile(client, SCHEMA_RESOURCE);

          // Install Java procedures. Note: we intentionally don't reference the types
          // If we did, we'd depend on the very large voltdb server jar at runtime.
          new InstallJavaProcedure(client, PROCEDURE_GET_SERVICE_NAMES).install();
          new InstallJavaProcedure(client, PROCEDURE_GET_SPANS).install();
          new InstallJavaProcedure(client, PROCEDURE_STORE_SPAN)
              .withPartition("TABLE " + Schema.TABLE_SPAN + " COLUMN trace_id")
              .install();
          new InstallJavaProcedure(client, PROCEDURE_LINK_TRACE)
              .withPartition("TABLE " + Schema.TABLE_DEPENDENCY_LINK + " COLUMN trace_id")
              .withSuperType("BaseLinkTrace")
              .addZipkin()
              .install();
          new InstallJavaProcedure(client, PROCEDURE_COMPLETE_PENDING_TRACES)
              .withPartition("TABLE " + Schema.TABLE_PENDING_TRACE + " COLUMN trace_id")
              .install();
          new InstallJavaProcedure(client, PROCEDURE_LINK_COMPLETE_TRACES)
              .withPartition("TABLE " + Schema.TABLE_COMPLETE_TRACE + " COLUMN trace_id")
              .withSuperType("BaseLinkTrace")
              .addZipkin()
              .install();
        } catch (Exception e1) {
          LOG.log(Level.SEVERE, e.getMessage(), e1);
        }
      }
    } catch (Exception e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
    }
  }

  static void applySqlFile(Client client, String resource) throws Exception {
    try (Scanner scanner = new Scanner(Schema.class.getResourceAsStream(resource), "UTF-8")) {
      for (String cmd : scanner.useDelimiter("\\A").next().split(";", -1)) {
        if (cmd.trim().isEmpty()) continue;
        executeAdHoc(client, cmd);
      }
    }
  }

  Schema() {
  }
}
