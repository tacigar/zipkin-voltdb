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

import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.scheduling.annotation.Scheduled;
import org.voltdb.VoltTable;
import org.voltdb.client.AllPartitionProcedureCallback;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientResponseWithPartitionKey;
import zipkin2.internal.Nullable;
import zipkin2.storage.voltdb.VoltDBStorage;

import static zipkin2.storage.voltdb.Schema.PROCEDURE_COMPLETE_PENDING_TRACES;
import static zipkin2.storage.voltdb.Schema.PROCEDURE_LINK_COMPLETE_TRACES;

final class VoltDBScheduledTasks {
  final Logger logger = Logger.getLogger(VoltDBScheduledTasks.class.getName());
  final VoltDBStorage storage;
  int maxPerPartition = 250;

  VoltDBScheduledTasks(VoltDBStorage storage) {
    this.storage = storage;
  }

  @Scheduled(fixedRate = 250) // 4 times a second * maxPerPartition * partition count
  public void completePendingTraces() throws Exception {
    Client client = tryClient();
    if (client == null) return;

    int minAgeSeconds = 3, maxAgeSeconds = 30;
    client.callAllPartitionProcedure(new LoggingCallback(PROCEDURE_COMPLETE_PENDING_TRACES),
        PROCEDURE_COMPLETE_PENDING_TRACES, maxPerPartition, minAgeSeconds, maxAgeSeconds);
  }

  @Scheduled(fixedRate = 250) // 4 times a second * maxPerPartition * partition count
  public void linkCompleteTraces() throws Exception {
    Client client = tryClient();
    if (client == null) return;

    client.callAllPartitionProcedure(new LoggingCallback(PROCEDURE_LINK_COMPLETE_TRACES),
        PROCEDURE_LINK_COMPLETE_TRACES, maxPerPartition);
  }

  // the client might fail for reasons such as not started yet or shutting down.
  @Nullable Client tryClient() {
    try {
      return storage.client();
    } catch (Exception ignored) {
      return null;
    }
  }

  final class LoggingCallback implements AllPartitionProcedureCallback {
    final String procedure;

    LoggingCallback(String procedure) {
      this.procedure = procedure;
    }

    @Override public void clientCallback(ClientResponseWithPartitionKey[] responses) {
      boolean shouldLogFine = logger.isLoggable(Level.FINE);

      for (ClientResponseWithPartitionKey resp : responses) {
        ClientResponse response = resp.response;

        if (response.getStatus() != ClientResponse.SUCCESS) {
          logger.log(Level.WARNING, "{0} on partition {1} failed with {2} {3}", new Object[] {
              procedure, resp.partitionKey, response.getStatusString(),
              response.getAppStatusString()
          });
        }

        VoltTable table = response.getResults()[0];
        int traceCount = table.getRowCount();
        if (!shouldLogFine || traceCount == 0) continue;

        logger.log(Level.FINE, "{0} on partition {1} processed {2} trace IDs",
            new Object[] {procedure, resp.partitionKey, traceCount}
        );
      }
    }
  }
}
