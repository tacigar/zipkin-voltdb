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

import org.junit.Test;
import org.voltdb.client.Client;

import static zipkin2.storage.voltdb.VoltDBStorage.executeAdHoc;

abstract class ITEnsureSchema {

  abstract Client client();

  @Test public void installsTablesWhenMissing() throws Exception {
    executeAdHoc(client(), "Drop procedure " + Schema.PROCEDURE_STORE_SPAN);
    executeAdHoc(client(), "Drop procedure " + Schema.PROCEDURE_GET_SPAN);
    executeAdHoc(client(), "Drop procedure " + Schema.PROCEDURE_GET_SPANS);
    executeAdHoc(client(), "Drop procedure " + Schema.PROCEDURE_GET_SERVICE_NAMES);
    executeAdHoc(client(), "Drop procedure " + Schema.PROCEDURE_GET_SPAN_NAMES);
    executeAdHoc(client(), "Drop procedure " + Schema.PROCEDURE_GET_DEPENDENCY_LINKS);
    executeAdHoc(client(), "Drop procedure " + Schema.PROCEDURE_LINK_TRACE);
    executeAdHoc(client(), "Drop procedure " + Schema.PROCEDURE_COMPLETE_PENDING_TRACES);
    executeAdHoc(client(), "Drop procedure " + Schema.PROCEDURE_PROCESS_COMPLETE_TRACES);
    executeAdHoc(client(), "Drop table " + Schema.TABLE_SPAN);
    executeAdHoc(client(), "Drop table " + Schema.TABLE_DEPENDENCY_LINK);
    executeAdHoc(client(), "Drop table " + Schema.TABLE_PENDING_TRACE);
    executeAdHoc(client(), "Drop table " + Schema.TABLE_PENDING_EXPORT);
    executeAdHoc(client(), "Drop table " + Schema.TABLE_COMPLETE_TRACE);

    Schema.ensureExists(client(), "localhost");

    executeAdHoc(client(), "Select count(*) from " + Schema.TABLE_SPAN);
  }
}
