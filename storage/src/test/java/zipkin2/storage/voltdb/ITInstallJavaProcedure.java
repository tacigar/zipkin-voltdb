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
import org.voltdb.client.ProcCallException;
import zipkin2.storage.SpanStore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static zipkin2.storage.voltdb.Schema.PROCEDURE_GET_SERVICE_NAMES;
import static zipkin2.storage.voltdb.VoltDBStorage.executeAdHoc;

abstract class ITInstallJavaProcedure {

  abstract VoltDBStorage storage();

  @Test public void installsProcedure() throws Exception {
    executeAdHoc(storage().client, "Drop procedure " + PROCEDURE_GET_SERVICE_NAMES);

    try {
      store().getServiceNames().execute();

      failBecauseExceptionWasNotThrown(ProcCallException.class);
    } catch (IOException e) {
      assertThat(e).hasMessageContaining("Procedure GetServiceNames was not found");
    }

    new InstallJavaProcedure(storage().client, PROCEDURE_GET_SERVICE_NAMES).install();

    assertThat(store().getServiceNames().execute())
        .isEmpty(); // now we work again
  }

  SpanStore store() {
    return storage().spanStore();
  }
}
