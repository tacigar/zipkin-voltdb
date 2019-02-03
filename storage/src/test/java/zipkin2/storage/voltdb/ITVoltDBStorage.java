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
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.voltdb.client.Client;
import zipkin2.Span;
import zipkin2.TestObjects;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Enclosed.class)
public class ITVoltDBStorage {

  /** Written intentionally to allow you to run a single nested method via the CLI. See README */
  static VoltDBStorageRule classRule() {
    return new VoltDBStorageRule("voltdb/voltdb-community:8.4");
  }

  public static class ITSpanStore extends zipkin2.storage.ITSpanStore {
    @ClassRule public static VoltDBStorageRule voltdb = classRule();

    @Override public VoltDBStorage storage() {
      return voltdb.storage;
    }

    @Before public void clear() throws Exception {
      voltdb.clear();
    }

    @Test public void dupesOk() throws IOException {
      accept(TestObjects.TRACE.toArray(new Span[0]));
      accept(TestObjects.TRACE.toArray(new Span[0]));

      assertThat(store().getTrace(TestObjects.TRACE.get(0).traceId()).execute())
          .containsExactlyInAnyOrderElementsOf(TestObjects.TRACE);
    }

    // Below need some thinking as we need compound queries and it appears statements need to be
    // pre-defined
    @Override @Test @Ignore("TODO") public void getTraces_differentiateOnServiceName() {
    }

    @Override @Test @Ignore("TODO") public void getTraces_filteringMatchesMostRecentTraces() {
    }

    @Override @Test @Ignore("TODO") public void getTraces_duration() {
    }

    @Override @Test @Ignore("TODO") public void getTraces_multipleAnnotationsBecomeAndFilter() {
    }

    @Override @Test @Ignore("TODO") public void getTraces_considersBitsAbove64bit() {
    }

    @Override @Test @Ignore("TODO") public void getTraces_tags() {
    }

    @Override @Test @Ignore("TODO") public void getTraces_minDuration() {
    }

    @Override @Test @Ignore("TODO") public void getTraces_maxDuration() {
    }
  }

  public static class ITEnsureSchema extends zipkin2.storage.voltdb.ITEnsureSchema {
    @ClassRule public static VoltDBStorageRule voltdb = classRule();

    @Override Client client() {
      return voltdb.storage.client;
    }
  }

  public static class ITInstallJavaProcedure extends zipkin2.storage.voltdb.ITInstallJavaProcedure {
    @ClassRule public static VoltDBStorageRule voltdb = classRule();

    @Override VoltDBStorage storage() {
      return voltdb.storage;
    }

    @Before public void clear() throws Exception {
      voltdb.clear();
    }
  }

  public static class ITLinkTrace extends zipkin2.storage.voltdb.ITLinkTrace {
    @ClassRule public static VoltDBStorageRule voltdb = classRule();

    @Override VoltDBStorage storage() {
      return voltdb.storage;
    }

    @Before public void clear() throws Exception {
      voltdb.clear();
    }
  }

  public static class ITCompletePendingTraces
      extends zipkin2.storage.voltdb.ITCompletePendingTraces {
    @ClassRule public static VoltDBStorageRule voltdb = classRule();

    @Override VoltDBStorage storage() {
      return voltdb.storage;
    }

    @Before public void clear() throws Exception {
      voltdb.clear();
    }
  }

  public static class ITLinkCompleteTraces
      extends zipkin2.storage.voltdb.ITLinkCompleteTraces {
    @ClassRule public static VoltDBStorageRule voltdb = classRule();

    @Override VoltDBStorage storage() {
      return voltdb.storage;
    }

    @Before public void clear() throws Exception {
      voltdb.clear();
    }
  }
}
