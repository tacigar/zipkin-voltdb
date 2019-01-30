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

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import zipkin2.autoconfigure.storage.voltdb.Access;

import static org.assertj.core.api.Assertions.assertThat;

public class ZipkinVoltDBStorageAutoConfigurationTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  AnnotationConfigApplicationContext context;

  @After public void close() {
    if (context != null) {
      context.close();
    }
  }

  @Test public void doesntProvidesStorageComponent_whenStorageTypeNotVoltDB() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of("zipkin.storage.type:elasticsearch").applyTo(context);
    Access.registerVoltDB(context);
    context.refresh();

    thrown.expect(NoSuchBeanDefinitionException.class);
    context.getBean(VoltDBStorage.class);
  }

  @Test public void providesStorageComponent_whenStorageTypeVoltDB() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of("zipkin.storage.type:voltdb").applyTo(context);
    Access.registerVoltDB(context);
    context.refresh();

    assertThat(context.getBean(VoltDBStorage.class)).isNotNull();
  }

  @Test public void canOverridesProperty_host() {
    context = new AnnotationConfigApplicationContext();
    TestPropertyValues.of(
      "zipkin.storage.type:voltdb",
      "zipkin.storage.voltdb.host:host1:21212"
    ).applyTo(context);
    Access.registerVoltDB(context);
    context.refresh();

    assertThat(context.getBean(VoltDBStorage.class).host).isEqualTo("host1:21212");
  }
}
