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

import org.junit.AssumptionViolatedException;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import zipkin2.CheckResult;

final class VoltDBStorageRule extends ExternalResource {
  static final Logger LOGGER = LoggerFactory.getLogger(VoltDBStorageRule.class);
  static final int VOLTDB_PORT = 21212;
  final String image;
  GenericContainer container;
  VoltDBStorage storage;

  VoltDBStorageRule(String image) {
    this.image = image;
  }

  @Override protected void before() {
    try {
      LOGGER.info("Starting docker image " + image);
      container = new GenericContainer(image)
        .withEnv("HOST_COUNT", "1")
        .withExposedPorts(VOLTDB_PORT)
        .waitingFor(new HostPortWaitStrategy());
      container.start();
      System.out.println("Starting docker image " + image);
    } catch (RuntimeException e) {
      LOGGER.warn("Couldn't start docker image " + image + ": " + e.getMessage(), e);
    }

    try {
      tryToInitializeSession();
    } catch (RuntimeException | Error e) {
      if (container == null) throw e;
      LOGGER.warn("Couldn't connect to docker image " + image + ": " + e.getMessage(), e);
      container.stop();
      container = null; // try with local connection instead
      tryToInitializeSession();
    }
  }

  void tryToInitializeSession() {
    VoltDBStorage result = computeStorageBuilder().build();
    CheckResult check = result.check();
    if (!check.ok()) {
      throw new AssumptionViolatedException(check.error().getMessage(), check.error());
    }
    this.storage = result;
  }

  VoltDBStorage.Builder computeStorageBuilder() {
    return VoltDBStorage.newBuilder().host(host());
  }

  String host() {
    if (container != null && container.isRunning()) {
      return container.getContainerIpAddress() + ":" + container.getMappedPort(VOLTDB_PORT);
    } else {
      // Use localhost if we failed to start a container (i.e. Docker is not available)
      return "localhost:" + VOLTDB_PORT;
    }
  }

  @Override protected void after() {
    if (storage != null) {
      storage.close();
    }
    if (container != null) {
      LOGGER.info("Stopping docker image " + image);
      container.stop();
    }
  }
}
