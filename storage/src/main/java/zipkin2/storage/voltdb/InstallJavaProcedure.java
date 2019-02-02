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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;

import static zipkin2.storage.voltdb.VoltDBStorage.executeAdHoc;

final class InstallJavaProcedure {

  /** This installs a procedure that has no dependencies apart from VoltDB */
  static void installProcedure(Client client, Class<?> type, String partition) throws Exception {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    JarOutputStream jarOut = new JarOutputStream(bout);

    String path = type.getName().replace('.', '/') + ".class";

    // Jar format requires that you have an entry for each part of a directory path.
    // For example, to put a class under zipkin2/storage/voltdb, you need the path entries:
    // zipkin2/, zipkin2/storage/ and zipkin2/storage/voltdb/
    String[] paths = path.substring(0, path.lastIndexOf('/')).split("/");
    StringBuilder currentDir = new StringBuilder();
    for (int i = 0; i < paths.length; i++) {
      currentDir.append(paths[i]).append('/');
      jarOut.putNextEntry(new ZipEntry(currentDir.toString()));
    }

    // now, define the path of the class itself, followed by the bytecode of the class
    jarOut.putNextEntry(new ZipEntry(path));
    ClassLoader classLoader = type.getClassLoader();
    if (classLoader == null) classLoader = ClassLoader.getSystemClassLoader();
    try (InputStream classBytes = classLoader.getResourceAsStream(path)) {
      copy(classBytes, jarOut); // this copies the bytecode of the type we want into the jar
    }
    jarOut.closeEntry();
    jarOut.close();

    ClientResponse response =
        client.callProcedure("@UpdateClasses", bout.toByteArray(), null);
    if (response.getStatus() != ClientResponse.SUCCESS) {
      throw new RuntimeException(
          "@UpdateClasses for " + type.getName() + " resulted in " + response.getStatus());
    }
    executeAdHoc(client, "CREATE PROCEDURE " + (partition != null ?
        (" PARTITION ON " + partition) : "") + " FROM CLASS " + type.getName() + ";");
  }

  private static final int BUF_SIZE = 0x800; // 2K chars (4K bytes)

  /**
   * Adapted from {@code com.google.common.io.ByteStreams.copy()}.
   */
  private static long copy(InputStream from, OutputStream to) throws IOException {
    byte[] buf = new byte[BUF_SIZE];
    long total = 0;
    while (true) {
      int r = from.read(buf);
      if (r == -1) {
        break;
      }
      to.write(buf, 0, r);
      total += r;
    }
    return total;
  }
}
