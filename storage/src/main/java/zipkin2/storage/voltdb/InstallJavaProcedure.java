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
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;

import static zipkin2.storage.voltdb.VoltDBStorage.executeAdHoc;

final class InstallJavaProcedure {
  final Client client;
  final String typeName;
  List<String> additionalClasses = new ArrayList();
  String partition;
  boolean addZipkin;

  InstallJavaProcedure(Client client, String simpleTypeName) {
    this.client = client;
    this.typeName = "zipkin2.storage.voltdb.procedure." + simpleTypeName;
  }

  InstallJavaProcedure withAdditionalClass(String simpleName) {
    additionalClasses.add("zipkin2.storage.voltdb.procedure." + simpleName);
    return this;
  }

  InstallJavaProcedure withPartition(String partition) {
    this.partition = partition;
    return this;
  }

  InstallJavaProcedure addZipkin() {
    this.addZipkin = true;
    return this;
  }

  /** This installs a procedure that has no dependencies apart from VoltDB */
  void install() throws Exception {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    JarOutputStream jarOut = new JarOutputStream(bout);

    if (addZipkin) { // add zipkin's core jar
      JarInputStream jarIn = new JarInputStream(
          zipkin2.Span.class.getProtectionDomain().getCodeSource().getLocation().openStream()
      );
      try {
        copy(jarIn, jarOut);
      } finally {
        jarIn.close();
      }
    }

    // Jar format requires that you have an entry for each part of a directory path.
    // For example, to put a class under zipkin2/storage/voltdb, you need the path entries:
    // zipkin2/, zipkin2/storage/ and zipkin2/storage/voltdb/
    String[] paths = typeName.substring(0, typeName.lastIndexOf('.')).split("\\.");
    StringBuilder currentDir = new StringBuilder();
    for (int i = addZipkin ? /* skip zipkin/storage/ */ 2 : 0; i < paths.length; i++) {
      currentDir.append(paths[i]).append('/');

      jarOut.putNextEntry(new ZipEntry(currentDir.toString()));
    }

    // Allow subclassing in the same package
    for (String additionalClass : additionalClasses) {
      addClass(additionalClass, jarOut);
    }
    addClass(typeName, jarOut);
    jarOut.close();

    ClientResponse response =
        client.callProcedure("@UpdateClasses", bout.toByteArray(), null);
    if (response.getStatus() != ClientResponse.SUCCESS) {
      throw new RuntimeException(
          "@UpdateClasses for " + typeName + " resulted in " + response.getStatus());
    }
    executeAdHoc(client, "CREATE PROCEDURE " + (partition != null ?
        (" PARTITION ON " + partition) : "") + " FROM CLASS " + typeName + ";");
  }

  /** Adds the path of the class itself, followed by the bytecode of the class */
  void addClass(String typeName, JarOutputStream out) throws IOException {
    String path = typeName.replace('.', '/') + ".class";
    out.putNextEntry(new ZipEntry(path));
    ClassLoader classLoader = getClass().getClassLoader();
    if (classLoader == null) classLoader = ClassLoader.getSystemClassLoader();
    try (InputStream classBytes = classLoader.getResourceAsStream(path)) {
      copy(classBytes, out); // this copies the bytecode of the type we want into the jar
    }
    out.closeEntry();
  }

  private static final int BUF_SIZE = 0x800; // 2K chars (4K bytes)

  private static void copy(JarInputStream jarIn, JarOutputStream jarOut) throws IOException {
    byte[] buf = new byte[BUF_SIZE];
    JarEntry nextEntry;
    while ((nextEntry = jarIn.getNextJarEntry()) != null) {
      jarOut.putNextEntry(nextEntry);

      while (jarIn.available() == 1) {
        int length = jarIn.read(buf, 0, buf.length);
        if (length > 0) jarOut.write(buf, 0, length);
      }
    }
  }

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
