/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.system.p2p;

import com.google.common.primitives.Longs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;

public class Util {
  public static long readFile(Path filePath) {
    try {
      return Files.isReadable(filePath)
          ? Longs.fromByteArray(Files.readAllBytes(filePath))
          : 0L;
    } catch (IOException | IllegalArgumentException e) {
      return 0L;
    }
  }

  public static void writeFile(Path filePath, long content) throws Exception {
    Files.createDirectories(filePath.getParent());
    Files.write(
        filePath,
        Longs.toByteArray(content),
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING,
        StandardOpenOption.WRITE,
        StandardOpenOption.SYNC);
  }

  public static void rmrf(String path) throws IOException {
    Files.walk(Paths.get(path))
        .sorted(Comparator.reverseOrder())
        .map(Path::toFile)
        .forEach(File::delete);
  }
}
