/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.sharing.server

import java.io.File
import java.nio.file.Files

import io.delta.sharing.server.config._

object TestResource {
  def env(key: String): String = {
    sys.env.getOrElse(key, throw new IllegalArgumentException(s"Cannot find $key in sys env"))
  }

  object AWS {
    val bucket = "delta-exchange-test"
  }

  val TEST_PORT = 12345

  val testAuthorizationToken = "dapi5e3574ec767ca1548ae5bbed1a2dc04d"

  def setupTestTables(): File = {
    val testConfigFile = Files.createTempFile("delta-sharing", ".yaml").toFile
    testConfigFile.deleteOnExit()
    val shares = java.util.Arrays.asList(
      ShareConfig("share1",
        java.util.Arrays.asList(
          SchemaConfig(
            "default",
            java.util.Arrays.asList(
              TableConfig("table1", s"s3a://${TestResource.AWS.bucket}/delta-exchange-test/table1"),
              TableConfig("table3", s"s3a://${TestResource.AWS.bucket}/delta-exchange-test/table3")
            )
          )
        )
      ),
      ShareConfig("share2",
        java.util.Arrays.asList(
          SchemaConfig("default", java.util.Arrays.asList(
            TableConfig("table2", s"s3a://${TestResource.AWS.bucket}/delta-exchange-test/table2")
          )
          )
        ))
    )

    val serverConfig = new ServerConfig()
    serverConfig.setVersion(1)
    serverConfig.setShares(shares)
    serverConfig.setAuthorization(Authorization(testAuthorizationToken))
    serverConfig.setPort(TEST_PORT)
    serverConfig.setSsl(SSLConfig(selfSigned = true, null, null, null))

    serverConfig.save(testConfigFile.getCanonicalPath)
    testConfigFile
  }
}
