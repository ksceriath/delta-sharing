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

import io.delta.sharing.server.config.TableConfig

object TableConfigGenerator {

  private val (scheme, host) = {
    val conf = com.typesafe.config.ConfigFactory.load(
      new java.net.URLClassLoader(Array[java.net.URL](new java.io.File("/").toURI.toURL)),
      "delta.conf")
    (conf.getString("conf.delta_scheme"), conf.getString("conf.delta_host"))
  }

  private def location(share: String, schema: String, table: String) = {
    s"$scheme://$share@$host/$schema/$table"
  }

  def generateTableConfig(share: String, schema: String, table: String): TableConfig = {
    TableConfig(table, location(share, schema, table), cdfEnabled = true)
  }

}
