/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.spark.hudi.sink

import com.alibaba.fastjson.JSONObject
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.common.config.TypesafeConfigUtils.mergeConfig
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.batch.SparkBatchSink
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._

class Hudi extends SparkBatchSink {

  val writePath = "hoodie.base.path"
  val tableName = "hoodie.table.name"
  val saveMode = "save.mode"
  val recordKeyField = "hoodie.datasource.write.recordkey.field"
  val preCombineField = "hoodie.datasource.write.precombine.field"

  override def checkConfig(): CheckResult = {
    checkAllExists(config, writePath, tableName)
  }

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = new JSONObject()
    defaultConfig.put(saveMode, "append")
    defaultConfig.put(recordKeyField, "uuid")
    defaultConfig.put(preCombineField, "ts")
    config = mergeConfig(config, defaultConfig)
  }

  override def output(df: Dataset[Row], env: SparkEnvironment): Unit = {
    val writer = df.write.format("hudi")
    for (e <- config.entrySet()) {
      writer.option(e.getKey, String.valueOf(e.getValue))
    }
    //    writer.option(DataSourceWriteOptions.PRECOMBINE_FIELD.key(), "id")
    //        writer.option(DataSourceWriteOptions.RECORDKEY_FIELD.key(), "id")
    //        writer.option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key(), "address")
    writer
      .mode(config.getString(saveMode))
      .save(config.getString(writePath))
  }
}
