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

package org.apache.seatunnel.spark.transform

import com.alibaba.fastjson.{JSONArray, JSONObject}
import org.apache.seatunnel.common.Constants
import org.apache.seatunnel.common.config.CheckConfigUtil.checkAllExists
import org.apache.seatunnel.common.config.CheckResult
import org.apache.seatunnel.spark.{BaseSparkTransform, SparkEnvironment}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._

class Split extends BaseSparkTransform {

  override def getPluginName: String = "Split"

  override def process(df: Dataset[Row], env: SparkEnvironment): Dataset[Row] = {
    val keys: JSONArray = config.getJSONArray("fields")
    val srcField: String = config.getString("source_field")
    val tarField: String = config.getString("target_field")
    val separator = config.getString("separator")

    // https://stackoverflow.com/a/33345698/1145750
    var func: UserDefinedFunction = null
    val ds = tarField match {
      case Constants.ROW_ROOT =>
        func = udf((s: String) => {
          split(s, separator, keys.size())
        })
        var filterDf = df.withColumn(Constants.ROW_TMP, func(col(srcField)))
        for (i <- 0 until keys.size()) {
          filterDf = filterDf.withColumn(keys.getString(i), col(Constants.ROW_TMP)(i))
        }
        filterDf.drop(Constants.ROW_TMP)
      case targetField: String =>
        func = udf((s: String) => {
          val values = split(s, separator, keys.size)
          val kvs = (keys zip values).toMap
          kvs
        })
        df.withColumn(targetField, func(col(srcField)))
    }
    if (func != null) {
      env.getSparkSession.udf.register("Split", func)
    }
    ds
  }

  override def checkConfig(): CheckResult = {
    checkAllExists(config, "fields")
  }

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = new JSONObject()
    defaultConfig.put("separator", " ")
    defaultConfig.put("source_field", "raw_message")
    defaultConfig.put("target_field", Constants.ROW_ROOT)
    config = mergeConfig(config, defaultConfig)
  }

  def mergeConfig(config: JSONObject, defaultConfig: JSONObject): JSONObject = {
    for (map <- config.entrySet) {
      defaultConfig.put(map.getKey, map.getValue)
    }
    defaultConfig
  }

  /**
   * Split string by separator, if size of splited parts is less than fillLength,
   * empty string is filled; if greater than fillLength, parts will be truncated.
   */
  private def split(str: String, separator: String, fillLength: Int): Seq[String] = {
    val parts = str.split(separator).map(_.trim)
    val filled = fillLength compare parts.length match {
      case 0 => parts
      case 1 => parts ++ Array.fill[String](fillLength - parts.length)("")
      case -1 => parts.slice(0, fillLength)
    }
    filled.toSeq
  }
}
