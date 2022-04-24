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

package org.apache.seatunnel.spark.socket.source

import com.alibaba.fastjson.JSONObject
import org.apache.seatunnel.spark.SparkEnvironment
import org.apache.seatunnel.spark.socket.Config.{DEFAULT_HOST, DEFAULT_PORT, HOST, PORT}
import org.apache.seatunnel.spark.stream.SparkStreamingSource
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}
import org.apache.spark.streaming.dstream.DStream

import scala.collection.JavaConversions._

class SocketStream extends SparkStreamingSource[String] {

  override def getPluginName: String = "SocketStream"

  override def prepare(env: SparkEnvironment): Unit = {
    val defaultConfig = new JSONObject()
    defaultConfig.put(HOST, DEFAULT_HOST)
    defaultConfig.put(PORT, DEFAULT_PORT)
    config = mergeConfig(config, defaultConfig)
  }

  def mergeConfig(config: JSONObject, defaultConfig: JSONObject): JSONObject = {
    for (map <- config.entrySet) {
      defaultConfig.put(map.getKey, map.getValue)
    }
    defaultConfig
  }

  override def getData(env: SparkEnvironment): DStream[String] = {
    env.getStreamingContext.socketTextStream(config.getString(HOST), config.getIntValue("port"))
  }

  override def rdd2dataset(sparkSession: SparkSession, rdd: RDD[String]): Dataset[Row] = {
    val rowsRDD = rdd.map(element => {
      RowFactory.create(element)
    })

    val schema = StructType(Array(StructField("raw_message", DataTypes.StringType)))
    sparkSession.createDataFrame(rowsRDD, schema)
  }

}
