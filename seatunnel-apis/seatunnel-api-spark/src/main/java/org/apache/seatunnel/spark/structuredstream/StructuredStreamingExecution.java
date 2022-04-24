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

package org.apache.seatunnel.spark.structuredstream;

import com.alibaba.fastjson.JSONObject;
import org.apache.seatunnel.env.Execution;
import org.apache.seatunnel.spark.BaseSparkTransform;
import org.apache.seatunnel.spark.SparkEnvironment;

import java.util.List;

public class StructuredStreamingExecution implements Execution<StructuredStreamingSource, BaseSparkTransform, StructuredStreamingSink, SparkEnvironment> {

    private final SparkEnvironment sparkEnvironment;

    private JSONObject config = new JSONObject();

    public StructuredStreamingExecution(SparkEnvironment sparkEnvironment) {
        this.sparkEnvironment = sparkEnvironment;
    }

    @Override
    public void start(List<StructuredStreamingSource> sources, List<BaseSparkTransform> transforms, List<StructuredStreamingSink> sinks) {

    }

    @Override
    public void setConfig(JSONObject config) {
        this.config = config;
    }

    @Override
    public JSONObject getConfig() {
        return this.config;
    }

}
