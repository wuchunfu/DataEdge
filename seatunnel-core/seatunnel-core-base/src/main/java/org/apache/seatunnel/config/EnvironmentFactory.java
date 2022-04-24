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

package org.apache.seatunnel.config;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.env.RuntimeEnv;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.spark.SparkEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Used to create the {@link RuntimeEnv}.
 *
 * @param <ENVIRONMENT> environment type
 */
public class EnvironmentFactory<ENVIRONMENT extends RuntimeEnv> {

    private static final String PLUGIN_NAME_KEY = "plugin_name";

    private final JSONObject config;
    private final EngineType engine;

    public EnvironmentFactory(JSONObject config, EngineType engine) {
        this.config = config;
        this.engine = engine;
    }

    public synchronized ENVIRONMENT getEnvironment() {
        JSONObject envConfig = config.getJSONObject("env");
        boolean enableHive = checkIsContainHive();
        ENVIRONMENT env;
        switch (engine) {
            case SPARK:
                env = (ENVIRONMENT) new SparkEnvironment().setEnableHive(enableHive);
                break;
            case FLINK:
                env = (ENVIRONMENT) new FlinkEnvironment();
                break;
            default:
                throw new IllegalArgumentException("Engine: " + engine + " is not supported");
        }
        env.setConfig(envConfig)
                .setJobMode(getJobMode(envConfig)).prepare();
        return env;
    }

    private boolean checkIsContainHive() {
        JSONArray sourceConfigList = config.getJSONArray(PluginType.SOURCE.getType());
        for (Object config : sourceConfigList) {
            JSONObject jsonObject = JSON.parseObject(String.valueOf(config));
            if (jsonObject.getString(PLUGIN_NAME_KEY).toLowerCase().contains("hive")) {
                return true;
            }
        }
        JSONArray sinkConfigList = config.getJSONArray(PluginType.SINK.getType());
        for (Object config : sinkConfigList) {
            JSONObject jsonObject = JSON.parseObject(String.valueOf(config));
            if (jsonObject.getString(PLUGIN_NAME_KEY).toLowerCase().contains("hive")) {
                return true;
            }
        }
        return false;
    }

    private JobMode getJobMode(JSONObject envConfig) {
        JobMode jobMode;
        if (envConfig.containsKey("job.mode")) {
            String mode = envConfig.getString("job.mode");
            jobMode = JobMode.valueOf(mode.toUpperCase());
        } else {
            //Compatible with previous logic
            List<JSONObject> sourceConfigList = new ArrayList<>();
            JSONArray jsonArray = config.getJSONArray(PluginType.SOURCE.getType());
            jsonArray.forEach(str -> sourceConfigList.add(JSONObject.parseObject(str.toString())));
            jobMode = sourceConfigList.get(0).getString(PLUGIN_NAME_KEY).toLowerCase().endsWith("stream") ? JobMode.STREAMING : JobMode.BATCH;
        }
        return jobMode;
    }

}
