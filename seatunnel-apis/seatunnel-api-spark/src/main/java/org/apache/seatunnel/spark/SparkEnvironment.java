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

package org.apache.seatunnel.spark;

import static org.apache.seatunnel.apis.base.plugin.Plugin.RESULT_TABLE_NAME;
import static org.apache.seatunnel.apis.base.plugin.Plugin.SOURCE_TABLE_NAME;

import com.alibaba.fastjson.JSONObject;
import org.apache.seatunnel.apis.base.env.RuntimeEnv;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.config.ConfigRuntimeException;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.List;

public class SparkEnvironment implements RuntimeEnv {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkEnvironment.class);

    private static final long DEFAULT_SPARK_STREAMING_DURATION = 5;

    private SparkSession sparkSession;

    private StreamingContext streamingContext;

    private JSONObject config = new JSONObject();

    private boolean enableHive = false;

    private JobMode jobMode;

    @Override
    public RuntimeEnv setJobMode(JobMode mode) {
        this.jobMode = mode;
        return this;
    }

    @Override
    public JobMode getJobMode() {
        return jobMode;
    }

    public SparkEnvironment setEnableHive(boolean enableHive) {
        this.enableHive = enableHive;
        return this;
    }

    @Override
    public SparkEnvironment setConfig(JSONObject config) {
        this.config = config;
        return this;
    }

    @Override
    public JSONObject getConfig() {
        return this.config;
    }

    @Override
    public CheckResult checkConfig() {
        return CheckResult.success();
    }

    @Override
    public void registerPlugin(List<URL> pluginPaths) {
        LOGGER.info("register plugins :" + pluginPaths);
        // TODO we use --jar parameter to support submit multi-jar in spark cluster at now. Refactor it to
        //  support submit multi-jar in code or remove this logic.
        // this.sparkSession.conf().set("spark.jars",pluginPaths.stream().map(URL::getPath).collect(Collectors.joining(",")));
    }

    @Override
    public SparkEnvironment prepare() {
        SparkConf sparkConf = createSparkConf();
        SparkSession.Builder builder = SparkSession.builder().config(sparkConf);
        if (enableHive) {
            builder.enableHiveSupport();
        }
        this.sparkSession = builder.getOrCreate();
        createStreamingContext();
        return this;
    }

    public SparkSession getSparkSession() {
        return this.sparkSession;
    }

    public StreamingContext getStreamingContext() {
        return this.streamingContext;
    }

    private SparkConf createSparkConf() {
        SparkConf sparkConf = new SparkConf();
        this.config.entrySet().forEach(entry -> sparkConf.set(entry.getKey(), String.valueOf(entry.getValue())));
        return sparkConf;
    }

    private void createStreamingContext() {
        SparkConf conf = this.sparkSession.sparkContext().getConf();
        long duration = conf.getLong("spark.stream.batchDuration", DEFAULT_SPARK_STREAMING_DURATION);
        if (this.streamingContext == null) {
            this.streamingContext = new StreamingContext(sparkSession.sparkContext(), Seconds.apply(duration));
        }
    }

    public static void registerTempView(String tableName, Dataset<Row> ds) {
        ds.createOrReplaceTempView(tableName);
    }

    public static Dataset<Row> registerInputTempView(BaseSparkSource<Dataset<Row>> source, SparkEnvironment environment) {
        JSONObject config = source.getConfig();
        if (config.containsKey(RESULT_TABLE_NAME)) {
            String tableName = config.getString(RESULT_TABLE_NAME);
            Dataset<Row> data = source.getData(environment);
            registerTempView(tableName, data);
            return data;
        } else {
            throw new ConfigRuntimeException("Plugin[" + source.getClass().getName() + "] " + "must be registered as dataset/table, please set " + RESULT_TABLE_NAME + " config");
        }
    }

    public static Dataset<Row> transformProcess(SparkEnvironment environment, BaseSparkTransform transform, Dataset<Row> ds) {
        Dataset<Row> fromDs;
        JSONObject config = transform.getConfig();
        if (config.containsKey(SOURCE_TABLE_NAME)) {
            String sourceTableName = config.getString(SOURCE_TABLE_NAME);
            fromDs = environment.getSparkSession().read().table(sourceTableName);
        } else {
            fromDs = ds;
        }
        return transform.process(fromDs, environment);
    }

    public static void registerTransformTempView(BaseSparkTransform transform, Dataset<Row> ds) {
        JSONObject config = transform.getConfig();
        if (config.containsKey(RESULT_TABLE_NAME)) {
            String resultTableName = config.getString(RESULT_TABLE_NAME);
            registerTempView(resultTableName, ds);
        }
    }

    public static <T extends Object> T sinkProcess(SparkEnvironment environment, BaseSparkSink<T> sink, Dataset<Row> ds) {
        Dataset<Row> fromDs;
        JSONObject config = sink.getConfig();
        if (config.containsKey(SOURCE_TABLE_NAME)) {
            String sourceTableName = config.getString(SOURCE_TABLE_NAME);
            fromDs = environment.getSparkSession().read().table(sourceTableName);
        } else {
            fromDs = ds;
        }
        return sink.output(fromDs, environment);
    }
}
