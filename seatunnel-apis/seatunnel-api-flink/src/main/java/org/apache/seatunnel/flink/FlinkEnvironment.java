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

package org.apache.seatunnel.flink;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.TernaryBoolean;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.env.RuntimeEnv;
import org.apache.seatunnel.flink.util.ConfigKeyName;
import org.apache.seatunnel.flink.util.EnvironmentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class FlinkEnvironment implements RuntimeEnv {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkEnvironment.class);

    private JSONObject config;

    private StreamExecutionEnvironment environment;

    private StreamTableEnvironment tableEnvironment;

    private ExecutionEnvironment batchEnvironment;

    private BatchTableEnvironment batchTableEnvironment;

    private JobMode jobMode;

    private String jobName = "seatunnel";

    @Override
    public FlinkEnvironment setConfig(JSONObject config) {
        this.config = config;
        return this;
    }

    @Override
    public JSONObject getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return EnvironmentUtil.checkRestartStrategy(config);
    }

    @Override
    public FlinkEnvironment prepare() {
        if (isStreaming()) {
            createStreamEnvironment();
            createStreamTableEnvironment();
        } else {
            createExecutionEnvironment();
            createBatchTableEnvironment();
        }
        if (config.containsKey("job.name")) {
            jobName = config.getString("job.name");
        }
        return this;
    }

    public String getJobName() {
        return jobName;
    }

    public boolean isStreaming() {
        return JobMode.STREAMING.equals(jobMode);
    }

    @Override
    public FlinkEnvironment setJobMode(JobMode jobMode) {
        this.jobMode = jobMode;
        return this;
    }

    @Override
    public JobMode getJobMode() {
        return jobMode;
    }

    @Override
    public void registerPlugin(List<URL> pluginPaths) {
        LOGGER.info("register plugins :" + pluginPaths);
        Configuration configuration;
        try {
            if (isStreaming()) {
                configuration =
                        (Configuration) Objects.requireNonNull(ReflectionUtils.getDeclaredMethod(StreamExecutionEnvironment.class,
                                "getConfiguration")).orElseThrow(() -> new RuntimeException("can't find " +
                                "method: getConfiguration")).invoke(this.environment);
            } else {
                configuration = batchEnvironment.getConfiguration();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        List<String> jars = configuration.get(PipelineOptions.JARS);
        if (jars == null) {
            jars = new ArrayList<>();
        }
        jars.addAll(pluginPaths.stream().map(URL::toString).collect(Collectors.toList()));
        configuration.set(PipelineOptions.JARS, jars);
        List<String> classpath = configuration.get(PipelineOptions.CLASSPATHS);
        if (classpath == null) {
            classpath = new ArrayList<>();
        }
        classpath.addAll(pluginPaths.stream().map(URL::toString).collect(Collectors.toList()));
        configuration.set(PipelineOptions.CLASSPATHS, classpath);
    }

    public StreamExecutionEnvironment getStreamExecutionEnvironment() {
        return environment;
    }

    public StreamTableEnvironment getStreamTableEnvironment() {
        return tableEnvironment;
    }

    private void createStreamTableEnvironment() {
        // use blink and streammode
        EnvironmentSettings.Builder envBuilder = EnvironmentSettings.newInstance().inStreamingMode();
        if (this.config.containsKey(ConfigKeyName.PLANNER) && "blink".equals(this.config.getString(ConfigKeyName.PLANNER))) {
            envBuilder.useBlinkPlanner();
        } else {
            envBuilder.useOldPlanner();
        }
        EnvironmentSettings environmentSettings = envBuilder.build();

        tableEnvironment = StreamTableEnvironment.create(getStreamExecutionEnvironment(), environmentSettings);
        TableConfig config = tableEnvironment.getConfig();
        if (this.config.containsKey(ConfigKeyName.MAX_STATE_RETENTION_TIME) && this.config.containsKey(ConfigKeyName.MIN_STATE_RETENTION_TIME)) {
            long max = this.config.getLong(ConfigKeyName.MAX_STATE_RETENTION_TIME);
            long min = this.config.getLong(ConfigKeyName.MIN_STATE_RETENTION_TIME);
            config.setIdleStateRetentionTime(Time.seconds(min), Time.seconds(max));
        }
    }

    private void createStreamEnvironment() {
        environment = StreamExecutionEnvironment.getExecutionEnvironment();
        setTimeCharacteristic();

        setCheckpoint();

        EnvironmentUtil.setRestartStrategy(config, environment.getConfig());

        if (config.containsKey(ConfigKeyName.BUFFER_TIMEOUT_MILLIS)) {
            long timeout = config.getLong(ConfigKeyName.BUFFER_TIMEOUT_MILLIS);
            environment.setBufferTimeout(timeout);
        }

        if (config.containsKey(ConfigKeyName.PARALLELISM)) {
            int parallelism = config.getIntValue(ConfigKeyName.PARALLELISM);
            environment.setParallelism(parallelism);
        }

        if (config.containsKey(ConfigKeyName.MAX_PARALLELISM)) {
            int max = config.getIntValue(ConfigKeyName.MAX_PARALLELISM);
            environment.setMaxParallelism(max);
        }
    }

    public ExecutionEnvironment getBatchEnvironment() {
        return batchEnvironment;
    }

    public BatchTableEnvironment getBatchTableEnvironment() {
        return batchTableEnvironment;
    }

    private void createExecutionEnvironment() {
        batchEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        if (config.containsKey(ConfigKeyName.PARALLELISM)) {
            int parallelism = config.getIntValue(ConfigKeyName.PARALLELISM);
            batchEnvironment.setParallelism(parallelism);
        }
        EnvironmentUtil.setRestartStrategy(config, batchEnvironment.getConfig());
    }

    private void createBatchTableEnvironment() {
        batchTableEnvironment = BatchTableEnvironment.create(batchEnvironment);
    }

    private void setTimeCharacteristic() {
        if (config.containsKey(ConfigKeyName.TIME_CHARACTERISTIC)) {
            String timeType = config.getString(ConfigKeyName.TIME_CHARACTERISTIC);
            switch (timeType.toLowerCase()) {
                case "event-time":
                    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
                    break;
                case "ingestion-time":
                    environment.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
                    break;
                case "processing-time":
                    environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
                    break;
                default:
                    LOGGER.warn("set time-characteristic failed, unknown time-characteristic [{}],only support event-time,ingestion-time,processing-time", timeType);
                    break;
            }
        }
    }

    private void setCheckpoint() {
        if (config.containsKey(ConfigKeyName.CHECKPOINT_INTERVAL)) {
            CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
            long interval = config.getLong(ConfigKeyName.CHECKPOINT_INTERVAL);
            environment.enableCheckpointing(interval);

            if (config.containsKey(ConfigKeyName.CHECKPOINT_MODE)) {
                String mode = config.getString(ConfigKeyName.CHECKPOINT_MODE);
                switch (mode.toLowerCase()) {
                    case "exactly-once":
                        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
                        break;
                    case "at-least-once":
                        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
                        break;
                    default:
                        LOGGER.warn("set checkpoint.mode failed, unknown checkpoint.mode [{}],only support exactly-once,at-least-once", mode);
                        break;
                }
            }

            if (config.containsKey(ConfigKeyName.CHECKPOINT_TIMEOUT)) {
                long timeout = config.getLong(ConfigKeyName.CHECKPOINT_TIMEOUT);
                checkpointConfig.setCheckpointTimeout(timeout);
            }

            if (config.containsKey(ConfigKeyName.CHECKPOINT_DATA_URI)) {
                String uri = config.getString(ConfigKeyName.CHECKPOINT_DATA_URI);
                StateBackend fsStateBackend = new FsStateBackend(uri);
                if (config.containsKey(ConfigKeyName.STATE_BACKEND)) {
                    String stateBackend = config.getString(ConfigKeyName.STATE_BACKEND);
                    if ("rocksdb".equalsIgnoreCase(stateBackend)) {
                        StateBackend rocksDBStateBackend = new RocksDBStateBackend(fsStateBackend, TernaryBoolean.TRUE);
                        environment.setStateBackend(rocksDBStateBackend);
                    }
                } else {
                    environment.setStateBackend(fsStateBackend);
                }
            }

            if (config.containsKey(ConfigKeyName.MAX_CONCURRENT_CHECKPOINTS)) {
                int max = config.getIntValue(ConfigKeyName.MAX_CONCURRENT_CHECKPOINTS);
                checkpointConfig.setMaxConcurrentCheckpoints(max);
            }

            if (config.containsKey(ConfigKeyName.CHECKPOINT_CLEANUP_MODE)) {
                boolean cleanup = config.getBoolean(ConfigKeyName.CHECKPOINT_CLEANUP_MODE);
                if (cleanup) {
                    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
                } else {
                    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
                }
            }

            if (config.containsKey(ConfigKeyName.MIN_PAUSE_BETWEEN_CHECKPOINTS)) {
                long minPause = config.getLong(ConfigKeyName.MIN_PAUSE_BETWEEN_CHECKPOINTS);
                checkpointConfig.setMinPauseBetweenCheckpoints(minPause);
            }

            if (config.containsKey(ConfigKeyName.FAIL_ON_CHECKPOINTING_ERRORS)) {
                int failNum = config.getIntValue(ConfigKeyName.FAIL_ON_CHECKPOINTING_ERRORS);
                checkpointConfig.setTolerableCheckpointFailureNumber(failNum);
            }
        }
    }

}
