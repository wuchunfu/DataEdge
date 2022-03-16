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
import org.apache.seatunnel.apis.BaseSink;
import org.apache.seatunnel.apis.BaseSource;
import org.apache.seatunnel.apis.BaseTransform;
import org.apache.seatunnel.common.config.ConfigRuntimeException;
import org.apache.seatunnel.env.Execution;
import org.apache.seatunnel.env.RuntimeEnv;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.batch.FlinkBatchExecution;
import org.apache.seatunnel.flink.stream.FlinkStreamExecution;
import org.apache.seatunnel.plugin.Plugin;
import org.apache.seatunnel.spark.SparkEnvironment;
import org.apache.seatunnel.spark.batch.SparkBatchExecution;
import org.apache.seatunnel.spark.stream.SparkStreamingExecution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

public class ConfigBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigBuilder.class);

    private static final String PLUGIN_NAME_KEY = "plugin_name";
    private final String configFile;
    private final EngineType engine;
    private ConfigPackage configPackage;
    private final JSONObject config;
    private boolean streaming;
    private JSONObject envConfig;
    private boolean enableHive;
    private final RuntimeEnv env;

    /**
     * 读取json文件，返回json串
     *
     * @param fileName
     * @return
     */
    public static String readJsonFile(String fileName) {
        String jsonStr = "";
        try {
            File jsonFile = new File(fileName);
            FileReader fileReader = new FileReader(jsonFile);

            Reader reader = new InputStreamReader(new FileInputStream(jsonFile), StandardCharsets.UTF_8);
            int ch = 0;
            StringBuilder sb = new StringBuilder();
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            fileReader.close();
            reader.close();
            jsonStr = sb.toString();
            return jsonStr;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public ConfigBuilder(String configFile, EngineType engine) {
        this.configFile = configFile;
        this.engine = engine;
        this.config = load();
        this.env = createEnv();
        this.configPackage = new ConfigPackage(engine.getEngine());
    }

    private JSONObject load() {
        if (configFile.isEmpty()) {
            throw new ConfigRuntimeException("Please specify config file");
        }

        LOGGER.info("Loading config file: {}", configFile);

        String content = readJsonFile(configFile);

        // variables substitution / variables resolution order:
        // config file --> system environment --> java properties
        String config = JSON.toJSONString(content);
        LOGGER.info("parsed config file: \n{}", JSON.toJSONString(content));
        return JSONObject.parseObject(content);
    }

    public JSONObject getEnvConfigs() {
        return envConfig;
    }

    public RuntimeEnv getEnv() {
        return env;
    }

    private boolean checkIsStreaming() {
        JSONArray sourceConfigList = config.getJSONArray(PluginType.SOURCE.getType());
        return sourceConfigList.getJSONObject(0).getString(PLUGIN_NAME_KEY).toLowerCase().endsWith("stream");
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

    /**
     * create plugin class instance, ignore case.
     **/
    private <T extends Plugin<?>> T createPluginInstanceIgnoreCase(String name, PluginType pluginType) throws Exception {
        if (name.split("\\.").length != 1) {
            // canonical class name
            return (T) Class.forName(name).newInstance();
        }
        String packageName;
        ServiceLoader<T> plugins;
        switch (pluginType) {
            case SOURCE:
                packageName = configPackage.getSourcePackage();
                Class<T> baseSource = (Class<T>) Class.forName(configPackage.getBaseSourceClass());
                plugins = ServiceLoader.load(baseSource);
                break;
            case TRANSFORM:
                packageName = configPackage.getTransformPackage();
                Class<T> baseTransform = (Class<T>) Class.forName(configPackage.getBaseTransformClass());
                plugins = ServiceLoader.load(baseTransform);
                break;
            case SINK:
                packageName = configPackage.getSinkPackage();
                Class<T> baseSink = (Class<T>) Class.forName(configPackage.getBaseSinkClass());
                plugins = ServiceLoader.load(baseSink);
                break;
            default:
                throw new IllegalArgumentException("PluginType not support : [" + pluginType + "]");
        }
        String canonicalName = packageName + "." + name;
        for (Iterator<T> it = plugins.iterator(); it.hasNext(); ) {
            try {
                T plugin = it.next();
                Class<?> serviceClass = plugin.getClass();
                String serviceClassName = serviceClass.getName();
                String clsNameToLower = serviceClassName.toLowerCase();
                if (clsNameToLower.equals(canonicalName.toLowerCase())) {
                    return plugin;
                }
            } catch (ServiceConfigurationError e) {
                // Iterator.next() may throw ServiceConfigurationError,
                // but maybe caused by a not used plugin in this job
                LOGGER.warn("Error when load plugin: [{}]", canonicalName, e);
            }
        }
        throw new ClassNotFoundException("Plugin class not found by name :[" + canonicalName + "]");
    }

    /**
     * check if config is valid.
     **/
    public void checkConfig() {
        this.createEnv();
        this.createPlugins(PluginType.SOURCE);
        this.createPlugins(PluginType.TRANSFORM);
        this.createPlugins(PluginType.SINK);
    }

    public <T extends Plugin<? extends RuntimeEnv>> List<T> createPlugins(PluginType type) {
        Objects.requireNonNull(type, "PluginType can not be null when create plugins!");
        List<T> basePluginList = new ArrayList<>();
        JSONArray configList = config.getJSONArray(type.getType());
        configList.forEach(plugin -> {
            try {
                JSONObject jsonObjectPlugin = JSONObject.parseObject(plugin.toString());
                T t = createPluginInstanceIgnoreCase(jsonObjectPlugin.getString(PLUGIN_NAME_KEY), type);
                t.setConfig(jsonObjectPlugin);
                basePluginList.add(t);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        return basePluginList;
    }

    private RuntimeEnv createEnv() {
        envConfig = config.getJSONObject("env");
        streaming = checkIsStreaming();
        enableHive = checkIsContainHive();
        RuntimeEnv env = null;
        switch (engine) {
            case SPARK:
                env = new SparkEnvironment().setEnableHive(enableHive);
                break;
            case FLINK:
                env = new FlinkEnvironment();
                break;
            default:
                break;
        }
        env.setConfig(envConfig);
        env.prepare(streaming);
        return env;
    }

    public Execution<
            ? extends BaseSource<? extends RuntimeEnv>,
            ? extends BaseTransform<? extends RuntimeEnv>,
            ? extends BaseSink<? extends RuntimeEnv>> createExecution() {
        Execution<
                ? extends BaseSource<? extends RuntimeEnv>,
                ? extends BaseTransform<? extends RuntimeEnv>,
                ? extends BaseSink<? extends RuntimeEnv>> execution = null;
        switch (engine) {
            case SPARK:
                SparkEnvironment sparkEnvironment = (SparkEnvironment) env;
                if (streaming) {
                    execution = new SparkStreamingExecution(sparkEnvironment);
                } else {
                    execution = new SparkBatchExecution(sparkEnvironment);
                }
                break;
            case FLINK:
                FlinkEnvironment flinkEnvironment = (FlinkEnvironment) env;
                if (streaming) {
                    execution = new FlinkStreamExecution(flinkEnvironment);
                } else {
                    execution = new FlinkBatchExecution(flinkEnvironment);
                }
                break;
            default:
                break;
        }
        return execution;
    }
}
