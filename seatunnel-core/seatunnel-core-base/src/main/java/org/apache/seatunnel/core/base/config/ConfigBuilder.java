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

package org.apache.seatunnel.core.base.config;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.seatunnel.common.config.ConfigRuntimeException;
import org.apache.seatunnel.apis.base.env.RuntimeEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Used to build the {@link JSONObject} from file.
 *
 * @param <ENVIRONMENT> environment type.
 */
public class ConfigBuilder<ENVIRONMENT extends RuntimeEnv> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigBuilder.class);

    private static final String PLUGIN_NAME_KEY = "plugin_name";
    private final Path configFile;
    private final EngineType engine;
    private final JSONObject config;

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

            Reader reader = new InputStreamReader(Files.newInputStream(jsonFile.toPath()), StandardCharsets.UTF_8);
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

    public ConfigBuilder(Path configFile, EngineType engine) {
        this.configFile = configFile;
        this.engine = engine;
        this.config = load();
    }

    private JSONObject load() {
        if (configFile == null) {
            throw new ConfigRuntimeException("Please specify config file");
        }

        LOGGER.info("Loading config file: {}", configFile);

        String content = readJsonFile(configFile.toFile().getPath());

        // variables substitution / variables resolution order:
        // config file --> system environment --> java properties
        String config = JSON.toJSONString(content);
        LOGGER.info("parsed config file: \n{}", JSON.toJSONString(content));
        return JSONObject.parseObject(content);
    }

    public JSONObject getConfig() {
        return config;
    }

    /**
     * check if config is valid.
     **/
    public void checkConfig() {
        // check environment
        ENVIRONMENT environment = new EnvironmentFactory<ENVIRONMENT>(config, engine).getEnvironment();
        // check plugins
        PluginFactory<ENVIRONMENT> pluginFactory = new PluginFactory<>(config, engine);
        pluginFactory.createPlugins(PluginType.SOURCE);
        pluginFactory.createPlugins(PluginType.TRANSFORM);
        pluginFactory.createPlugins(PluginType.SINK);
    }

}
