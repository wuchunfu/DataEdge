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

package org.apache.seatunnel.common.config;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.LinkedHashMap;
import java.util.Map;

public final class TypesafeConfigUtils {

    private TypesafeConfigUtils() {
    }

    /**
     * Extract sub config with fixed prefix
     *
     * @param source     config source
     * @param prefix     config prefix
     * @param keepPrefix true if keep prefix
     */
    public static JSONObject extractSubConfig(JSONObject source, String prefix, boolean keepPrefix) {
        // use LinkedHashMap to keep insertion order
        Map<String, String> values = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            final String key = entry.getKey();
            final String value = String.valueOf(entry.getValue());
            if (key.startsWith(prefix)) {
                if (keepPrefix) {
                    values.put(key, value);
                } else {
                    values.put(key.substring(prefix.length()), value);
                }
            }
        }
        return JSON.parseObject(JSON.toJSONString(values));
    }

    /**
     * Check if config with specific prefix exists
     *
     * @param source config source
     * @param prefix config prefix
     * @return true if it has sub config
     */
    public static boolean hasSubConfig(JSONObject source, String prefix) {
        boolean hasConfig = false;
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            final String key = entry.getKey();
            if (key.startsWith(prefix)) {
                hasConfig = true;
                break;
            }
        }
        return hasConfig;
    }

    public static JSONObject extractSubConfigThrowable(JSONObject source, String prefix, boolean keepPrefix) {
        JSONObject config = extractSubConfig(source, prefix, keepPrefix);
        if (config.isEmpty()) {
            throw new ConfigRuntimeException("config is empty");
        }
        return config;
    }

    public static JSONObject mergeConfig(JSONObject config, JSONObject defaultConfig) {
        defaultConfig.putAll(config);
        return defaultConfig;
    }

}
