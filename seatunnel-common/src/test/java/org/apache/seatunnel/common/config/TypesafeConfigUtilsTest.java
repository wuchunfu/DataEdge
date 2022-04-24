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
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.common.config.TypesafeConfigUtils.extractSubConfig;
import static org.apache.seatunnel.common.config.TypesafeConfigUtils.extractSubConfigThrowable;
import static org.apache.seatunnel.common.config.TypesafeConfigUtils.hasSubConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class TypesafeConfigUtilsTest {
    @Test
    public void testExtractSubConfig() {
        JSONObject config = getConfig();
        JSONObject subConfig = extractSubConfig(config, "test.", true);
        Map<String, String> configMap = new HashMap<>();
        configMap.put("test.t0", "v0");
        configMap.put("test.t1", "v1");
        Assert.assertEquals(JSON.parseObject(JSON.toJSONString(configMap)), subConfig);

        subConfig = extractSubConfig(config, "test.", false);
        configMap = new HashMap<>();
        configMap.put("t0", "v0");
        configMap.put("t1", "v1");
        Assert.assertEquals(JSON.parseObject(JSON.toJSONString(configMap)), subConfig);
    }

    @Test
    public void testHasSubConfig() {
        JSONObject config = getConfig();
        boolean hasSubConfig = hasSubConfig(config, "test.");
        Assert.assertTrue(hasSubConfig);

        hasSubConfig = hasSubConfig(config, "test1.");
        Assert.assertFalse(hasSubConfig);
    }

    @Test
    public void testExtractSubConfigThrowable() {
        JSONObject config = getConfig();
        Throwable exception = assertThrows(ConfigRuntimeException.class, () -> {
            extractSubConfigThrowable(config, "test1.", false);
        });
        assertEquals("config is empty", exception.getMessage());

        JSONObject subConfig = extractSubConfigThrowable(config, "test.", false);
        Map<String, String> configMap = new HashMap<>();
        configMap.put("t0", "v0");
        configMap.put("t1", "v1");
        Assert.assertEquals(JSON.parseObject(JSON.toJSONString(configMap)), subConfig);
    }

    public JSONObject getConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("test.t0", "v0");
        configMap.put("test.t1", "v1");
        configMap.put("k0", "v2");
        configMap.put("k1", "v3");
        configMap.put("l1", Long.parseLong("100"));
        return JSON.parseObject(JSON.toJSONString(configMap));
    }

    @Test
    public void testGetConfig() {
        JSONObject config = getConfig();
        Assert.assertEquals(Long.parseLong("100"), (long) TypesafeConfigUtils.getConfig(config, "l1", Long.parseLong("101")));
        Assert.assertEquals(Long.parseLong("100"), (long) TypesafeConfigUtils.getConfig(config, "l2", Long.parseLong("100")));
    }
}
