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

package org.apache.seatunnel;

import org.apache.seatunnel.core.flink.FlinkStarter;
import org.junit.Assert;
import org.junit.Test;

public class FlinkStarterTest {

    @Test
    public void buildCommands() {
        String[] args = {"--config", "test.conf", "-m", "yarn-cluster", "-i", "key1=value1", "-i", "key2=value2"};
        FlinkStarter flinkStarter = new FlinkStarter(args);
        String flinkExecuteCommand = String.join(" ", flinkStarter.buildCommands());
        // since we cannot get the actual jar path, so we just check the command contains the command
        Assert.assertTrue(flinkExecuteCommand.contains("--config test.conf"));
        Assert.assertTrue(flinkExecuteCommand.contains("-m yarn-cluster"));
        Assert.assertTrue(flinkExecuteCommand.contains("-Dkey1=value1"));
        Assert.assertTrue(flinkExecuteCommand.contains("${FLINK_HOME}/bin/flink run"));

        String[] args1 = {"--config", "test.conf", "-m", "yarn-cluster", "-i", "key1=value1", "-i", "key2=value2", "--run-mode", "run-application"};
        flinkExecuteCommand = String.join(" ", new FlinkStarter(args1).buildCommands());
        Assert.assertTrue(flinkExecuteCommand.contains("${FLINK_HOME}/bin/flink run-application"));

        String[] args2 = {"--config", "test.conf", "-m", "yarn-cluster", "-i", "key1=value1", "-i", "key2=value2", "--run-mode", "run"};
        flinkExecuteCommand = String.join(" ", new FlinkStarter(args2).buildCommands());
        Assert.assertTrue(flinkExecuteCommand.contains("${FLINK_HOME}/bin/flink run"));

        try {
            String[] args3 = {"--config", "test.conf", "-m", "yarn-cluster", "-i", "key1=value1", "-i", "key2=value2", "--run-mode", "run123"};
            new FlinkStarter(args3);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
            Assert.assertEquals("Run mode run123 not supported", e.getMessage());
        }
    }

    @Test
    public void buildCommandsMissingConfig() {
        try {
            String[] args = {"-m", "yarn-cluster", "-i", "key1=value1", "-i", "key2=value2"};
            FlinkStarter flinkStarter = new FlinkStarter(args);
            String flinkExecuteCommand = String.join(" ", flinkStarter.buildCommands());
            // since we cannot get the actual jar path, so we just check the command contains the command
            Assert.assertTrue(flinkExecuteCommand.contains("--config flink.yarn.conf"));
        } catch (Exception e) {
            Assert.assertEquals("The following option is required: [-c | --config]", e.getMessage());
        }
    }
}
