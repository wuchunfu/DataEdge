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

package org.apache.seatunnel.core.flink;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.core.base.Starter;
import org.apache.seatunnel.core.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.flink.config.FlinkJobType;
import org.apache.seatunnel.core.flink.utils.CommandLineUtils;

import java.util.List;

/**
 * The SeaTunnel flink starter. This class is responsible for generate the final flink job execute command.
 */
public class FlinkStarter implements Starter {

    private static final String APP_NAME = SeatunnelFlink.class.getName();
    private static final String APP_JAR_NAME = "seatunnel-core-flink.jar";

    /**
     * SeaTunnel parameters, used by SeaTunnel application. e.g. `-c config.conf`
     */
    private final FlinkCommandArgs flinkCommandArgs;

    /**
     * SeaTunnel flink job jar.
     */
    private final String appJar;

    FlinkStarter(String[] args) {
        this.flinkCommandArgs = CommandLineUtils.parseCommandArgs(args, FlinkJobType.JAR);
        // set the deployment mode, used to get the job jar path.
        Common.setDeployMode(flinkCommandArgs.getDeployMode().getName());
        this.appJar = Common.appLibDir().resolve(APP_JAR_NAME).toString();
    }

    @SuppressWarnings("checkstyle:RegexpSingleline")
    public static void main(String[] args) throws Exception {
        FlinkStarter flinkStarter = new FlinkStarter(args);
        System.out.println(String.join(" ", flinkStarter.buildCommands()));
    }

    @Override
    public List<String> buildCommands() throws Exception {
        return CommandLineUtils.buildFlinkCommand(flinkCommandArgs, APP_NAME, appJar, FlinkJobType.JAR);
    }

}
