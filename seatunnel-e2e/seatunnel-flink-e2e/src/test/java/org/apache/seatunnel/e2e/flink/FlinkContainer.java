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

package org.apache.seatunnel.e2e.flink;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * This class is the base class of FlinkEnvironment test.
 * The before method will create a Flink cluster, and after method will close the Flink cluster.
 * You can use {@link FlinkContainer#executeSeaTunnelFLinkJob} to submit a seatunnel config and run a seatunnel job.
 */
public abstract class FlinkContainer {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkContainer.class);

    private static final String FLINK_DOCKER_IMAGE = "flink:1.13.6-scala_2.11";
    public static final Network NETWORK = Network.newNetwork();

    protected GenericContainer<?> jobManager;
    protected GenericContainer<?> taskManager;
    private static final String SEATUNNEL_FLINK_JAR = "seatunnel-core-flink.jar";
    private static final String FLINK_JAR_PATH = Paths.get("/tmp", SEATUNNEL_FLINK_JAR).toString();

    private static final int WAIT_FLINK_JOB_SUBMIT = 5000;

    private static final String FLINK_PROPERTIES = String.join(
            "\n",
            Arrays.asList(
                    "jobmanager.rpc.address: jobmanager",
                    "taskmanager.numberOfTaskSlots: 10",
                    "parallelism.default: 4",
                    "env.java.opts: -Doracle.jdbc.timezoneAsRegion=false"));

    @Before
    public void before() {
        jobManager = new GenericContainer<>(FLINK_DOCKER_IMAGE)
                .withCommand("jobmanager")
                .withNetwork(NETWORK)
                .withNetworkAliases("jobmanager")
                .withExposedPorts()
                .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
                .withLogConsumer(new Slf4jLogConsumer(LOG));

        taskManager =
                new GenericContainer<>(FLINK_DOCKER_IMAGE)
                        .withCommand("taskmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases("taskmanager")
                        .withEnv("FLINK_PROPERTIES", FLINK_PROPERTIES)
                        .dependsOn(jobManager)
                        .withLogConsumer(new Slf4jLogConsumer(LOG));

        Startables.deepStart(Stream.of(jobManager)).join();
        Startables.deepStart(Stream.of(taskManager)).join();
        copySeatunnelFlinkCoreJar();
        LOG.info("Containers are started.");
    }

    @After
    public void close() {
        if (taskManager != null) {
            taskManager.stop();
        }
        if (jobManager != null) {
            jobManager.stop();
        }
    }

    public Container.ExecResult executeSeaTunnelFLinkJob(String confFile) throws IOException, InterruptedException {
        final String confPath = getResource(confFile);
        if (!new File(confPath).exists()) {
            throw new IllegalArgumentException(confFile + " doesn't exist");
        }
        final String targetConfInContainer = Paths.get("/tmp", confFile).toString();
        jobManager.copyFileToContainer(MountableFile.forHostPath(confPath), targetConfInContainer);

        final List<String> command = new ArrayList<>();
        command.add("flink");
        command.add("run");
        command.add("-c org.apache.seatunnel.SeatunnelFlink " + FLINK_JAR_PATH);
        command.add("--config " + targetConfInContainer);

        Container.ExecResult execResult = jobManager.execInContainer("bash", "-c", String.join(" ", command));
        LOG.info(execResult.getStdout());
        LOG.error(execResult.getStderr());
        // wait job start
        Thread.sleep(WAIT_FLINK_JOB_SUBMIT);
        return execResult;
    }

    protected void copySeatunnelFlinkCoreJar() {
        String currentModuleHome = System.getProperty("user.dir");
        String seatunnelCoreFlinkJarPath = currentModuleHome.replace("/seatunnel-e2e/seatunnel-flink-e2e", "")
                + "/seatunnel-core/seatunnel-core-flink/target/seatunnel-core-flink.jar";
        jobManager.copyFileToContainer(MountableFile.forHostPath(seatunnelCoreFlinkJarPath), FLINK_JAR_PATH);
    }

    private String getResource(String confFile) {
        return System.getProperty("user.dir") + "/src/test/resources" + confFile;
    }

}
