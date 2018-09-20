/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cognitree.kronos;

import com.cognitree.kronos.executor.ExecutorApp;
import com.cognitree.kronos.listeners.ExecutorContextListener;
import com.cognitree.kronos.listeners.SchedulerContextListener;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        Option modeOpt = new Option("mode", "mode", true,
                "Kronos deployment mode (scheduler/ executor/ all)");
        modeOpt.setRequired(false);
        options.addOption(modeOpt);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args, true);
            final String mode = cmd.getOptionValue("mode", "all");
            switch (mode) {
                case "executor":
                    ExecutorApp.main(new String[]{});
                    break;
                case "scheduler":
                default:
                    cmd = parseSchedulerOptions(args, options, parser);
                    new Application().start(mode, cmd);
            }
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("Kronos application", options);
            System.exit(1);
        }
    }

    private static CommandLine parseSchedulerOptions(String[] args, Options options, CommandLineParser parser)
            throws ParseException {
        Option hostOpt = new Option("host", "host", true, "Kronos server address");
        hostOpt.setRequired(false);
        options.addOption(hostOpt);
        Option portOpt = new Option("port", "port", true, "Kronos server port");
        portOpt.setRequired(false);
        options.addOption(portOpt);
        Option resourceBaseOpt = new Option("resourceBase", "resourceBase", true,
                "Kronos resource base");
        resourceBaseOpt.setRequired(true);
        options.addOption(resourceBaseOpt);
        Option contextPathOpt = new Option("contextPath", "contextPath", true,
                "Kronos context path");
        contextPathOpt.setRequired(false);
        options.addOption(contextPathOpt);
        Option descriptorPathOpt = new Option("descriptor", "descriptor", true,
                "Kronos descriptor path");
        descriptorPathOpt.setRequired(true);
        options.addOption(descriptorPathOpt);
        return parser.parse(options, args);
    }

    private void start(String mode, CommandLine schedulerOptions) throws Exception {
        final String host = schedulerOptions.getOptionValue("host", "localhost");
        final Integer port = Integer.parseInt(schedulerOptions.getOptionValue("port", "8080"));
        final String resourceBase = schedulerOptions.getOptionValue("resourceBase");
        final String contextPath = schedulerOptions.getOptionValue("contextPath", "/");
        final String descriptorFile = schedulerOptions.getOptionValue("descriptor");

        logger.info("Starting Kronos application with params mode {}, host {}, port {}, resource base {}, " +
                "context path {}, descriptor {}", mode, host, port, resourceBase, contextPath, descriptorFile);
        final InetSocketAddress socketAddress = new InetSocketAddress(host, port);
        Server server = new Server(socketAddress);
        WebAppContext webapp = new WebAppContext();
        webapp.setResourceBase(resourceBase);
        webapp.setContextPath(contextPath);
        webapp.setDescriptor(descriptorFile);
        switch (mode) {
            case "scheduler":
                webapp.addEventListener(new SchedulerContextListener());
                break;
            default:
                webapp.addEventListener(new SchedulerContextListener());
                webapp.addEventListener(new ExecutorContextListener());
                break;
        }
        server.setHandler(webapp);
        server.start();
        server.join();
    }
}