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
                "Kronos deployment mode (scheduler/ all)");
        modeOpt.setRequired(true);
        options.addOption(modeOpt);
        Option hostOpt = new Option("host", "host", true, "Kronos server address");
        hostOpt.setRequired(false);
        options.addOption(hostOpt);
        Option portOpt = new Option("port", "port", true, "Kronos server port");
        portOpt.setRequired(false);
        options.addOption(portOpt);
        Option resourceBaseOpt = new Option("resourceBase", "resourceBase", true,
                "Kronos resource base");
        resourceBaseOpt.setRequired(false);
        options.addOption(resourceBaseOpt);
        Option contextPathOpt = new Option("contextPath", "contextPath", true,
                "Kronos context path");
        contextPathOpt.setRequired(false);
        options.addOption(contextPathOpt);
        Option descriptorPathOpt = new Option("descriptor", "descriptor", true,
                "Kronos descriptor path");
        descriptorPathOpt.setRequired(true);
        options.addOption(descriptorPathOpt);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("Kronos web app", options);
            System.exit(1);
            return;
        }

        final String mode = cmd.getOptionValue("mode", "all");
        final String host = cmd.getOptionValue("host", "localhost");
        final Integer port = Integer.parseInt(cmd.getOptionValue("port", "8080"));
        final String resourceBase = cmd.getOptionValue("resourceBase", "src/main/webapp");
        final String contextPath = cmd.getOptionValue("contextPath", "/");
        final String descriptorFile = cmd.getOptionValue("descriptor", "web-server/src/main/webapp/WEB-INF/web.xml");

        logger.info("Starting Kronos application with params mode {}, host {}, port {}, resource base {}, " +
                "context path {}, descriptor {}", mode, host, port, resourceBase, contextPath, descriptorFile);

        final InetSocketAddress socketAddress = new InetSocketAddress(host, port);
        Server server = new Server(socketAddress);
        WebAppContext webapp = buildWebApp(resourceBase, contextPath, descriptorFile, mode);
        server.setHandler(webapp);
        server.start();
        server.join();
    }

    private static WebAppContext buildWebApp(String resourceBase, String contextPath, String descriptorFile, String mode) {
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
        return webapp;
    }
}