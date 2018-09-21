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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Application {
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
                    // executor app does not need a web server, it can be started as a standalone java process as long as
                    // the message queue is distributed and accessible to executor and scheduler
                    ExecutorApp.main(args);
                    break;
                case "scheduler":
                    // scheduler app exposes APIs and hence needs to be deployed inside a web server
                default:
                    WebServer.main(args);
            }
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("Kronos application", options);
            System.exit(1);
        }
    }
}