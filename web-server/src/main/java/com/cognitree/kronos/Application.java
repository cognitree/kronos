package com.cognitree.kronos;

import org.apache.commons.cli.*;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

import java.net.InetSocketAddress;

public class Application {

    public static void main(String[] args) throws Exception {
        Options options = new Options();
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
        final String host = cmd.getOptionValue("host", "localhost");
        final Integer port = Integer.parseInt(cmd.getOptionValue("port", "8080"));
        final String resourceBase = cmd.getOptionValue("resourceBase", "src/main/webapp");
        final String contextPath = cmd.getOptionValue("contextPath", "/");
        final String descriptorFile = cmd.getOptionValue("descriptor", "web-server/src/main/webapp/WEB-INF/web.xml");

        final InetSocketAddress socketAddress = new InetSocketAddress(host, port);
        Server server = new Server(socketAddress);
        WebAppContext webapp = new WebAppContext();
        webapp.setResourceBase(resourceBase);
        webapp.setContextPath(contextPath);
        webapp.setDefaultsDescriptor(descriptorFile);
        server.setHandler(webapp);
        server.start();
        server.join();
    }
}