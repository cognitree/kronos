package com.cognitree.kronos;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;

public class Application {
    public static void main(String[] args) throws Exception {
        Server server = new Server(8080);

        WebAppContext webapp = new WebAppContext();
        webapp.setResourceBase("src/main/webapp");
        webapp.setContextPath("/");
        webapp.setDefaultsDescriptor("web-server/src/main/webapp/WEB-INF/web.xml");

        server.setHandler(webapp);
        server.start();
        server.join();
    }
}