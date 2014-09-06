/*
 * Copyright 2012. Bloomberg Finance L.P.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:  The above
 * copyright notice and this permission notice shall be included in all copies
 * or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */
package com.bloomberglp.blpapi.examples;

import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.EventHandler;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.SchemaElementDefinition;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;

public class ServiceSchemaDump {

    private String d_service;

    private int d_port;
    private String d_host;

    class MyEventHandler implements EventHandler {
        public void processEvent(Event event, Session session) {
            System.out
                    .println("Received event " + event.eventType().toString());

            MessageIterator iter = event.messageIterator();
            while (iter.hasNext()) {
                Message msg = iter.next();
                System.out.println("Message = " + msg);
            }
        }

    }

    public ServiceSchemaDump() {
        d_service = "//blp/mktdata";
        d_port = 8194;
        d_host = "localhost";
    }

    private void printUsage() {
        System.out.println("Usage:");
        System.out.println("    Publish on a topic ");
        System.out.println("        [-ip        <ipAddress  = localhost>");
        System.out.println("        [-p         <tcpPort    = 8194>");
        System.out.println("        [-s         <service = //blp/mktdata>]");
    }

    private boolean parseCommandLine(String[] args) {
        for (int i = 0; i < args.length; ++i) {

            if (args[i].equalsIgnoreCase("-s")) {
                d_service = args[++i];
            } else if (args[i].equalsIgnoreCase("-ip")) {
                d_host = args[++i];
            } else if (args[i].equalsIgnoreCase("-p")) {
                d_port = Integer.parseInt(args[++i]);
            } else if (args[i].equalsIgnoreCase("-h")) {
                printUsage();
                return false;
            } else {
                printUsage();
                return false;
            }
        }

        return true;
    }

    public void run(String[] args) throws Exception {
        if (!parseCommandLine(args))
            return;

        SessionOptions sessionOptions = new SessionOptions();
        sessionOptions.setServerHost(d_host);
        sessionOptions.setServerPort(d_port);

        System.out.println("Connecting to " + d_host + ":" + d_port);
        Session session = new Session(sessionOptions, new MyEventHandler());

        if (!session.start()) {
            System.err.println("Failed to start session");
            return;
        }

        if (!session.openService(d_service)) {
            System.err.println("Failed to open service " + d_service);
            return;
        }

        Service service = session.getService(d_service);

        // Dump schema for the service
        System.out.println("Schema for service:" + d_service + "\n\n");
        for (int i = 0; i < service.numEventDefinitions(); ++i) {
            SchemaElementDefinition eventDefinition = service
                    .getEventDefinition(i);
            System.out.println(eventDefinition);
        }
        System.out.println("");
        session.stop();

    }

    public static void main(String[] args) throws Exception {
        ServiceSchemaDump example = new ServiceSchemaDump();
        example.run(args);
    }
}
