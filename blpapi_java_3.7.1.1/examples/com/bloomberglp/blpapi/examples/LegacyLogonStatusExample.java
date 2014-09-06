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

import com.bloomberglp.blpapi.CorrelationID;

import com.bloomberglp.blpapi.Event;

import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.EventQueue;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;

public class LegacyLogonStatusExample {
     private static final String API_AUTH_SVC_NAME = "//blp/apiauth";
     private static final Name LOGONSTATUS_RESPONSE = Name.getName(
        "LogonStatusResponse");
     private String d_serverHost;
     private int d_serverPort;
     private int d_uuid = 0;
     private String  d_loginAddress;

     public LegacyLogonStatusExample()
     {
        d_serverHost = "localhost";
        d_serverPort = 8194;
     }

    public static void main(String[] args) throws java.lang.Exception{
        LegacyLogonStatusExample example = new LegacyLogonStatusExample();
        example.run(args);
        System.out.println("Press ENTER to quit");
        System.in.read();
    }
    private void run(String[] args) throws Exception
    {
        if (!parseCommandLine(args)){
            printUsage();
            System.exit(-1);
        }
        SessionOptions sessionOptions = new SessionOptions();
        sessionOptions.setServerHost(d_serverHost);
        sessionOptions.setServerPort(d_serverPort);

        System.out.println("Connecting to " + d_serverHost + ":" + d_serverPort);
        Session session = new Session(sessionOptions);
        if (!session.start()) {
            System.err.println("Failed to start session.");
            return;
        }
        if (!session.openService(API_AUTH_SVC_NAME)) {
            System.out.println("Failed to open service: " + API_AUTH_SVC_NAME);
            System.exit(-1);
        }
        Service apiAuthSvc = session.getService(API_AUTH_SVC_NAME);
        EventQueue eventQueue = new EventQueue();
        CorrelationID correlator = new CorrelationID(10);
        Request logonStatusRequest = apiAuthSvc.createRequest("LogonStatusRequest");
        Element userInfo = logonStatusRequest.getElement("userInfo");
        userInfo.setElement("uuid", d_uuid);

        logonStatusRequest.set("ipAddress", d_loginAddress);
        session.sendRequest(logonStatusRequest, eventQueue, correlator);

        Event event = eventQueue.nextEvent(5000*60);
        if (event.eventType() == Event.EventType.TIMEOUT){
            printEvent(event);
            session.cancel(correlator);
        } else if (event.eventType() == Event.EventType.RESPONSE ||
                    event.eventType() == Event.EventType.REQUEST_STATUS) {
            MessageIterator msgIter = event.messageIterator();
            if (msgIter.hasNext()) {
                Message msg = msgIter.next();
                if (msg.messageType().equals(LOGONSTATUS_RESPONSE)) {
                    boolean status = msg.getElementAsBool("isLoggedOn");
                    if (status == true) {
                        System.out.println("User " + d_uuid +
                            " is logged on at " + d_loginAddress);
                    } else {
                        System.out.println("User " + d_uuid +
                                " is NOT logged on at " + d_loginAddress);
                    }
                    session.cancel(correlator);
                }
            }
        }
    }

    private boolean parseCommandLine(String[] args) throws Exception
    {
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equalsIgnoreCase("-u")) {
                d_uuid = Integer.parseInt(args[i+1]);
            }
            else if (args[i].equalsIgnoreCase("-ip")) {
                d_serverHost = args[i+1];
            }
            else if (args[i].equalsIgnoreCase("-p")) {
                d_serverPort = Integer.parseInt(args[i+1]);
            }
            else if (args[i].equalsIgnoreCase("-a")) {
                d_loginAddress = args[i+1];
            }
            else if (args[i].equalsIgnoreCase("-h")) {
                return false;
            }
        }

        if (d_loginAddress == null) {
            System.out.println("Must specify a login address");
            return false;
        }

        if (d_uuid == 0){
            System.out.println("Must specifiy a nonzero UUID");
            return false;
        }
        return true;
    }
    private void printEvent(Event event) throws Exception
    {
        MessageIterator msgIter = event.messageIterator();
        while (msgIter.hasNext()) {
            Message msg = msgIter.next();

            CorrelationID correlationId = msg.correlationID();
            if (correlationId != null) {
                System.out.println("Correlator: " + correlationId);
            }

            Service service = msg.service();
            if (service != null) {
                System.out.println("Service: " + service.name());
            }
            System.out.println(msg);
        }
    }
    private void printUsage()
    {
        System.out.println("Usage:");
        System.out.println("    Check user's logon status");
        System.out.println("        [-ip <ipAddress = localhost> ]");
        System.out.println("        [-p  <tcpPort   = 8194>      ]");
        System.out.println("        [-u  <uuid>                  ]");
        System.out.println("        [-a  <login address>         ]");

        System.out.println("Press ENTER to quit");
    }
}
