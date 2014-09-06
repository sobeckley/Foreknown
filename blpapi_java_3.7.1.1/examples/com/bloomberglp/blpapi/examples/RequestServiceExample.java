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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;

import com.bloomberglp.blpapi.AbstractSession;
import com.bloomberglp.blpapi.CorrelationID;
import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.EventFormatter;
import com.bloomberglp.blpapi.EventHandler;
import com.bloomberglp.blpapi.EventQueue;
import com.bloomberglp.blpapi.Identity;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.ProviderEventHandler;
import com.bloomberglp.blpapi.ProviderSession;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.Event.EventType;
import com.bloomberglp.blpapi.SessionOptions.ServerAddress;

public class RequestServiceExample {
    static private enum AuthorizationStatus {
        WAITING,
        AUTHORIZED,
        FAILED
    }

    static private enum Role {
        SERVER,
        CLIENT,
        BOTH
    }

    private static final Name AUTHORIZATION_SUCCESS = Name.getName("AuthorizationSuccess");
    private static final Name TOKEN_SUCCESS = Name.getName("TokenGenerationSuccess");
    private static final Name REFERENCE_DATA_REQUEST = Name.getName("ReferenceDataRequest");

    private static final String AUTH_USER        = "AuthenticationType=OS_LOGON";
    private static final String AUTH_APP_PREFIX  = "AuthenticationMode=APPLICATION_ONLY;ApplicationAuthenticationType=APPNAME_AND_KEY;ApplicationName=";
    private static final String AUTH_DIR_PREFIX  = "AuthenticationType=DIRECTORY_SERVICE;DirSvcPropertyName=";
    private static final String AUTH_OPTION_NONE = "none";
    private static final String AUTH_OPTION_USER = "user";
    private static final String AUTH_OPTION_APP  = "app=";
    private static final String AUTH_OPTION_DIR  = "dir=";

    private String              d_service = "//example/refdata";
    private int                 d_verbose = 0;
    private ArrayList<String>   d_hosts = new ArrayList<String>();
    private int                 d_port = 8194;
    private int                 d_numRetry = 2;

    private String              d_authOptions = AUTH_USER;
    private ArrayList<String>   d_securities = new ArrayList<String>();
    private ArrayList<String>   d_fields = new ArrayList<String>();
    private Role                d_role = Role.BOTH;
    private final Hashtable<CorrelationID, AuthorizationStatus> d_authorizationStatus =
        new Hashtable<CorrelationID, AuthorizationStatus> ();

    static double getTimestamp() {
        return ((double)System.nanoTime()) / 1000000000;
    }

    class MyProviderEventHandler implements ProviderEventHandler {
        public void processEvent(Event event, ProviderSession session) {
            System.out.println("Server received event " + event.eventType());
            if (event.eventType() == EventType.REQUEST) {
                MessageIterator iter = event.messageIterator();
                while (iter.hasNext()) {
                    Message msg = iter.next();
                    System.out.println("Message = " + msg);
                    if (msg.messageType() == REFERENCE_DATA_REQUEST) {
                        // Similar to createPublishEvent. We assume just one
                        // service - d_service. A responseEvent can only be
                        // for single request so we can specify the
                        // correlationId - which establishes context -
                        // when we create the Event.
                        Service service = session.getService(d_service);
                        if (msg.hasElement("timestamp")) {
                            double requestTime = msg.getElementAsFloat64("timestamp");
                            double latency = getTimestamp() - requestTime;
                            System.out.format("Request latency = %.4f\n", latency);
                        }
                        Event response = service.createResponseEvent(msg.correlationID());
                        EventFormatter ef = new EventFormatter(response);

                        // In appendResponse the string is the name of the
                        // operation, the correlationId indicates
                        // which request we are responding to.
                        ef.appendResponse("ReferenceDataRequest");
                        Element securities = msg.getElement("securities");
                        Element fields = msg.getElement("fields");
                        ef.setElement("timestamp", getTimestamp());
                        ef.pushElement("securityData");
                        for (int i = 0; i < securities.numValues(); ++i) {
                            ef.appendElement();
                            ef.setElement("security", securities.getValueAsString(i));
                            ef.pushElement("fieldData");
                            for (int j = 0; j < fields.numValues(); ++j) {
                                ef.appendElement();
                                ef.setElement("fieldId", fields.getValueAsString(j));
                                ef.pushElement("data");
                                ef.setElement("doubleValue", getTimestamp());
                                ef.popElement();
                                ef.popElement();
                            }
                            ef.popElement();
                            ef.popElement();
                        }
                        ef.popElement();

                        // Service is implicit in the Event. sendResponse has a
                        // second parameter - partialResponse -
                        // that defaults to false.
                        session.sendResponse(response);
                    }
                }
            }
            else {
                MessageIterator iter = event.messageIterator();
                while (iter.hasNext()) {
                    Message msg = iter.next();
                    if (msg.correlationID() != null && d_verbose > 0) {
                        System.out.println("cid = " + msg.correlationID());
                    }
                    System.out.println("Message = " + msg);
                    if (msg.correlationID() == null) {
                        continue;
                    }
                    synchronized (d_authorizationStatus) {
                        if (d_authorizationStatus.containsKey(msg.correlationID())) {
                            if (msg.messageType() == AUTHORIZATION_SUCCESS) {
                                d_authorizationStatus.put(
                                        msg.correlationID(),
                                        AuthorizationStatus.AUTHORIZED);
                            } else {
                                d_authorizationStatus.put(
                                        msg.correlationID(),
                                        AuthorizationStatus.FAILED);
                            }
                            d_authorizationStatus.notify();
                        }
                    }
                }
            }
        }
    }

    class MyRequesterEventHandler implements EventHandler {
        public void processEvent(Event event, Session session) {
            System.out.println("Client received event " + event.eventType());
            MessageIterator iter = event.messageIterator();
            while (iter.hasNext()) {
                Message msg = iter.next();
                if (msg.correlationID() != null && d_verbose > 1) {
                    System.out.println("cid = " + msg.correlationID());
                }
                System.out.println("Message = " + msg);

                if (msg.correlationID() == null) {
                    continue;
                }
                synchronized (d_authorizationStatus) {
                    if (d_authorizationStatus.containsKey(msg.correlationID())) {
                        if (msg.messageType() == AUTHORIZATION_SUCCESS) {
                            d_authorizationStatus.put(
                                    msg.correlationID(),
                                    AuthorizationStatus.AUTHORIZED);
                        } else {
                            d_authorizationStatus.put(
                                    msg.correlationID(),
                                    AuthorizationStatus.FAILED);
                        }
                        d_authorizationStatus.notify();
                    }
                }
            }
        }
    }


    private void printUsage() {
        System.out.println("Usage:");
        System.out.println("\t[-ip   <ipAddress>]  \tserver name or IP (default: localhost)");
        System.out.println("\t[-p    <tcpPort>]    \tserver port (default: 8194)");
        System.out.println("\t[-t    <number>]     \tnumber of retrying connection on disconnected (default: number of hosts)");
        System.out.println("\t[-v]                 \tincrease verbosity (can be specified more than once)");
        System.out.println("\t[-auth <option>]     \tauthentication option: user|none|app=<app>|dir=<property> (default: user)");
        System.out.println("\t[-s    <security>]   \trequest security for client (default: IBM US Equity)");
        System.out.println("\t[-f    <field>]      \trequest field for client (default: PX_LAST)");
        System.out.println("\t[-r    <option>]     \tservice role option: server|client|both (default: both)");
    }

    private boolean parseCommandLine(String[] args) {
        boolean numRetryProvidedByUser = false;
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equalsIgnoreCase("-v")) {
                ++ d_verbose;
            } else if (args[i].equalsIgnoreCase("-ip") && i + 1 < args.length) {
                d_hosts.add(args[++i]);
            } else if (args[i].equalsIgnoreCase("-p") && i + 1 < args.length) {
                d_port = Integer.parseInt(args[++i]);
            } else if (args[i].equalsIgnoreCase("-t") && i + 1 < args.length) {
                d_numRetry = Integer.parseInt(args[++i]);
                numRetryProvidedByUser = true;
            } else if (args[i].equalsIgnoreCase("-s") && i + 1 < args.length) {
                d_securities.add(args[++i]);
            } else if (args[i].equalsIgnoreCase("-f") && i + 1 < args.length) {
                d_fields.add(args[++i]);
            } else if (args[i].equalsIgnoreCase("-auth") && i + 1 < args.length) {
                ++i;
                if (args[i].equalsIgnoreCase(AUTH_OPTION_NONE)) {
                    d_authOptions = null;
                } else if (args[i].equalsIgnoreCase(AUTH_OPTION_USER)) {
                    d_authOptions = AUTH_USER;
                } else if (args[i].regionMatches(true, 0, AUTH_OPTION_APP,
                                                0, AUTH_OPTION_APP.length())) {
                    d_authOptions = AUTH_APP_PREFIX
                            + args[i].substring(AUTH_OPTION_APP.length());
                } else if (args[i].regionMatches(true, 0, AUTH_OPTION_DIR,
                                                0, AUTH_OPTION_DIR.length())) {
                    d_authOptions = AUTH_DIR_PREFIX
                            + args[i].substring(AUTH_OPTION_DIR.length());
                } else {
                    printUsage();
                    return false;
                }
            } else if (args[i].equalsIgnoreCase("-r") && i + 1 < args.length) {
                ++i;
                if (args[i].equalsIgnoreCase("server")) {
                    d_role = Role.SERVER;
                } else if (args[i].equalsIgnoreCase("client")) {
                    d_role = Role.CLIENT;
                } else if (args[i].equalsIgnoreCase("both")) {
                    d_role = Role.BOTH;
                } else {
                    printUsage();
                    return false;
                }
            } else {
                printUsage();
                return false;
            }
        }

        if (d_hosts.isEmpty()) {
            d_hosts.add("localhost");
        }
        if (d_securities.isEmpty()) {
            d_securities.add("IBM US Equity");
        }
        if (d_fields.isEmpty()) {
            d_fields.add("PX_LAST");
        }
        if (!numRetryProvidedByUser) {
            d_numRetry = d_hosts.size();
        }

        return true;
    }

    void printMessage(Event event) {
        for (Message msg: event) {
            System.out.println(msg);
        }
    }

    private boolean authorize(
                Service authService,
                Identity identity,
                AbstractSession session,
                CorrelationID cid)
                throws IOException, InterruptedException {

        synchronized (d_authorizationStatus) {
            d_authorizationStatus.put(cid, AuthorizationStatus.WAITING);
        }
        EventQueue tokenEventQueue = new EventQueue();
        try {
            session.generateToken(new CorrelationID(), tokenEventQueue);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        String token = null;
        int timeoutMilliSeconds = 10000;
        Event event = tokenEventQueue.nextEvent(timeoutMilliSeconds);
        if (event.eventType() == EventType.TOKEN_STATUS ||
                event.eventType() == EventType.REQUEST_STATUS) {
            MessageIterator iter = event.messageIterator();
            while (iter.hasNext()) {
                Message msg = iter.next();
                System.out.println(msg.toString());
                if (msg.messageType() == TOKEN_SUCCESS) {
                    token = msg.getElementAsString("token");
                }
            }
        }
        if (token == null){
            System.err.println("Failed to get token");
            return false;
        }

        Request authRequest = authService.createAuthorizationRequest();
        authRequest.set("token", token);

        synchronized (d_authorizationStatus) {
            session.sendAuthorizationRequest(authRequest, identity, cid);

            long startTime = System.currentTimeMillis();
            final int WAIT_TIME = 10000; // 10 seconds
            while (true) {
                d_authorizationStatus.wait(WAIT_TIME);
                if (d_authorizationStatus.get(cid) != AuthorizationStatus.WAITING) {
                    return d_authorizationStatus.get(cid) == AuthorizationStatus.AUTHORIZED;
                }
                if (System.currentTimeMillis() - startTime > WAIT_TIME) {
                    return false;
                }
            }
        }
    }

    private void serverRun(ProviderSession providerSession) throws Exception
    {
        System.out.println("Server is starting------");
        if (!providerSession.start()) {
            System.err.println("Failed to start server session");
            return;
        }

        Identity identity = null;
        if (d_authOptions != null) {
            boolean isAuthorized = false;
            identity = providerSession.createIdentity();
            if (providerSession.openService("//blp/apiauth")) {
                Service authService = providerSession.getService("//blp/apiauth");
                if (authorize(authService, identity, providerSession, new CorrelationID())) {
                    isAuthorized = true;
                }
            }
            else {
                if (!isAuthorized) {
                    System.err.println("No authorization");
                    return;
                }
            }
        }

        if (!providerSession.registerService(d_service, identity)) {
            System.err.println("Failed to register " + d_service);
            return;
        }
    }

    private void clientRun(Session session) throws Exception
    {
        System.out.println("Client is starting------");
        if (!session.start()) {
            System.err.println("Failed to start client session");
            return;
        }

        Identity identity = null;
        if (d_authOptions != null) {
            boolean isAuthorized = false;
            identity = session.createIdentity();
            if (session.openService("//blp/apiauth")) {
                Service authService = session.getService("//blp/apiauth");
                if (authorize(authService, identity, session, new CorrelationID())) {
                    isAuthorized = true;
                }
            }
            else {
                if (!isAuthorized) {
                    System.err.println("No authorization");
                    return;
                }
            }
        }

        if (!session.openService(d_service)) {
            System.err.println("Failed to open " + d_service);
            return;
        }

        Service service = session.getService(d_service);
        Request request = service.createRequest("ReferenceDataRequest");

        // Add securities to request
        Element securities = request.getElement("securities");
        for (int i = 0; i < d_securities.size(); ++i) {
            securities.appendValue(d_securities.get(i));
        }
        // Add fields to request
        Element fields = request.getElement("fields");
        for (int i = 0; i < d_fields.size(); ++i) {
            fields.appendValue(d_fields.get(i));
        }
        // Set time stamp
        request.set("timestamp", getTimestamp());

        System.out.println("Sending Request: " + request);
        EventQueue eventQueue = new EventQueue();
        session.sendRequest(request, identity, eventQueue, new CorrelationID());

        while (true) {
            Event event = eventQueue.nextEvent();
            System.out.println("Client received an event");
            MessageIterator msgIter = event.messageIterator();
            while (msgIter.hasNext()) {
                Message msg = msgIter.next();
                if (event.eventType() == EventType.RESPONSE) {
                    if (msg.hasElement("timestamp")) {
                        double responseTime = msg.getElementAsFloat64("timestamp");
                        double latency = getTimestamp() - responseTime;
                        System.out.format("Response latency = %.4f\n", latency);
                    }
                }
                System.out.println(msg);
            }
            if (event.eventType() == EventType.RESPONSE) {
                break;
            }
        }
    }

    public void run(String[] args) throws Exception {
        if (!parseCommandLine(args))
            return;

        ServerAddress[] servers = new ServerAddress[d_hosts.size()];
        for (int i = 0; i < d_hosts.size(); ++i) {
            servers[i] = new ServerAddress(d_hosts.get(i), d_port);
        }

        SessionOptions sessionOptions = new SessionOptions();
        sessionOptions.setServerAddresses(servers);
        sessionOptions.setAuthenticationOptions(d_authOptions);
        sessionOptions.setAutoRestartOnDisconnection(true);
        sessionOptions.setNumStartAttempts(d_numRetry);

        System.out.print("Connecting to");
        for (ServerAddress server: sessionOptions.getServerAddresses()) {
            System.out.print(" " + server);
        }
        System.out.println();

        if (d_role == Role.SERVER || d_role == Role.BOTH) {
            ProviderSession session = new ProviderSession(sessionOptions, new MyProviderEventHandler());
            serverRun(session);
        }

        if (d_role == Role.CLIENT || d_role == Role.BOTH) {
            Session session = new Session(sessionOptions, new MyRequesterEventHandler());
            clientRun(session);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("RequestServiceExample");
        RequestServiceExample example = new RequestServiceExample();
        example.run(args);
        System.out.println("Press ENTER to quit");
        System.in.read();
    }
}
