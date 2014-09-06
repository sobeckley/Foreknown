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

import com.bloomberglp.blpapi.CorrelationID;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.Event.EventType;
import com.bloomberglp.blpapi.SessionOptions.ServerAddress;
import com.bloomberglp.blpapi.EventQueue;
import com.bloomberglp.blpapi.Identity;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.Subscription;
import com.bloomberglp.blpapi.SubscriptionList;

public class LocalPageSubscriptionExample {

    private static final Name AUTHORIZATION_SUCCESS = Name.getName("AuthorizationSuccess");
    private static final Name TOKEN_SUCCESS = Name.getName("TokenGenerationSuccess");

    private static final String AUTH_USER        = "AuthenticationType=OS_LOGON";
    private static final String AUTH_APP_PREFIX  = "AuthenticationMode=APPLICATION_ONLY;ApplicationAuthenticationType=APPNAME_AND_KEY;ApplicationName=";
    private static final String AUTH_DIR_PREFIX  = "AuthenticationType=DIRECTORY_SERVICE;DirSvcPropertyName=";
    private static final String AUTH_OPTION_NONE = "none";
    private static final String AUTH_OPTION_USER = "user";
    private static final String AUTH_OPTION_APP  = "app=";
    private static final String AUTH_OPTION_DIR  = "dir=";

    private String serviceName = "//viper/page";
    private ArrayList<String> serverHost = new ArrayList<String>();
    private int    serverPort  = 8194;
    private String pageName    = "330/1/1";
    private String authOptions = AUTH_USER;

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        System.out.println("LocalPageSubscriptionExample");
        LocalPageSubscriptionExample example = new LocalPageSubscriptionExample();
        example.run(args);
    }

    private boolean authorize(
            Service authService,
            Identity identity,
            Session session,
            CorrelationID cid)
    throws IOException, InterruptedException {

        EventQueue tokenEventQueue = new EventQueue();
        try {
            session.generateToken(new CorrelationID(), tokenEventQueue);
        } catch (Exception e) {
            System.out.println("Generate token failed with exception: \n"  + e);
            return false;
        }
        String token = null;
        int timeoutMilliSeconds = 10000;
        Event event = tokenEventQueue.nextEvent(timeoutMilliSeconds);
        if (event.eventType() == EventType.TOKEN_STATUS ||
                event.eventType() == EventType.REQUEST_STATUS) {
            for (Message msg: event) {
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

        session.sendAuthorizationRequest(authRequest, identity, cid);

        long startTime = System.currentTimeMillis();
        final int WAIT_TIME = 10000; // 10 seconds
        while (true) {
            event = session.nextEvent(WAIT_TIME);
            if (event.eventType() == EventType.RESPONSE
                    || event.eventType() == EventType.PARTIAL_RESPONSE
                    || event.eventType() == EventType.REQUEST_STATUS) {
                for (Message msg: event) {
                    System.out.println(msg.toString());
                    if (msg.messageType() == AUTHORIZATION_SUCCESS) {
                        return true;
                    } else {
                        return false;
                    }
                }
            }
            if (System.currentTimeMillis() - startTime > WAIT_TIME) {
                return false;
            }
        }
    }

    private void run(String[] args) throws Exception {
        if (!parseCommandLine(args))
            return;

        ServerAddress[] servers = new ServerAddress[serverHost.size()];
        for (int i = 0; i < serverHost.size(); ++i) {
            servers[i] = new ServerAddress(serverHost.get(i), serverPort);
        }

        SessionOptions sessionOptions = new SessionOptions();
        sessionOptions.setServerAddresses(servers);
        sessionOptions.setAuthenticationOptions(authOptions);
        sessionOptions.setAutoRestartOnDisconnection(true);
        sessionOptions.setNumStartAttempts(servers.length);

        System.out.print("Connecting to");
        for (ServerAddress server: sessionOptions.getServerAddresses()) {
            System.out.print(" " + server);
        }
        System.out.println();

        Session session = new Session(sessionOptions);
        if (!session.start()) {
            System.err.println("Failed to start session.");
            return;
        }

        System.out.println("Connected successfully.");

        Identity identity = null;
        if (authOptions != null) {
            boolean isAuthorized = false;
            identity = session.createIdentity();
            if (session.openService("//blp/apiauth")) {
                Service authService = session.getService("//blp/apiauth");
                if (authorize(authService, identity, session, new CorrelationID())) {
                    isAuthorized = true;
                }
            }
            else {
                System.err.println("Failed to open //blp/apiauth.");
            }
            if (!isAuthorized) {
                System.err.println("No authorization");
                return;
            }
        }

        if (!session.openService(serviceName)) {
            System.err.println("Failed to open service " + serviceName);
            session.stop();
            return;
        }

        String topicName = serviceName + "/" + pageName;

        SubscriptionList subscriptions = new SubscriptionList();

        subscriptions.add(new Subscription(topicName, new CorrelationID(
                topicName)));

        System.out.println("Subscribing...");
        session.subscribe(subscriptions, identity);

        while (true) {
            Event event = session.nextEvent();

            MessageIterator msgIter = event.messageIterator();
            while (msgIter.hasNext()) {
                Message msg = msgIter.next();
                if (event.eventType() == Event.EventType.SUBSCRIPTION_DATA
                        || event.eventType() == Event.EventType.SUBSCRIPTION_STATUS) {
                    String topic = (String) msg.correlationID().object();
                    System.out.print(topic + " - ");
                }
                msg.print(System.out);
            }
        }
    }

    private void printUsage() {
        System.out.println("Usage:");
        System.out.println("    Local Page Subscription ");
        System.out.println("        [-ip    <ipAddress  = " + serverHost + ">]");
        System.out.println("        [-p     <tcpPort    = " + Integer.toString(serverPort) + ">]");
        System.out.println("        [-s     <service    = " + serviceName + ">]");
        System.out.println("        [-P     <Page       = " + pageName + ">]");
        System.out.println("        [-auth  <user|none|app={app}|dir={property}> (default: user)]");
    }

    private boolean parseCommandLine(String[] args) {
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equalsIgnoreCase("-ip") && i + 1 < args.length) {
                serverHost.add(args[++i]);
            } else if (args[i].equals("-p") && i + 1 < args.length) {
                serverPort = Integer.parseInt(args[++i]);
            } else if (args[i].equalsIgnoreCase("-s") && i + 1 < args.length) {
                serviceName = args[++i];
            } else if (args[i].equals("-P") && i + 1 < args.length) {
                pageName = args[++i];
            } else if (args[i].equalsIgnoreCase("-auth") && i + 1 < args.length) {
                ++i;
                if (args[i].equalsIgnoreCase(AUTH_OPTION_NONE)) {
                    authOptions = null;
                } else if (args[i].equalsIgnoreCase(AUTH_OPTION_USER)) {
                    authOptions = AUTH_USER;
                } else if (args[i].regionMatches(true, 0, AUTH_OPTION_APP,
                                                0, AUTH_OPTION_APP.length())) {
                    authOptions = AUTH_APP_PREFIX
                            + args[i].substring(AUTH_OPTION_APP.length());
                } else if (args[i].regionMatches(true, 0, AUTH_OPTION_DIR,
                                                0, AUTH_OPTION_DIR.length())) {
                    authOptions = AUTH_DIR_PREFIX
                            + args[i].substring(AUTH_OPTION_DIR.length());
                } else {
                    printUsage();
                    return false;
                }
            } else {
                printUsage();
                return false;
            }
        }
        if (serverHost.isEmpty()) {
            serverHost.add("localhost");
        }
        return true;
    }
}
