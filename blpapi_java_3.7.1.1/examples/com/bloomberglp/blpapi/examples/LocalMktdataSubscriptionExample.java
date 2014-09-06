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
import com.bloomberglp.blpapi.EventQueue;
import com.bloomberglp.blpapi.Identity;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.Subscription;
import com.bloomberglp.blpapi.SubscriptionList;
import com.bloomberglp.blpapi.Event.EventType;
import com.bloomberglp.blpapi.SessionOptions.ServerAddress;

public class LocalMktdataSubscriptionExample {
    private static final Name AUTHORIZATION_SUCCESS = Name.getName("AuthorizationSuccess");
    private static final Name TOKEN_SUCCESS         = Name.getName("TokenGenerationSuccess");

    private static final String AUTH_USER        = "AuthenticationType=OS_LOGON";
    private static final String AUTH_APP_PREFIX  = "AuthenticationMode=APPLICATION_ONLY;ApplicationAuthenticationType=APPNAME_AND_KEY;ApplicationName=";
    private static final String AUTH_USER_APP_PREFIX = "AuthenticationMode=USER_AND_APPLICATION;AuthenticationType=OS_LOGON;ApplicationAuthenticationType=APPNAME_AND_KEY;ApplicationName=";
    private static final String AUTH_DIR_PREFIX  = "AuthenticationType=DIRECTORY_SERVICE;DirSvcPropertyName=";
    private static final String AUTH_OPTION_NONE = "none";
    private static final String AUTH_OPTION_USER = "user";
    private static final String AUTH_OPTION_APP  = "app=";
    private static final String AUTH_OPTION_USER_APP = "userapp=";
    private static final String AUTH_OPTION_DIR  = "dir=";

    private static final String d_defaultHost      = "localhost";
    private static final int    d_defaultPort      = 8194;
    private static final String d_defaultService   = "//viper/mktdata";
    private static final int    d_defaultMaxEvents = Integer.MAX_VALUE;

    private String              d_service = d_defaultService;
    private ArrayList<String>   d_hosts = new ArrayList<String>();
    private int                 d_port = d_defaultPort;
    private int                 d_maxEvents = d_defaultMaxEvents;

    private String              d_authOptions = AUTH_USER;
    private ArrayList<String>   d_topics = new ArrayList<String>();
    private ArrayList<String>   d_fields = new ArrayList<String>();
    private ArrayList<String>   d_options = new ArrayList<String>();

    private void printUsage() {
        System.out.println("Usage:");
        System.out.println("\t[-ip   <ipAddress>]  \tserver name or IP (default: localhost)");
        System.out.println("\t[-p    <tcpPort>]    \tserver port (default: 8194)");
        System.out.println("\t[-s    <service>]    \tservice name (default: //viper/mktdata)");
        System.out.println("\t[-t    <topic>]      \ttopic to subscribe to (default: \"/ticker/IBM Equity\")");
        System.out.println("\t[-f    <field>]      \tfield to subscribe to (default: empty)");
        System.out.println("\t[-o    <option>]     \tsubscription options (default: empty)");
        System.out.println("\t[-auth <option>]     \tauthentication option: user|none|app=<app>|userapp=<app>|dir=<property> (default: user)");
    }

    private boolean parseCommandLine(String[] args) {
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equalsIgnoreCase("-s") && i + 1 < args.length) {
                d_service = args[++i];
            } else if (args[i].equalsIgnoreCase("-t") && i + 1 < args.length) {
                d_topics.add(args[++i]);
            } else if (args[i].equalsIgnoreCase("-f") && i + 1 < args.length) {
                d_fields.add(args[++i]);
            } else if (args[i].equalsIgnoreCase("-o") && i + 1 < args.length) {
                d_options.add(args[++i]);
            } else if (args[i].equalsIgnoreCase("-ip") && i + 1 < args.length) {
                d_hosts.add(args[++i]);
            } else if (args[i].equalsIgnoreCase("-p") && i + 1 < args.length) {
                d_port = Integer.parseInt(args[++i]);
            } else if (args[i].equalsIgnoreCase("-me") && i + 1 < args.length) {
                d_maxEvents = Integer.parseInt(args[++i]);
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
                } else if (args[i].regionMatches(true, 0, AUTH_OPTION_USER_APP,
                                            0, AUTH_OPTION_USER_APP.length())) {
                    d_authOptions = AUTH_USER_APP_PREFIX
                            + args[i].substring(AUTH_OPTION_USER_APP.length());

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
            d_hosts.add(d_defaultHost);
        }
        if (d_topics.size() == 0) {
            d_topics.add("/ticker/IBM Equity");
        }

        return true;
    }

    void printMessage(Event event) {
        for (Message msg: event) {
            System.out.println(msg);
        }
    }

    private void run(String[] args) throws IOException, InterruptedException {
        if (!parseCommandLine(args))
            return;

        ServerAddress[] servers = new ServerAddress[d_hosts.size()];
        for (int i = 0; i < d_hosts.size(); ++i) {
            servers[i] = new ServerAddress(d_hosts.get(i), d_port);
        }

        SessionOptions sessionOptions = new SessionOptions();
        sessionOptions.setServerAddresses(servers);
        sessionOptions.setDefaultSubscriptionService(d_service);
        sessionOptions.setDefaultTopicPrefix("ticker"); // normally defaults to "ticker"
        sessionOptions.setAuthenticationOptions(d_authOptions);
        sessionOptions.setAutoRestartOnDisconnection(true);
        sessionOptions.setNumStartAttempts(servers.length);

        System.out.print("Connecting to");
        for (ServerAddress server: sessionOptions.getServerAddresses()) {
            System.out.print(" " + server);
        }
        System.out.println();

        Session session = new Session(sessionOptions);
        if (!session.start()) {
            for (;;) {
                Event e = session.tryNextEvent();
                if (e == null)
                    break;
                printMessage(e);
            }
            System.err.println("Failed to start session.");
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
                System.err.println("Failed to open //blp/apiauth.");
            }
            if (!isAuthorized) {
                System.err.println("No authorization");
                return;
            }
        }

        SubscriptionList subscriptions = new SubscriptionList();
        for (int i = 0; i < d_topics.size(); ++i) {
            String topic = new String(d_service + d_topics.get(i));
            subscriptions.add(
                    new Subscription(
                            topic,
                            d_fields,
                            d_options,
                            new CorrelationID(topic)));
        }
        session.subscribe(subscriptions, identity);

        int eventCount = 0;
        while (true) {
            Event event = session.nextEvent();
            for (Message msg: event) {
                if (event.eventType() == EventType.SUBSCRIPTION_STATUS ||
                    event.eventType() == EventType.SUBSCRIPTION_DATA) {
                    System.out.println(msg.correlationID() + " - ");
                }
                System.out.println(msg);
            }
            if (event.eventType() == EventType.SUBSCRIPTION_DATA) {
                if (++ eventCount >= d_maxEvents) {
                    break;
                }
            }
        }
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

    public static void main(String[] args) {
        System.out.println("LocalMktdataSubscriptionExample");
        LocalMktdataSubscriptionExample example = new LocalMktdataSubscriptionExample();
        try {
            example.run(args);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
