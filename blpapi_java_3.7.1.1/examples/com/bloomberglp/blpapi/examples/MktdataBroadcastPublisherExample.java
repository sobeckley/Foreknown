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

import com.bloomberglp.blpapi.CorrelationID;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.EventFormatter;
import com.bloomberglp.blpapi.EventQueue;
import com.bloomberglp.blpapi.Identity;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.ProviderEventHandler;
import com.bloomberglp.blpapi.ProviderSession;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.ServiceRegistrationOptions;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.TopicList;
import com.bloomberglp.blpapi.SessionOptions.ServerAddress;
import com.bloomberglp.blpapi.Topic;
import com.bloomberglp.blpapi.Event.EventType;

public class MktdataBroadcastPublisherExample {
    private static final Name AUTHORIZATION_SUCCESS = Name.getName("AuthorizationSuccess");
    private static final Name TOKEN_SUCCESS = Name.getName("TokenGenerationSuccess");
    private static final Name SESSION_TERMINATED = Name.getName("SessionTerminated");

    private static final String AUTH_USER        = "AuthenticationType=OS_LOGON";
    private static final String AUTH_APP_PREFIX  = "AuthenticationMode=APPLICATION_ONLY;ApplicationAuthenticationType=APPNAME_AND_KEY;ApplicationName=";
    private static final String AUTH_DIR_PREFIX  = "AuthenticationType=DIRECTORY_SERVICE;DirSvcPropertyName=";
    private static final String AUTH_OPTION_NONE = "none";
    private static final String AUTH_OPTION_USER = "user";
    private static final String AUTH_OPTION_APP  = "app=";
    private static final String AUTH_OPTION_DIR  = "dir=";

    private String              d_service = "//viper/mktdata";
    private int                 d_verbose = 0;
    private ArrayList<String>   d_hosts = new ArrayList<String>();
    private int                 d_port = 8194;
    private int                 d_numRetry = 2;
    private int                 d_maxEvents = 100;

    private String              d_authOptions = AUTH_USER;
    private ArrayList<String>   d_topics = new ArrayList<String>();

    private String              d_groupId = null;
    private int                 d_priority = Integer.MAX_VALUE;

    enum AuthorizationStatus {
        WAITING,
        AUTHORIZED,
        FAILED
    };
    private final Hashtable<CorrelationID, AuthorizationStatus> d_authorizationStatus =
                new Hashtable<CorrelationID, AuthorizationStatus> ();
    private volatile boolean d_running = true;

    class MyStream {
        String d_id;
        Topic d_topic;

        public MyStream() {
            d_id = "";
        }

        public MyStream(String id) {
            d_id = id;
        }

        public void setTopic(Topic topic) {
            d_topic = topic;
        }

        public String getId() {
            return d_id;
        }

        public Topic getTopic() {
            return d_topic;
        }
    };

    class MyEventHandler implements ProviderEventHandler {
        public void processEvent(Event event, ProviderSession session) {
            if (d_verbose > 1) {
                System.out.println("Received event " + event.eventType());
            }
            for (Message msg: event) {
                if (msg.correlationID() != null && d_verbose > 1) {
                    System.out.println("cid = " + msg.correlationID());
                }
                System.out.println("Message = " + msg);
                if (event.eventType() == Event.EventType.SESSION_STATUS) {
                    if (msg.messageType() == SESSION_TERMINATED) {
                        d_running = false;
                    }
                    continue;
                }

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
        System.out.println("\t[-r    <number>]     \tnumber of retrying connection on disconnected (default: number of hosts)");
        System.out.println("\t[-s    <service>]    \tservice name (default: //viper/mktdata)");
        System.out.println("\t[-t    <topic>]      \ttopic to publish (default: \"IBM Equity\")");
        System.out.println("\t[-g    <groupId>]    \tpublisher groupId (defaults to a unique value)");
        System.out.println("\t[-pri  <piority>]    \tpublisher priority (default: Integer.MAX_VALUE)");
        System.out.println("\t[-me   <maxEvents>]  \tstop after publishing this many events (default: 100)");
        System.out.println("\t[-v]                 \tincrease verbosity (can be specified more than once)");
        System.out.println("\t[-auth <option>]     \tauthentication option: user|none|app=<app>|dir=<property> (default: user)");
    }

    private boolean parseCommandLine(String[] args) {
        boolean numRetryProvidedByUser = false;
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equalsIgnoreCase("-s") && i + 1 < args.length) {
                d_service = args[++i];
            } else if (args[i].equalsIgnoreCase("-t") && i + 1 < args.length) {
                d_topics.add(args[++i]);
            } else if (args[i].equalsIgnoreCase("-v")) {
                ++ d_verbose;
            } else if (args[i].equalsIgnoreCase("-ip") && i + 1 < args.length) {
                d_hosts.add(args[++i]);
            } else if (args[i].equalsIgnoreCase("-p") && i + 1 < args.length) {
                d_port = Integer.parseInt(args[++i]);
            } else if (args[i].equalsIgnoreCase("-r") && i + 1 < args.length) {
                d_numRetry = Integer.parseInt(args[++i]);
                numRetryProvidedByUser = true;
            } else if (args[i].equalsIgnoreCase("-g") && i + 1 < args.length) {
                d_groupId = args[++i];
            } else if (args[i].equalsIgnoreCase("-pri") && i + 1 < args.length) {
                d_priority = Integer.parseInt(args[++i]);
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
        if (d_topics.isEmpty()) {
            d_topics.add("IBM Equity");
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
                ProviderSession session,
                CorrelationID cid)
                throws IOException, InterruptedException {

        synchronized (d_authorizationStatus) {
            d_authorizationStatus.put(cid, AuthorizationStatus.WAITING);
        }
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

        synchronized (d_authorizationStatus) {
            session.sendAuthorizationRequest(authRequest, identity, cid);

            long startTime = System.currentTimeMillis();
            long waitTime = 10000; // 10 seconds
            while (true) {
                d_authorizationStatus.wait(waitTime);
                if (d_authorizationStatus.get(cid) != AuthorizationStatus.WAITING) {
                    return d_authorizationStatus.get(cid) == AuthorizationStatus.AUTHORIZED;
                }
                waitTime -= System.currentTimeMillis() - startTime;
                if (waitTime <= 0) {
                    return false;
                }
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

        ProviderSession session = new ProviderSession(sessionOptions, new MyEventHandler());

        if (!session.start()) {
            System.err.println("Failed to start session");
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
            if (!isAuthorized) {
                System.err.println("No authorization");
                return;
            }
        }

        if (d_groupId != null) {
            // NOTE: will perform explicit service registration here, instead of letting
            //       createTopics do it, as the latter approach doesn't allow for custom
            //       ServiceRegistrationOptions
            ServiceRegistrationOptions serviceRegistrationOptions = new ServiceRegistrationOptions();
            serviceRegistrationOptions.setGroupId(d_groupId);
            serviceRegistrationOptions.setServicePriority(d_priority);

            if (!session.registerService(d_service, identity, serviceRegistrationOptions)) {
                System.out.print("Failed to register " + d_service);
                return;
            }
        }

        TopicList topicList = new TopicList();
        for (int i = 0; i < d_topics.size(); i++) {
            topicList.add(
                    d_service + "/ticker/" + d_topics.get(i),
                    new CorrelationID(new MyStream(d_topics.get(i))));
        }

        session.createTopics(
                topicList,
                ProviderSession.ResolveMode.AUTO_REGISTER_SERVICES,
                identity);
        // createTopics() is synchronous, topicList will be updated
        // with the results of topic creation (resolution will happen
        // under the covers)

        ArrayList<MyStream> myStreams = new ArrayList<MyStream>();

        for (int i = 0; i < topicList.size(); ++i) {
            MyStream stream = (MyStream) topicList.correlationIdAt(i).object();
            if (topicList.statusAt(i) == TopicList.Status.CREATED) {
                Message msg = topicList.messageAt(i);
                stream.setTopic(session.getTopic(msg));
                myStreams.add(stream);
                System.out.println("Topic created: " + topicList.topicStringAt(i));
            }
            else {
                System.out.println("Stream '" + stream.getId()
                        + "': topic not resolved, status = " + topicList.statusAt(i));
            }
        }
        Service service = session.getService(d_service);
        if (service == null) {
            System.err.println("Service registration failed: " + d_service);
            System.exit(1);
        }
        if (myStreams.isEmpty()) {
            System.err.println("No topics created for publishing");
            System.exit(1);
        }

        // Now we will start publishing
        Name eventName = Name.getName("MarketDataEvents");
        Name high = Name.getName("HIGH");
        Name low = Name.getName("LOW");
        long tickCount = 1;
        for (int eventCount = 0; eventCount < d_maxEvents; ++eventCount) {
            if (!d_running) {
                break;
            }
            Event event = service.createPublishEvent();
            EventFormatter eventFormatter = new EventFormatter(event);

            for (int index = 0; index < myStreams.size(); index++) {
                Topic topic = myStreams.get(index).getTopic();
                if (!topic.isActive()) {
                    System.out.println("[WARNING] Publishing on an inactive topic.");
                }
                eventFormatter.appendMessage(eventName, topic);
                if (1 == tickCount) {
                    eventFormatter.setElement("OPEN", 1.0);
                } else if (2 == tickCount) {
                    eventFormatter.setElement("BEST_BID", 3.0);
                }
                eventFormatter.setElement(high, tickCount * 1.0);
                eventFormatter.setElement(low, tickCount * 0.5);
                ++ tickCount;
            }

            printMessage(event);

            session.publish(event);
            Thread.sleep(2 * 1000);
        }

        session.stop();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("MktdataBroadcastPublisherExample");
        MktdataBroadcastPublisherExample example = new MktdataBroadcastPublisherExample();
        example.run(args);
        System.out.println("Press ENTER to quit");
        System.in.read();
    }
}
