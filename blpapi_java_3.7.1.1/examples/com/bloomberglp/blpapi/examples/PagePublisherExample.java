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
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

import com.bloomberglp.blpapi.CorrelationID;
import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.EventFormatter;
import com.bloomberglp.blpapi.EventQueue;
import com.bloomberglp.blpapi.Identity;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.ProviderEventHandler;
import com.bloomberglp.blpapi.ProviderSession;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.Topic;
import com.bloomberglp.blpapi.TopicList;
import com.bloomberglp.blpapi.Event.EventType;
import com.bloomberglp.blpapi.SessionOptions.ServerAddress;

public class PagePublisherExample {
    private static final Name TOPIC_SUBSCRIBED      = Name.getName("TopicSubscribed");
    private static final Name TOPIC_UNSUBSCRIBED    = Name.getName("TopicUnsubscribed");
    private static final Name TOPIC_RECAP           = Name.getName("TopicRecap");
    private static final Name TOPIC_CREATED         = Name.getName("TopicCreated");
    private static final Name START_COL             = Name.getName("startCol");
    private static final Name TOKEN_SUCCESS         = Name.getName("TokenGenerationSuccess");
    private static final Name AUTHORIZATION_SUCCESS = Name.getName("AuthorizationSuccess");
    private static final Name SESSION_TERMINATED    = Name.getName("SessionTerminated");
    private static final Name PERMISSION_REQUEST    = Name.getName("PermissionRequest");

    private static final String AUTH_USER        = "AuthenticationType=OS_LOGON";
    private static final String AUTH_APP_PREFIX  = "AuthenticationMode=APPLICATION_ONLY;ApplicationAuthenticationType=APPNAME_AND_KEY;ApplicationName=";
    private static final String AUTH_DIR_PREFIX  = "AuthenticationType=DIRECTORY_SERVICE;DirSvcPropertyName=";
    private static final String AUTH_OPTION_NONE = "none";
    private static final String AUTH_OPTION_USER = "user";
    private static final String AUTH_OPTION_APP  = "app=";
    private static final String AUTH_OPTION_DIR  = "dir=";

    private static final Set<Topic>   d_topicSet = new HashSet<Topic>();

    private String              d_service = "//viper/page";
    private int                 d_verbose = 0;
    private ArrayList<String>   d_hosts = new ArrayList<String>();
    private int                 d_port = 8194;

    private String              d_authOptions = AUTH_USER;

    enum AuthorizationStatus {
        WAITING,
        AUTHORIZED,
        FAILED
    };
    private final Hashtable<CorrelationID, AuthorizationStatus> d_authorizationStatus =
                new Hashtable<CorrelationID, AuthorizationStatus> ();

    private volatile boolean d_running = true;

    class MyEventHandler implements ProviderEventHandler {

        private void processTopicStatus(Event event, ProviderSession session) {
            TopicList topicList = new TopicList();
            for (Message msg: event) {
                System.out.println(msg);
                if (msg.messageType() == TOPIC_SUBSCRIBED) {
                    Topic topic = session.getTopic(msg);
                    if (topic == null) {
                        CorrelationID cid = new CorrelationID(msg.getElementAsString("topic"));
                        topicList.add(msg, cid);
                    }
                    else {
                        synchronized (d_topicSet) {
                            if (d_topicSet.add(topic))
                                d_topicSet.notifyAll();
                        }
                    }
                }
                else if (msg.messageType() == TOPIC_UNSUBSCRIBED) {
                    Topic topic = session.getTopic(msg);
                    synchronized (d_topicSet) {
                        d_topicSet.remove(topic);
                    }
                }
                else if (msg.messageType() == TOPIC_CREATED) {
                    Topic topic = session.getTopic(msg);
                    synchronized (d_topicSet) {
                        if (d_topicSet.add(topic))
                            d_topicSet.notifyAll();
                    }
                }
                else if (msg.messageType() == TOPIC_RECAP) {
                    Topic topic = session.getTopic(msg);
                    synchronized (d_topicSet) {
                        if (!d_topicSet.contains(topic)) {
                            continue;
                        }
                    }
                    // send initial paint, this should come from my own cache
                    Service service = session.getService(d_service);
                    Event recapEvent = service.createPublishEvent();
                    EventFormatter eventFormatter = new EventFormatter(recapEvent);
                    eventFormatter.appendRecapMessage(topic, msg.correlationID());
                    eventFormatter.setElement("numRows", 25);
                    eventFormatter.setElement("numCols", 80);
                    eventFormatter.pushElement("rowUpdate");
                    for (int i = 1; i <= 5; ++i) {
                        eventFormatter.appendElement();
                        eventFormatter.setElement("rowNum", i);
                        eventFormatter.pushElement("spanUpdate");
                        eventFormatter.appendElement();
                        eventFormatter.setElement("startCol", 1);
                        eventFormatter.setElement("length", 5);
                        eventFormatter.setElement("text", "RECAP");
                        eventFormatter.popElement();
                        eventFormatter.popElement();
                        eventFormatter.popElement();
                    }
                    eventFormatter.popElement();
                    session.publish(recapEvent);
                }
            }

            // resolveAsync will result in RESOLUTION_STATUS events.
            if (topicList.size() > 0) {
                session.createTopicsAsync(topicList);
            }
        }

        public void processEvent(Event event, ProviderSession session) {
            if (d_verbose > 0) {
                printMessage(event);
            }
            if (event.eventType() == EventType.SESSION_STATUS) {
                for (Message msg: event) {
                    if (msg.messageType() == SESSION_TERMINATED) {
                        d_running = false;
                        break;
                    }
                }
            }
            else if (event.eventType() == EventType.TOPIC_STATUS) {
                processTopicStatus(event, session);
            }
            else if (event.eventType() == EventType.REQUEST) {
                Service service = session.getService(d_service);
                for (Message msg: event) {
                    if (d_verbose > 0)
                        System.out.println(msg);
                    if (msg.messageType() == PERMISSION_REQUEST) {
                        // This example always sends a 'PERMISSIONED' response.
                        // See 'MktdataPublisherExample' on how to parse a Permission
                        // request and send an appropriate 'PermissionResponse'.
                        Event response = service.createResponseEvent(msg.correlationID());
                        EventFormatter ef = new EventFormatter(response);
                        int permission = 0; // ALLOWED: 0, DENIED: 1
                        ef.appendResponse("PermissionResponse");
                        ef.pushElement("topicPermissions");
                        // For each of the topics in the request, add an entry
                        // to the response
                        Element topicsElement = msg.getElement(Name.getName("topics"));
                        for (int i = 0; i < topicsElement.numValues(); ++i) {
                            ef.appendElement();
                            ef.setElement("topic", topicsElement.getValueAsString(i));
                            ef.setElement("result", permission); // PERMISSIONED
                            ef.popElement();
                        }
                        ef.popElement();
                        session.sendResponse(response);
                    }
                }
            }
            else if (event.eventType() == EventType.RESPONSE
                    || event.eventType() == EventType.PARTIAL_RESPONSE
                    || event.eventType() == EventType.REQUEST_STATUS) {
                for (Message msg: event) {
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
            return;
        }
    }

    private void printUsage() {
        System.out.println("Usage:");
        System.out.println("  Publish on a topic ");
        System.out.println("    -v                verbose, use multiple times to increase verbosity");
        System.out.println("    -ip   <ipAddress> server name or IP (default = localhost)");
        System.out.println("    -p    <tcpPort>   server port (default = 8194)");
        System.out.println("    -s    <service>   service name (default = //viper/page>)");
        System.out.println("    -auth <option>    authentication option: user|none|app=<app>|dir=<property> (default = user)");
    }

    private boolean parseCommandLine(String[] args) {
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equalsIgnoreCase("-ip") && i + 1 < args.length) {
                d_hosts.add(args[++i]);
            }
            else if (args[i].equalsIgnoreCase("-p") && i + 1 < args.length) {
                d_port = Integer.parseInt(args[++i]);
            }
            else if (args[i].equalsIgnoreCase("-s") && i + 1 < args.length) {
                d_service = args[++i];
            }
            else if (args[i].equalsIgnoreCase("-v")) {
                ++d_verbose;
            }
            else if (args[i].equalsIgnoreCase("-auth") && i + 1 < args.length) {
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
            }
            else {
                printUsage();
                return false;
            }
        }

        if (d_hosts.isEmpty()) {
            d_hosts.add("localhost");
        }

        return true;
    }

    private void printMessage(Event event) {
        for (Message msg: event) {
            System.out.println(msg);
        }
    }

    private boolean authorize(
            Service authService,
            Identity identity,
            ProviderSession session,
            CorrelationID cid) throws IOException, InterruptedException {

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
        if (event.eventType() == EventType.TOKEN_STATUS
                || event.eventType() == EventType.REQUEST_STATUS) {
            MessageIterator iter = event.messageIterator();
            while (iter.hasNext()) {
                Message msg = iter.next();
                System.out.println(msg.toString());
                if (msg.messageType() == TOKEN_SUCCESS) {
                    token = msg.getElementAsString("token");
                }
            }
        }
        if (token == null) {
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
                d_authorizationStatus.wait(1000);
                if (d_authorizationStatus.get(cid) != AuthorizationStatus.WAITING) {
                    return d_authorizationStatus.get(cid) == AuthorizationStatus.AUTHORIZED;
                }
                if (System.currentTimeMillis() - startTime > WAIT_TIME) {
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
        sessionOptions.setNumStartAttempts(servers.length);

        System.out.print("Connecting to");
        for (ServerAddress server: sessionOptions.getServerAddresses()) {
            System.out.print(" " + server);
        }
        System.out.println();

        ProviderSession session = new ProviderSession(
                sessionOptions,
                new MyEventHandler());

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

        if (!session.registerService(d_service, identity)) {
            System.err.println("Failed to register " + d_service);
            return;
        }

        System.out.println("Service registered " + d_service);

        Service service = session.getService(d_service);
        while (d_running) {
            Event event;
            synchronized (d_topicSet) {
                try {
                    if (d_topicSet.isEmpty())
                        d_topicSet.wait(100);
                }
                catch (InterruptedException e) {
                }
                if (d_topicSet.isEmpty())
                    continue;

                System.out.println("Publishing");
                event = service.createPublishEvent();
                EventFormatter eventFormatter = new EventFormatter(event);

                for (Topic topic: d_topicSet) {
                    if (!topic.isActive()) {
                        System.out.println("[WARNING] Publishing on an inactive topic.");
                    }
                    String os = (new Date()).toString();

                    int numRows = 5;
                    for (int i = 1; i <= numRows; ++i) {
                        eventFormatter.appendMessage("RowUpdate", topic);
                        eventFormatter.setElement("rowNum", i);
                        eventFormatter.pushElement("spanUpdate");

                        eventFormatter.appendElement();
                        eventFormatter.setElement(START_COL, 1);
                        eventFormatter.setElement("length", os.length());
                        eventFormatter.setElement("text", os);
                        eventFormatter.popElement();

                        eventFormatter.popElement();
                    }
                }
            }

            if (d_verbose > 1) {
                printMessage(event);
            }

            session.publish(event);
            Thread.sleep(20 * 1000);
        }

        session.stop();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("PagePublisherExample");
        PagePublisherExample example = new PagePublisherExample();
        example.run(args);
    }
}
