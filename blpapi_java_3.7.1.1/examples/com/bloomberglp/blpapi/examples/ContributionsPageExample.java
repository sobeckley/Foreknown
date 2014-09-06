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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import com.bloomberglp.blpapi.CorrelationID;
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
import com.bloomberglp.blpapi.ProviderSession.ResolveMode;
import com.bloomberglp.blpapi.SessionOptions.ServerAddress;

public class ContributionsPageExample {

    public static void main(String[] args) {
        System.out.println("ContributionsPageExample");
        ContributionsPageExample example = new ContributionsPageExample();
        try {
            example.run(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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

    private String d_service          = "//blp/mpfbapi";
    private ArrayList<String> d_hosts = new ArrayList<String>();
    private int d_port                = 8194;
    private int d_maxEvents           = 100;
    private String d_authOptions      = AUTH_USER;
    private String d_topic            = "220/660/1";
    private int d_contributionId      = 8563;

    enum AuthorizationStatus {
        WAITING,
        AUTHORIZED,
        FAILED
    };
    private final Hashtable<CorrelationID, AuthorizationStatus> d_authorizationStatus =
                new Hashtable<CorrelationID, AuthorizationStatus> ();
    private volatile boolean d_running = true;

    private boolean parseCommandLine(String[] args) {
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equalsIgnoreCase("-s") && i + 1 < args.length) {
                d_service = args[++i];
            } else if (args[i].equalsIgnoreCase("-ip") && i + 1 < args.length) {
                d_hosts.add(args[++i]);
            } else if (args[i].equalsIgnoreCase("-p") && i + 1 < args.length) {
                d_port = Integer.parseInt(args[++i]);
            } else if (args[i].equalsIgnoreCase("-t") && i + 1 < args.length) {
                d_topic = args[++i];
            } else if (args[i].equals("-me") && i + 1 < args.length) {
                d_maxEvents = Integer.parseInt(args[++i]);
            } else if (args[i].equals("-c") && i + 1 < args.length) {
                d_contributionId = Integer.parseInt(args[++i]);
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

        return true;
    }

    private void printUsage() {
        System.out.println("Usage:");
        System.out.println("  Contribute page data to a topic");
        System.out.println("    -ip   <ipAddress>     server name or IP (default = localhost)");
        System.out.println("    -p    <tcpPort>       server port (default = 8194)");
        System.out.println("    -s    <service>       service name (default = //blp/mpfbapi)");
        System.out.println("    -t    <topic>         topic (default = 220/660/1)");
        System.out.println("    -me   <maxEvents>     max number of events (default = " + d_maxEvents + ")");
        System.out.println("    -c    <contributorId> contributor id (default = 8563)");
        System.out.println("    -auth <option>        authentication: <user|none|app={app}|dir={property}> (default = " + AUTH_OPTION_USER + ")");
    }

    class MyEventHandler implements ProviderEventHandler {
        public void processEvent(Event event, ProviderSession session) {
            for (Message msg: event) {
                System.out.println("Message = " + msg);
                if (event.eventType() == Event.EventType.SESSION_STATUS) {
                    if (msg.messageType() == SESSION_TERMINATED ) {
                        d_running = false;
                    }
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

    static class MyStream {
        private String d_id;
        private Topic  d_topic;

        public Topic getTopic() {
            return d_topic;
        }

        public void setTopic(Topic topic) {
            d_topic = topic;
        }

        public String getId() {
            return d_id;
        }

        public MyStream(String id) {
            d_id    = id;
            d_topic = null;
        }
    }

    private boolean authorize(Service authService, Identity identity,
            ProviderSession session, CorrelationID cid) throws IOException,
            InterruptedException
    {
        synchronized (d_authorizationStatus) {
            d_authorizationStatus.put(cid, AuthorizationStatus.WAITING);
        }
        EventQueue tokenEventQueue = new EventQueue();
        try {
            session.generateToken(new CorrelationID(), tokenEventQueue);
        } catch (Exception e) {
            System.out.println("Generate token failed with exception: \n" + e);
            return false;
        }
        String token = null;
        int timeoutMilliSeconds = 10000;
        Event event = tokenEventQueue.nextEvent(timeoutMilliSeconds);
        if (event.eventType() == EventType.TOKEN_STATUS
                || event.eventType() == EventType.REQUEST_STATUS) {
            for (Message msg: event) {
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


    private void run(String[] args) throws Exception {
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
        ProviderSession session = new ProviderSession(sessionOptions,
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

        TopicList topicList = new TopicList();
        topicList.add(
                d_service + "/" + d_topic,
                new CorrelationID(new MyStream(d_topic)));

        session.createTopics(
                topicList,
                ResolveMode.AUTO_REGISTER_SERVICES,
                identity);

        List<MyStream> myStreams = new LinkedList<MyStream>();
        for (int i = 0; i < topicList.size(); ++ i) {
            if (topicList.statusAt(i) == TopicList.Status.CREATED) {
                Topic topic = session.getTopic(topicList.messageAt(i));
                MyStream stream = (MyStream) topicList.correlationIdAt(i).object();
                stream.setTopic(topic);
                myStreams.add(stream);
            }
        }

        Service service = session.getService(d_service);
        if (service == null) {
            System.err.println("Failed to get Service: " + d_service);
            return;
        }
        SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss.SSS");

        int iteration = 0;
        while (iteration ++ < d_maxEvents) {
            if (!d_running) {
                break;
            }
            Event event = service.createPublishEvent();
            EventFormatter eventFormatter = new EventFormatter(event);

            for (MyStream stream: myStreams) {
                eventFormatter.appendMessage("PageData", stream.getTopic());
                eventFormatter.pushElement("rowUpdate");

                eventFormatter.appendElement();
                eventFormatter.setElement("rowNum", 1);
                eventFormatter.pushElement("spanUpdate");

                eventFormatter.appendElement();
                eventFormatter.setElement("startCol", 20);
                eventFormatter.setElement("length", 4);
                eventFormatter.setElement("text", "TEST");
                eventFormatter.setElement("attr", "INTENSIFY");
                eventFormatter.popElement();

                eventFormatter.appendElement();
                eventFormatter.setElement("startCol", 25);
                eventFormatter.setElement("length", 4);
                eventFormatter.setElement("text", "PAGE");
                eventFormatter.setElement("attr", "BLINK");
                eventFormatter.popElement();

                eventFormatter.appendElement();
                eventFormatter.setElement("startCol", 30);
                String timestamp = timeFormat.format(new Date());
                eventFormatter.setElement("length", timestamp.length());
                eventFormatter.setElement("text", timestamp);
                eventFormatter.setElement("attr", "REVERSE");
                eventFormatter.popElement();

                eventFormatter.popElement();
                eventFormatter.popElement();

                eventFormatter.appendElement();
                eventFormatter.setElement("rowNum", 2);
                eventFormatter.pushElement("spanUpdate");
                eventFormatter.appendElement();
                eventFormatter.setElement("startCol", 20);
                eventFormatter.setElement("length", 9);
                eventFormatter.setElement("text", "---------");
                eventFormatter.setElement("attr", "UNDERLINE");
                eventFormatter.popElement();
                eventFormatter.popElement();
                eventFormatter.popElement();

                eventFormatter.appendElement();
                eventFormatter.setElement("rowNum", 3);
                eventFormatter.pushElement("spanUpdate");
                eventFormatter.appendElement();
                eventFormatter.setElement("startCol", 10);
                eventFormatter.setElement("length", 9);
                eventFormatter.setElement("text", "TEST LINE");
                eventFormatter.popElement();
                eventFormatter.appendElement();
                eventFormatter.setElement("startCol", 23);
                eventFormatter.setElement("length", 5);
                eventFormatter.setElement("text", "THREE");
                eventFormatter.popElement();
                eventFormatter.popElement();
                eventFormatter.popElement();
                eventFormatter.popElement();

                eventFormatter.setElement("contributorId", d_contributionId);
                eventFormatter.setElement("productCode", 1);
                eventFormatter.setElement("pageNumber", 1);
            }

            System.out.println(timeFormat.format(new Date()) + " -");

            MessageIterator msgIter = event.messageIterator();
            while (msgIter.hasNext()) {
                Message msg = msgIter.next();
                System.out.println(msg);
            }

            session.publish(event);
            Thread.sleep(10 * 1000);
        }

        session.stop();
    }
}
