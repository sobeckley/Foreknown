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
import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.Event.EventType;
import com.bloomberglp.blpapi.EventFormatter;
import com.bloomberglp.blpapi.EventQueue;
import com.bloomberglp.blpapi.Identity;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.ProviderEventHandler;
import com.bloomberglp.blpapi.ProviderSession;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.SchemaElementDefinition;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.ServiceRegistrationOptions;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.SessionOptions.ServerAddress;
import com.bloomberglp.blpapi.Topic;
import com.bloomberglp.blpapi.TopicList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class MktdataPublisherExample {
    private static final Name SERVICE_REGISTERED
        = Name.getName("ServiceRegistered");
    private static final Name SERVICE_REGISTER_FAILURE
        = Name.getName("ServiceRegisterFailure");
    private static final Name TOPIC_SUBSCRIBED
        = Name.getName("TopicSubscribed");
    private static final Name TOPIC_UNSUBSCRIBED
        = Name.getName("TopicUnsubscribed");
    private static final Name TOPIC_CREATED
        = Name.getName("TopicCreated");
    private static final Name TOPIC_RECAP
        = Name.getName("TopicRecap");
    private static final Name TOPIC
        = Name.getName("topic");
    private static final Name RESOLUTION_SUCCESS
        = Name.getName("ResolutionSuccess");
    private static final Name RESOLUTION_FAILURE
        = Name.getName("ResolutionFailure");
    private static final Name PERMISSION_REQUEST
        = Name.getName("PermissionRequest");
    private static final Name TOKEN_SUCCESS
        = Name.getName("TokenGenerationSuccess");
    private static final Name AUTHORIZATION_SUCCESS
        = Name.getName("AuthorizationSuccess");
    private static final Name AUTHORIZATION_FAILURE
        = Name.getName("AuthorizationFailure");
    private static final Name SESSION_TERMINATED
        = Name.getName("SessionTerminated");

    private String              d_service = "//viper/mktdata";
    private int                 d_verbose = 0;
    private ArrayList<String>   d_hosts = new ArrayList<String>();
    private int                 d_port = 8194;
    private ArrayList<Integer>  d_eids = new ArrayList<Integer>();

    private final Set<Topic>    d_topicSet = new HashSet<Topic>();
    private final Set<String>   d_subscribedTopics = new HashSet<String>();
    private final Object        d_authLock = new Object();
    private boolean             d_authSuccess = false;

    private Boolean             d_registerServiceResponse = null;
    private String              d_groupId = null;
    private int                 d_priority = Integer.MAX_VALUE;

    private static final String AUTH_USER = "AuthenticationType=OS_LOGON";
    private String              d_authOptions = AUTH_USER;
    private int                 d_clearInterval = 0;

    private boolean             d_useSsc = false;
    private int                 d_sscBegin;
    private int                 d_sscEnd;
    private int                 d_sscPriority;

    private Integer             d_resolveSubServiceCode = null;
    private volatile boolean    d_running = true;

    class MyEventHandler implements ProviderEventHandler {
        public void processEvent(Event event, ProviderSession session) {
            try {
                doProcessEvent(event, session);
            }
            catch (Exception e) {
                // don't let exceptions thrown by the library go back
                // into the library unnoticed
                e.printStackTrace();
            }
        }

        private void doProcessEvent(Event event, ProviderSession session) {
            if (d_verbose > 0) {
                System.out.println("Received event " + event.eventType());
                for (Message msg: event) {
                    System.out.println("cid = " + msg.correlationID());
                    System.out.println("Message = " + msg);
                }
            }

            if (event.eventType() == EventType.SESSION_STATUS) {
                for (Message msg: event) {
                    if (msg.messageType() == SESSION_TERMINATED) {
                        d_running = false;
                        break;
                    }
                }
            } else if (event.eventType() == EventType.TOPIC_STATUS) {
                TopicList topicList = new TopicList();
                for (Message msg: event) {
                    if (msg.messageType() == TOPIC_SUBSCRIBED) {
                        Topic topic = session.getTopic(msg);
                        synchronized (d_topicSet) {
                            d_subscribedTopics.add(
                                msg.getElementAsString(TOPIC));
                            if (topic == null) {
                                CorrelationID cid
                                    = new CorrelationID(
                                          msg.getElementAsString("topic"));
                                topicList.add(msg, cid);
                            }
                            else {
                                if (d_topicSet.add(topic))
                                    d_topicSet.notifyAll();
                            }
                        }
                    } else if (msg.messageType() == TOPIC_UNSUBSCRIBED) {
                        synchronized (d_topicSet) {
                            d_subscribedTopics.remove(
                                msg.getElementAsString(TOPIC));
                            Topic topic = session.getTopic(msg);
                            d_topicSet.remove(topic);
                        }
                    } else if (msg.messageType() == TOPIC_CREATED) {
                        Topic topic = session.getTopic(msg);
                        synchronized (d_topicSet) {
                            if (d_subscribedTopics.contains(
                                    msg.getElementAsString(TOPIC))) {
                                if (d_topicSet.add(topic))
                                    d_topicSet.notifyAll();
                            }
                        }
                    }
                    else if (msg.messageType() == TOPIC_RECAP) {
                        // Here we send a recap in response to a Recap Request.
                        Topic topic = session.getTopic(msg);
                        synchronized (d_topicSet) {
                            if (!d_topicSet.contains(topic)) {
                                continue;
                            }
                        }
                        Service service = topic.service();
                        Event recapEvent = service.createPublishEvent();
                        EventFormatter eventFormatter
                            = new EventFormatter(recapEvent);
                        eventFormatter.appendRecapMessage(topic,
                                                          msg.correlationID());
                        eventFormatter.setElement("OPEN", 100.0);

                        session.publish(recapEvent);
                        for (Message recapMsg: recapEvent) {
                            System.out.println(recapMsg);
                        }

                    }
                }

                // createTopicsAsync will result in RESOLUTION_STATUS,
                // TOPIC_CREATED events.
                if (topicList.size() > 0) {
                    session.createTopicsAsync(topicList);
                }
            } else if (event.eventType() == EventType.SERVICE_STATUS) {
                for (Message msg: event) {
                    if (msg.messageType() == SERVICE_REGISTERED) {
                        Object registerServiceResponseMonitor
                            = msg.correlationID().object();
                        synchronized (registerServiceResponseMonitor) {
                            d_registerServiceResponse = Boolean.TRUE;
                            registerServiceResponseMonitor.notify();
                        }
                    } else if (msg.messageType() == SERVICE_REGISTER_FAILURE) {
                        Object registerServiceResponseMonitor
                            = msg.correlationID().object();
                        synchronized (registerServiceResponseMonitor) {
                            d_registerServiceResponse = Boolean.FALSE;
                            registerServiceResponseMonitor.notify();
                        }
                    }
                }
            } else if (event.eventType() == EventType.RESOLUTION_STATUS) {
                for (Message msg: event) {
                    if (msg.messageType() == RESOLUTION_SUCCESS) {
                        String resolvedTopic
                            = msg.getElementAsString(
                                  Name.getName("resolvedTopic"));
                        System.out.println("ResolvedTopic: " + resolvedTopic);
                    } else if (msg.messageType() == RESOLUTION_FAILURE) {
                        System.out.println(
                                "Topic resolution failed (cid = " +
                                msg.correlationID() +
                                ")");
                    }
                }
            } else if (event.eventType() == EventType.REQUEST) {
                Service service = session.getService(d_service);
                for (Message msg: event) {
                    if (msg.messageType() == PERMISSION_REQUEST) {
                        // Similar to createPublishEvent. We assume just one
                        // service - d_service. A responseEvent can only be
                        // for single request so we can specify the
                        // correlationId - which establishes context -
                        // when we create the Event.
                        Event response
                            = service.createResponseEvent(msg.correlationID());
                        EventFormatter ef = new EventFormatter(response);
                        int permission = 1; // ALLOWED: 0, DENIED: 1
                        if (msg.hasElement("uuid")) {
                            int uuid = msg.getElementAsInt32("uuid");
                            System.out.println("UUID = " + uuid);
                            permission = 0;
                        }
                        if (msg.hasElement("applicationId")) {
                            int applicationId
                                = msg.getElementAsInt32("applicationId");
                            System.out.println("APPID = " + applicationId);
                            permission = 0;
                        }

                        // In appendResponse the string is the name of the
                        // operation, the correlationId indicates
                        // which request we are responding to.
                        ef.appendResponse("PermissionResponse");
                        ef.pushElement("topicPermissions");
                        // For each of the topics in the request, add an entry
                        // to the response
                        Element topicsElement
                            = msg.getElement(Name.getName("topics"));
                        for (int i = 0; i < topicsElement.numValues(); ++i) {
                            ef.appendElement();
                            ef.setElement("topic",
                                          topicsElement.getValueAsString(i));
                            ef.setElement("result", permission); // ALLOWED: 0,
                                                                 // DENIED: 1

                            if (permission == 1) {// DENIED
                                ef.pushElement("reason");
                                ef.setElement("source", "My Publisher Name");
                                ef.setElement("category", "NOT_AUTHORIZED");
                                // or BAD_TOPIC, or custom

                                ef.setElement("subcategory",
                                              "Publisher Controlled");
                                ef.setElement(
                                    "description",
                                    "Permission denied by My Publisher Name");
                                ef.popElement();
                            }
                            else { // ALLOWED
                                if (d_resolveSubServiceCode != null) {
                                    ef.setElement("subServiceCode",
                                                  d_resolveSubServiceCode);
                                    System.err.println(
                                        String.format(
                                            "Mapping topic %1$s to "
                                                + "subserviceCode %2$d",
                                            topicsElement.getValueAsString(i),
                                            d_resolveSubServiceCode));
                                }
                                if (!d_eids.isEmpty()) {
                                    ef.pushElement("permissions");
                                    ef.appendElement();
                                    ef.setElement("permissionService",
                                                  "//blp/blpperm");
                                    ef.pushElement("eids");
                                    for (int j = 0; j < d_eids.size(); ++j) {
                                        ef.appendValue(d_eids.get(j));
                                    }
                                    ef.popElement();
                                    ef.popElement();
                                    ef.popElement();
                                }
                            }
                            ef.popElement();
                        }
                        ef.popElement();
                        // Service is implicit in the Event. sendResponse has a
                        // second parameter - partialResponse -
                        // that defaults to false.
                        session.sendResponse(response);
                    } else {
                        System.out.println("Received unknown request: " + msg);
                    }
                }
            }
            else if (event.eventType() == EventType.RESPONSE
                    || event.eventType() == EventType.PARTIAL_RESPONSE
                    || event.eventType() == EventType.REQUEST_STATUS) {
                for (Message msg : event) {
                    if (msg.messageType() == AUTHORIZATION_SUCCESS) {
                        synchronized (d_authLock) {
                            d_authSuccess = true;
                            d_authLock.notifyAll();
                        }
                    }
                    else if (msg.messageType() == AUTHORIZATION_FAILURE) {
                        synchronized (d_authLock) {
                            d_authSuccess = false;
                            System.err.println("Not authorized: "
                                + msg.getElement("reason"));
                            d_authLock.notifyAll();
                        }
                    }
                    else {
                        assert d_authSuccess == true;
                        System.out.println("Permissions updated");
                    }
                }
            }
        }
    }

    private void printUsage() {
        System.out.println("Publish market data.");
        System.out.println("Usage:");
        System.out.println(
            "\t[-ip   <ipAddress>]  \tserver name or IP (default: localhost)");
        System.out.println(
            "\t[-p    <tcpPort>]    \tserver port (default: 8194)");
        System.out.println(
            "\t[-s    <service>]    \tservice name "
                + "(default: //viper/mktdata)");
        System.out.println(
            "\t[-g    <groupId>]    \tpublisher groupId "
                + "(defaults to a unique value)");
        System.out.println(
            "\t[-pri  <piority>]    \tpublisher priority "
                + "(default: Integer.MAX_VALUE)");
        System.out.println(
            "\t[-v]                 \tincrease verbosity "
                + "(can be specified more than once)");
        System.out.println(
            "\t[-c    <event count>]\tnumber of events after which cache will "
                + "be cleared (default: 0 i.e cache never cleared)");
        System.out.println(
            "\t[-ssc  <ssc range>]  \tactive sub-service code option: "
                + "<begin>,<end>,<priority>");
        System.out.println(
            "\t[-rssc <ssc >]       \tsub-service code to be used in resolve");
        System.out.println(
            "\t[-auth <option>]     \tauthentication option: "
                + "user|none|app=<app>|userapp=<app>|dir=<property> "
                + "(default: user)");
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
            else if (args[i].equalsIgnoreCase("-e") && i + 1 < args.length) {
                d_eids.add(Integer.parseInt(args[++i]));
            }
            else if (args[i].equalsIgnoreCase("-g") && i + 1 < args.length) {
                d_groupId = args[++i];
            }
            else if (args[i].equalsIgnoreCase("-pri") && i + 1 < args.length) {
                d_priority = Integer.parseInt(args[++i]);
            }
            else if (args[i].equalsIgnoreCase("-v")) {
                ++ d_verbose;
            }
            else if (args[i].equalsIgnoreCase("-c") && i + 1 < args.length) {
                d_clearInterval = Integer.parseInt(args[++i]);
            }
            else if (args[i].equalsIgnoreCase("-ssc") && i + 1 < args.length) {
                String[] splitRange = args[++i].split(",");
                if (splitRange.length != 3) {
                    printUsage();
                    return false;
                }
                d_useSsc = true;
                d_sscBegin = Integer.parseInt(splitRange[0]);
                d_sscEnd = Integer.parseInt(splitRange[1]);
                d_sscPriority = Integer.parseInt(splitRange[2]);
            }
            else if (args[i].equalsIgnoreCase("-rssc") && i + 1 < args.length) {
                d_resolveSubServiceCode = Integer.parseInt(args[++i]);
            }
            else if (args[i].equalsIgnoreCase("-auth")
                         && i + 1 < args.length) {
                ++i;
                if (args[i].equalsIgnoreCase("none")) {
                    d_authOptions = null;
                }
                else if (args[i].startsWith("user")) {
                    d_authOptions = AUTH_USER;
                }
                else if (args[i].startsWith("app=")) {
                    d_authOptions =
                        "AuthenticationMode=APPLICATION_ONLY;"
                        + "ApplicationAuthenticationType=APPNAME_AND_KEY;"
                        + "ApplicationName="
                        + args[i].substring(4);
                }
                else if (args[i].startsWith("dir=")) {
                    d_authOptions =
                        "AuthenticationType=DIRECTORY_SERVICE;"
                        + "DirSvcPropertyName="
                        + args[i].substring(4);
                }
                else if (args[i].startsWith("userapp=")) {
                    d_authOptions =
                        "AuthenticationMode=USER_AND_APPLICATION;"
                        + "AuthenticationType=OS_LOGON;"
                        + "ApplicationAuthenticationType=APPNAME_AND_KEY;"
                        + "ApplicationName="
                        + args[i].substring(8);
                }
                else {
                    printUsage();
                    return false;
                }
            } else if (args[i].equalsIgnoreCase("-h")) {
                printUsage();
                return false;
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

    private void activate(ProviderSession session) {
        if (d_useSsc) {
            System.out.println(
                String.format(
                    "Activating sub service code range [%1$d, %2$d] "
                        + "@ priority: %3$d",
                    d_sscBegin,
                    d_sscEnd,
                    d_sscPriority));
            session.activateSubServiceCodeRange(d_service,
                                                d_sscBegin,
                                                d_sscEnd,
                                                d_sscPriority);
        }
    }

    private void deactivate(ProviderSession session) {
        if (d_useSsc) {
            System.out.println(
                String.format(
                    "DeActivating sub service code range [%1$d, %2$d] "
                        + "@ priority: %3$d",
                    d_sscBegin,
                    d_sscEnd,
                    d_sscPriority));
            session.deactivateSubServiceCodeRange(d_service,
                                                  d_sscBegin,
                                                  d_sscEnd);
        }
    }

    private boolean authorize(
            Service authService,
            Identity providerIdentity,
            ProviderSession session)
    {
        EventQueue eventQueue = new EventQueue();
        String token = null;

        try {
            session.generateToken(new CorrelationID(), eventQueue);
        } catch (Exception e) {
            System.err.println("Timeout waiting for token");
            e.printStackTrace();
            return false;
        }
        while (token == null) {
            Event event = null;
            try {
                event = eventQueue.nextEvent(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return false;
            }
            if (event.eventType() != EventType.TOKEN_STATUS) {
                System.err.println("Failed to get token");
                return false;
            }
            for (Message msg: event) {
                if (msg.messageType() != TOKEN_SUCCESS) {
                    System.err.println("Token generation failed");
                    System.err.println(msg);
                    return false;
                }
                token = msg.getElementAsString("token");
                if (token.length() == 0) {
                    System.err.println("Got empty token");
                    return false;
                }
            }
        }

        Request authReq = authService.createAuthorizationRequest();
        authReq.set("token", token);
        try {
            session.sendAuthorizationRequest(authReq,
                                             providerIdentity,
                                             new CorrelationID());
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        synchronized (d_authLock) {
            try {
                d_authLock.wait(60000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                return false;
            }
            return d_authSuccess;
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

        ProviderSession session
            = new ProviderSession(sessionOptions, new MyEventHandler());

        if (!session.start()) {
            System.err.println("Failed to start session");
            return;
        }

        Identity identity = null;
        if (d_authOptions != null) {
            if (session.openService("//blp/apiauth")) {
                Service authService = session.getService("//blp/apiauth");
                identity = session.createIdentity();
                if (!authorize(authService, identity, session)) {
                    System.err.println("Failed to authorize");
                    return;
                }
            }
        }

        ServiceRegistrationOptions serviceRegistrationOptions
            = new ServiceRegistrationOptions();
        serviceRegistrationOptions.setGroupId(d_groupId);
        serviceRegistrationOptions.setServicePriority(d_priority);

        if (d_useSsc) {
            System.out.println(
                String.format(
                    "Activating sub service code range [%1$d, %2$d] "
                        + "@ priority: %3$d",
                    d_sscBegin,
                    d_sscEnd,
                    d_sscPriority));
            try {
                serviceRegistrationOptions.addActiveSubServiceCodeRange(
                    d_sscBegin,
                    d_sscEnd,
                    d_sscPriority);
            } catch(Exception e) {
                System.err.println(
                    "FAILED to add active sub service codes. Exception " + e);
            }
        }

        boolean wantAsyncRegisterService = true;
        if (wantAsyncRegisterService) {
            Object registerServiceResponseMonitor = new Object();
            CorrelationID registerCID
                = new CorrelationID(registerServiceResponseMonitor);
            synchronized (registerServiceResponseMonitor) {
                if (d_verbose > 0) {
                    System.out.println(
                        "start registerServiceAsync, cid = " + registerCID);
                }
                session.registerServiceAsync(
                        d_service,
                        identity,
                        registerCID,
                        serviceRegistrationOptions);
                for (int i = 0;
                        d_registerServiceResponse == null && i < 10;
                        ++ i) {
                    registerServiceResponseMonitor.wait(1000);
                }
            }
        } else {
            boolean result = session.registerService(
                    d_service,
                    identity,
                    serviceRegistrationOptions);
            d_registerServiceResponse = Boolean.valueOf(result);
        }

        Service service = session.getService(d_service);
        if (service != null && d_registerServiceResponse == Boolean.TRUE) {
            System.out.println("Service registered: " + d_service);
        } else {
            System.err.println("Service registration failed: " + d_service);
            return;
        }

        // Dump schema for the service
        if (d_verbose > 1) {
            System.out.println("Schema for service:" + d_service);
            for (int i = 0; i < service.numEventDefinitions(); ++i) {
                SchemaElementDefinition eventDefinition
                    = service.getEventDefinition(i);
                System.out.println(eventDefinition);
            }
        }

        // Now we will start publishing
        int eventCount = 0;
        long tickCount = 1;
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

                event = service.createPublishEvent();
                EventFormatter eventFormatter = new EventFormatter(event);
                boolean publishNull = false;
                if (d_clearInterval > 0 && eventCount == d_clearInterval) {
                    eventCount = 0;
                    publishNull = true;
                }

                for (Topic topic: d_topicSet) {
                    if (!topic.isActive()) {
                        System.out.println(
                            "[WARNING] Publishing on an inactive topic.");
                    }
                    eventFormatter.appendMessage("MarketDataEvents", topic);
                    if (publishNull) {
                        eventFormatter.setElementNull("HIGH");
                        eventFormatter.setElementNull("LOW");
                    } else {
                        ++eventCount;
                        if (1 == tickCount) {
                            eventFormatter.setElement("BEST_ASK", 100.0);
                        } else if (2 == tickCount) {
                            eventFormatter.setElement("BEST_BID", 99.0);
                        }
                        eventFormatter.setElement("HIGH",
                                                  100 + tickCount * 0.01);
                        eventFormatter.setElement("LOW",
                                                  100 - tickCount * 0.005);
                        ++ tickCount;
                    }
                }
            }

            for (Message msg: event) {
                System.out.println(msg);
            }

            session.publish(event);
            Thread.sleep(2 * 1000);

            if (tickCount % 3 == 0) {
                deactivate(session);
                Thread.sleep(10 * 1000);
                activate(session);
            }
        }

        session.stop();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("MktdataPublisherExample");
        MktdataPublisherExample example = new MktdataPublisherExample();
        example.run(args);
        System.out.println("Press ENTER to quit");
        System.in.read();
    }
}
