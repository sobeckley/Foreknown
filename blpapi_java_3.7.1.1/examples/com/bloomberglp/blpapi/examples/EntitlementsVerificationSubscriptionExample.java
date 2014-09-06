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

import java.util.ArrayList;

import com.bloomberglp.blpapi.CorrelationID;
import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.EventHandler;
import com.bloomberglp.blpapi.Subscription;
import com.bloomberglp.blpapi.SubscriptionList;
import com.bloomberglp.blpapi.Identity;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.EventQueue;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.Event.EventType;

public class EntitlementsVerificationSubscriptionExample {

    private String              d_host;
    private int                 d_port;
    private ArrayList<String>   d_securities;
    private String              d_field;
    private ArrayList<Integer>  d_uuids;
    private ArrayList<Identity> d_identities;
    private ArrayList<String>   d_programAddresses;

    private Name                d_fieldAsName;
    private Session             d_session;
    private Service             d_apiAuthSvc;
    private SubscriptionList    d_subscriptions;

    private static final Name AUTHORIZATION_SUCCESS = Name.getName(
    "AuthorizationSuccess");

    private static final Name EID = Name.getName("EID");
    private static final String API_AUTH_SVC_NAME = "//blp/apiauth";
    private static final String MKT_DATA_SVC_NAME = "//blp/mktdata";

    public static void main(String[] args) throws Exception
    {
    System.out.println("Entitlements Verification Example");
    EntitlementsVerificationSubscriptionExample example =
        new EntitlementsVerificationSubscriptionExample();
    example.run(args);
    }

    public EntitlementsVerificationSubscriptionExample()
    {
    d_host = "localhost";
    d_port = 8194;

    d_securities       = new ArrayList<String>();
    d_field            = "BEST_BID1";
    d_fieldAsName      = Name.getName(d_field);
    d_uuids            = new ArrayList<Integer>();
    d_identities       = new ArrayList<Identity>();
    d_programAddresses = new ArrayList<String>();

    d_subscriptions    = new SubscriptionList();
    }

    private void run(String[] args) throws Exception
    {
    if (!parseCommandLine(args)) return;

    createSession();
    openServices();

    // Authorize all the users that are interested in receiving data
    authorizeUsers();

    // Make the various requests that we need to make
    d_session.subscribe(d_subscriptions);

    // wait for enter key to exit application
    System.in.read();

    d_session.stop();
    System.out.println("Exiting.");
    }

    private void createSession() throws Exception
    {
    SessionOptions options = new SessionOptions();
    options.setServerHost(d_host);
    options.setServerPort(d_port);

    System.out.println("Connecting to " + d_host + ":" + d_port);

    d_session = new Session(options, new SessionEventHandler());
    boolean sessionStarted = d_session.start();
    if (!sessionStarted) {
        System.err.println("Failed to start session. Exiting...");
        System.exit(-1);
    }
    }

    private void openServices() throws Exception
    {
    if (!d_session.openService(API_AUTH_SVC_NAME)) {
        System.out.println("Failed to open service: " + API_AUTH_SVC_NAME);
        System.exit(-1);
    }

    if (!d_session.openService(MKT_DATA_SVC_NAME)) {
        System.out.println("Failed to open service: " + MKT_DATA_SVC_NAME);
        System.exit(-2);
    }

    d_apiAuthSvc    = d_session.getService(API_AUTH_SVC_NAME);
    }

    private class SessionEventHandler implements EventHandler
    {
    public void processEvent(Event event, Session session)
    {
        try {
        switch(event.eventType().intValue()) {
        case EventType.Constants.SESSION_STATUS:
        case EventType.Constants.SERVICE_STATUS:
        case EventType.Constants.REQUEST_STATUS:
        case EventType.Constants.AUTHORIZATION_STATUS:
            printEvent(event);
            break;

        case EventType.Constants.SUBSCRIPTION_DATA:
            processSubscriptionDataEvent(event);
            break;
        }
        }
        catch (Exception e) {
        e.printStackTrace();
        }
    }
    }

    private void authorizeUsers() throws Exception
    {
    // Authorize each of the users
    for (int i = 0; i < d_uuids.size(); ++i) {
        int uuid = d_uuids.get(i).intValue();
        Identity userIdentity = d_session.createIdentity();
        d_identities.add(userIdentity);

        Request authRequest = d_apiAuthSvc.createAuthorizationRequest();
        authRequest.set("uuid", uuid);
        authRequest.set("ipAddress", d_programAddresses.get(i));

        CorrelationID correlator = new CorrelationID(i);
        EventQueue eventQueue = new EventQueue();
        d_session.sendAuthorizationRequest(authRequest, userIdentity,
            eventQueue, correlator);

        Event event = eventQueue.nextEvent();
        if (event.eventType() == Event.EventType.RESPONSE ||
            event.eventType() == Event.EventType.REQUEST_STATUS) {
        MessageIterator msgIter = event.messageIterator();
        if (msgIter.hasNext()) {
            Message msg = msgIter.next();
            if (msg.messageType().equals(AUTHORIZATION_SUCCESS)) {
            System.out.println(uuid + " authorization success");
            }
            else {
            System.out.println(uuid + " authorization failed");
            printEvent(event);
            }
        }
        }
    }
    }

    private void processSubscriptionDataEvent(Event event) throws Exception
    {
        MessageIterator msgIter = event.messageIterator();

        while (msgIter.hasNext()) {
            Message msg = msgIter.next();
            Service service = msg.service();
            if (!msg.hasElement(d_fieldAsName)) {
                continue;
            }
            String topic = (String)msg.correlationID().object();
            System.out.println("\t" + topic + " - " + msg.messageType());
            Element field = msg.getElement(d_fieldAsName);
            if (field.isNull()) {
                System.out.println(d_field + " is null, ignoring");
                continue;
            }
            boolean needsEntitlement = msg.hasElement(EID);
            for (int i = 0; i < d_identities.size(); ++i) {
                Identity userIdentity = d_identities.get(i);
                int uuid = d_uuids.get(i);
                if (!needsEntitlement || userIdentity.hasEntitlements(
                        msg.getElement(EID), service)) {
                    System.out.println("User: " + uuid + " is entitled to "
                            + field);
                    // Now Distribute message to the user.
                } else {
                    System.out.println("User: " + uuid + " is NOT entitled for "
                            + d_field + " because of " + msg.getElement(EID));
                }
            }
        }
    }

    private boolean parseCommandLine(String[] args)
    {
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equalsIgnoreCase("-s")) {
                d_securities.add(args[i+1]);
            }
            else if (args[i].equalsIgnoreCase("-f")) {
                d_field = args[i+1];
                d_fieldAsName = Name.getName(d_field);
            }
            else if (args[i].equalsIgnoreCase("-c")) {
                String credential = args[i+1];
                String[] credentialElements = credential.split(":");
                d_uuids.add(Integer.valueOf(credentialElements[0]));
                d_programAddresses.add(credentialElements[1]);
            }
            else if (args[i].equalsIgnoreCase("-ip")) {
                d_host = args[i+1];
            }
            else if (args[i].equalsIgnoreCase("-p")) {
                d_port = Integer.parseInt(args[i+1]);
            }
            else if (args[i].equalsIgnoreCase("-h")) {
                printUsage();
                return false;
            }
        }

        if (d_uuids.size() <= 0) {
            System.out.println("No uuids were specified");
            return false;
        }

        if (d_uuids.size() != d_programAddresses.size()) {
            System.out.println("Invalid number of program addresses provided");
        }

        if (d_securities.size() == 0) {
            d_securities.add("MSFT US Equity");
        }

        for (String security : d_securities) {
            d_subscriptions.add(new Subscription(
                    security, d_field, "", new CorrelationID(security)));
        }

        return true;
    }

    private void printUsage()
    {
    System.out.println("Usage:");
    System.out.println("    Entitlements Verification" +
    " Subscription example");
    System.out.println("   [-s     <security   = MSFT US Equity>]");
    System.out.println("   [-f     <field      = BEST_BID1>");
    System.out.println("   [-c     <credential uuid:ipAddress" +
    " eg:12345:10.20.30.40>]");
    System.out.println("   [-ip    <ipAddress  = localhost>]");
    System.out.println("   [-p     <tcpPort    = 8194>]");
    System.out.println("Note:");
    System.out.println("Multiple securities and credentials can be" +
    " specified. Only one field can be specified.");
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
}
