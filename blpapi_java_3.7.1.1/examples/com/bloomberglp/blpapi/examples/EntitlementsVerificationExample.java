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
import java.util.List;

import com.bloomberglp.blpapi.CorrelationID;
import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.EventHandler;
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

public class EntitlementsVerificationExample {

    private String              d_host;
    private int                 d_port;
    private ArrayList<String>   d_securities;
    private ArrayList<Integer>  d_uuids;
    private ArrayList<Identity> d_users;
    private ArrayList<String>   d_programAddresses;

    private Session             d_session;
    private Service             d_apiAuthSvc;
    private Service             d_blpRefDataSvc;

    private static final Name RESPONSE_ERROR = Name.getName("responseError");
    private static final Name SECURITY_DATA = Name.getName("securityData");
    private static final Name SECURITY = Name.getName("security");
    private static final Name EID_DATA = Name.getName("eidData");
    private static final Name AUTHORIZATION_SUCCESS = Name.getName(
            "AuthorizationSuccess");
    private static final Name AUTHORIZATION_REVOKED = Name.getName(
            "AuthorizationRevoked");
    private static final Name ENTITITLEMENT_CHANGED = Name.getName(
            "EntitlementChanged");

    private static final String API_AUTH_SVC_NAME = "//blp/apiauth";
    private static final String REF_DATA_SVC_NAME = "//blp/refdata";

    public static void main(String[] args) throws Exception
    {
        System.out.println("Entitlements Verification Example");
        EntitlementsVerificationExample example =
            new EntitlementsVerificationExample();
        example.run(args);
    }

    public EntitlementsVerificationExample()
    {
        d_host = "localhost";
        d_port = 8194;

        d_securities       = new ArrayList<String>();
        d_uuids            = new ArrayList<Integer>();
        d_users            = new ArrayList<Identity>();
        d_programAddresses = new ArrayList<String>();
    }

    private void run(String[] args) throws Exception
    {
        if (!parseCommandLine(args)) return;

        createSession();
        openServices();

        // Authorize all the users that are interested in receiving data
        authorizeUsers();

        // Make the various requests that we need to make
        sendRefDataRequest();

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

        if (!d_session.openService(REF_DATA_SVC_NAME)) {
            System.out.println("Failed to open service: " + REF_DATA_SVC_NAME);
            System.exit(-2);
        }

        d_apiAuthSvc    = d_session.getService(API_AUTH_SVC_NAME);
        d_blpRefDataSvc = d_session.getService(REF_DATA_SVC_NAME);
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
                        printEvent(event);
                    break;
                    case EventType.Constants.AUTHORIZATION_STATUS:
                    {
                        processAuthStatusEvent(event);
                    }
                    break;

                    case EventType.Constants.RESPONSE:
                    case EventType.Constants.PARTIAL_RESPONSE:
                        processResponseEvent(event);
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
            Identity userIdentity = d_session.createIdentity();
            d_users.add(userIdentity);

            Request authRequest = d_apiAuthSvc.createAuthorizationRequest();
            authRequest.set("uuid", d_uuids.get(i).intValue());
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
                        System.out.println(d_uuids.get(i) +
                            " authorization success");
                    }
                    else {
                        System.out.println(d_uuids.get(i) +
                            " authorization failed");
                        printEvent(event);
                    }
                }
            }
        }
    }

    private void sendRefDataRequest() throws Exception
    {
        Request request = d_blpRefDataSvc.createRequest("ReferenceDataRequest");

        // Add securities.
        Element securities = request.getElement("securities");
        for (int i = 0; i < d_securities.size(); ++i) {
            securities.appendValue(d_securities.get(i));
        }

        // Add fields
        Element fields = request.getElement("fields");
        fields.appendValue("PX_LAST");
        fields.appendValue("DS002");

        request.set("returnEids", true);

        // Send the request using the server's credentials
        System.out.println("Sending RefDataRequest using server " +
                "credentials...");
        d_session.sendRequest(request, null);
    }

    private void processResponseEvent(Event event) throws Exception
    {
        MessageIterator msgIter = event.messageIterator();
        while (msgIter.hasNext()) {
            Message msg = msgIter.next();
            if (msg.hasElement(RESPONSE_ERROR)) {
                System.out.println(msg);
                continue;
            }
            // We have a valid response. Distribute it to all the users.
            distributeMessage(msg);
        }
    }

    private void processAuthStatusEvent(Event event) throws Exception
    {
        MessageIterator msgIter = event.messageIterator();
        if (msgIter.hasNext()) {
            Message msg = msgIter.next();
            CorrelationID correlationId = msg.correlationID();
            int userid = (int)correlationId.value();
            if (msg.messageType() == AUTHORIZATION_REVOKED) {
                Element errorinfo = msg.getElement("reason");
                int code = errorinfo.getElementAsInt32("code");
                String reason = errorinfo.getElementAsString("message");
                System.out.println( "Authorization revoked for uuid " +
                        d_uuids.get(userid) +
                        " with code " + code + " and reason\n\t" + reason);
                /* Reauthorize user here if required, and obtain a new identity.
                 * Existing identity is invalid.
                 */
            } else if (msg.messageType() == ENTITITLEMENT_CHANGED) {
                System.out.println( "Entitlements updated for uuid " +
                        d_uuids.get(userid));
                /* This is just informational.
                 * Continue to use existing identity.
                 */
            }
        }
    }

    private void distributeMessage(Message msg) throws Exception
    {
        Service service = msg.service();

        ArrayList<Integer> failedEntitlements = new ArrayList<Integer>();
        Element securities = msg.getElement(SECURITY_DATA);
        int numSecurities = securities.numValues();

        System.out.println("Processing " + numSecurities + " securities:");
        for (int i = 0; i < numSecurities; ++i) {
            Element security     = securities.getValueAsElement(i);
            String ticker        = security.getElementAsString(SECURITY);
            Element entitlements = ((security.hasElement(EID_DATA) ?
                    security.getElement(EID_DATA) : null));

            int numUsers = d_users.size();
            if (entitlements != null) {
                // Entitlements are required to access this data
                for (int j = 0; j < numUsers; ++j) {
                    failedEntitlements.clear();
                    Identity userIdentity = d_users.get(j);
                    if (userIdentity.hasEntitlements(entitlements, service,
                            failedEntitlements)) {
                        System.out.println("User: " + d_uuids.get(j) +
                                " is entitled to get data for: " + ticker);
                        // Now Distribute message to the user.
                    }
                    else {
                        System.out.print("User: " + d_uuids.get(j) +
                                " is NOT entitled to get data for: " + ticker +
                                " - Failed eids: ");
                        printFailedEntitlements(failedEntitlements);
                    }
                }
            }
            else {
                // No Entitlements are required to access this data.
                for (int j = 0; j < numUsers; ++j) {
                    System.out.println("User: " + d_uuids.get(j) +
                            " is entitled to get data for: " + ticker);
                    // Now Distribute message to the user.
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

        if (d_securities.size() <= 0) {
            d_securities.add("IBM US Equity");
        }

        return true;
    }

    private void printUsage()
    {
        System.out.println("Usage:");
        System.out.println("    Entitlements verification example");
        System.out.println("        [-s     <security   = IBM US Equity>]");
        System.out.println("        [-c     <credential uuid:ipAddress" +
                " eg:12345:10.20.30.40>]");
        System.out.println("        [-ip    <ipAddress  = localhost>]");
        System.out.println("        [-p     <tcpPort    = 8194>]");
        System.out.println("        [-a     <program address>]");
        System.out.println("Note:");
        System.out.println("Multiple securities and credentials can be" +
                " specified.");
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

    private void printFailedEntitlements(List<Integer> failedEntitlements)
    {
        for (Integer item : failedEntitlements) {
            System.out.print(item + " ");
        }
        System.out.println();
    }
}
