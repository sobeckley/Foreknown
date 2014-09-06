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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

import com.bloomberglp.blpapi.CorrelationID;
import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.ElementIterator;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.EventHandler;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.NameEnumeration;
import com.bloomberglp.blpapi.NameEnumerationTable;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.Subscription;
import com.bloomberglp.blpapi.SubscriptionList;

public class NameEnumerationExample {

    private SessionOptions    d_sessionOptions;
    private Session           d_session;
    private ArrayList<String> d_securities;
    private SubscriptionList  d_subscriptions;
    private SimpleDateFormat  d_dateFormat;

    private NameEnumerationTable d_subscriptionDataMsgEnumTable;
    private NameEnumerationTable d_subscriptionStatusMsgEnumTable;

    private static final String BLP_MKTDATA_SVC = "//blp/mktdata";

    public static class SubscriptionDataMsgType implements NameEnumeration {
        public static final int BID        = 1;
        public static final int ASK        = 2;
        public static final int LAST_PRICE = 3;
    }

    public static class SubscriptionStatusMsgType implements NameEnumeration {
        public static final int SUBSCRIPTION_STARTED    = 1;
        public static final int SUBSCRIPTION_FAILURE    = 2;
        public static final int SUBSCRIPTION_TERMINATED = 3;

        public static class NameBindings {
            public static final String SUBSCRIPTION_STARTED =
                "SubscriptionStarted";
            public static final String SUBSCRIPTION_FAILURE =
                "SubscriptionFailure";
            public static final String SUBSCRIPTION_TERMINATED =
                "SubscriptionTerminated";
        }
    }

    public static void main(String[] args) throws java.lang.Exception
    {
        System.out.println("Name Enumeration Example");
        NameEnumerationExample example = new NameEnumerationExample();
        example.run(args);
    }

    public NameEnumerationExample() throws Exception
    {
        d_sessionOptions = new SessionOptions();
        d_sessionOptions.setServerHost("localhost");
        d_sessionOptions.setServerPort(8194);

        d_securities = new ArrayList<String>();
        d_subscriptions = new SubscriptionList();
        d_dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");

        d_subscriptionDataMsgEnumTable = new NameEnumerationTable(
            new SubscriptionDataMsgType());
        d_subscriptionStatusMsgEnumTable = new NameEnumerationTable(
            new SubscriptionStatusMsgType());
    }

    private boolean createSession() throws Exception
    {
        if (d_session != null) d_session.stop();

        System.out.println("Connecting to " + d_sessionOptions.getServerHost() +
                           ":" + d_sessionOptions.getServerPort());
        d_session = new Session(d_sessionOptions, new SessionEventHandler());
        if (!d_session.start()) {
            System.err.println("Failed to start session");
            return false;
        }
        System.out.println("Connected successfully");

        if (!d_session.openService(BLP_MKTDATA_SVC)) {
            System.err.println("Failed to open service: " + BLP_MKTDATA_SVC);
            d_session.stop();
            return false;
        }

        System.out.println("Subscribing...");
        d_session.subscribe(d_subscriptions);

        return true;
    }


    private void run(String[] args) throws Exception
    {
        if (!parseCommandLine(args)) return;
        if (!createSession()) return;

        // wait for enter key to exit application
        System.in.read();

        d_session.stop();
        System.out.println("Exiting.");
    }

    class SessionEventHandler implements EventHandler
    {
        public void processEvent(Event event, Session session)
        {
            try {
                switch (event.eventType().intValue()) {
                    case Event.EventType.Constants.SUBSCRIPTION_DATA:
                        processSubscriptionDataEvent(event, session);
                        break;

                    case Event.EventType.Constants.SUBSCRIPTION_STATUS:
                        processSubscriptionStatus(event, session);
                        break;

                    default:
                        System.out.println("Processing " + event.eventType());
                        printEvent(event, session);
                        break;
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void processSubscriptionStatus(Event event, Session session)
        throws Exception
        {
            System.out.println("Processing SUBSCRIPTION_STATUS");
            MessageIterator msgIter = event.messageIterator();
            while (msgIter.hasNext()) {
                Message msg = msgIter.next();
                String topic = (String) msg.correlationID().object();
                switch (d_subscriptionStatusMsgEnumTable.get(
                    msg.messageType())) {
                    case SubscriptionStatusMsgType.SUBSCRIPTION_STARTED: {
                        System.out.println("Subscription for: " +
                            topic + " started");
                    } break;

                    case SubscriptionStatusMsgType.SUBSCRIPTION_FAILURE: {
                        System.out.println("Subscription for: " +
                                topic + " failed");
                        printEvent(event, session);
                    } break;

                    case SubscriptionStatusMsgType.SUBSCRIPTION_TERMINATED: {
                        System.out.println("Subscription for: " +
                                topic + " has been terminated");
                        printEvent(event, session);
                    } break;

                    default:
                        System.out.println("Unhandled subscription status " +
                            msg.messageType());
                }
            }
        }

        private void processSubscriptionDataEvent(Event event, Session session)
        throws Exception
        {
            System.out.println("Processing SUBSCRIPTION_DATA");
            MessageIterator msgIter = event.messageIterator();
            while (msgIter.hasNext()) {
                Message msg = msgIter.next();
                String topic = (String)msg.correlationID().object();
                ElementIterator elemIter = msg.asElement().elementIterator();
                while (elemIter.hasNext()) {
                    Element subsDataElement = elemIter.next();
                    switch (d_subscriptionDataMsgEnumTable.get(
                        subsDataElement.name())) {
                        case SubscriptionDataMsgType.BID:
                        case SubscriptionDataMsgType.ASK:
                        case SubscriptionDataMsgType.LAST_PRICE: {
                            printValue(topic, subsDataElement);
                        } break;
                    }
                }
            }
        }

        private boolean printEvent(Event event, Session session)
        throws Exception
        {
            MessageIterator msgIter = event.messageIterator();
            while (msgIter.hasNext()) {
                Message msg = msgIter.next();
                System.out.println(msg);
            }
            return true;
        }

        private void printValue(String topicName, Element element)
        {
            System.out.println(
                d_dateFormat.format(Calendar.getInstance().getTime()) +
                " " + topicName + " " + element.name() + " " +
                element.getValueAsString());
        }
    }

    private boolean parseCommandLine(String[] args)
    {
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equalsIgnoreCase("-s")) {
                d_securities.add(args[i+1]);
            }
            else if (args[i].equalsIgnoreCase("-ip")) {
                d_sessionOptions.setServerHost(args[i+1]);
            }
            else if (args[i].equalsIgnoreCase("-p")) {
                d_sessionOptions.setServerPort(Integer.parseInt(args[i+1]));
            }
            else if (args[i].equalsIgnoreCase("-h")) {
                printUsage();
                return false;
            }
        }

        if (d_securities.size() == 0) {
            d_securities.add("IBM US Equity");
        }

        for (int i = 0; i < d_securities.size(); ++i) {
            String security = (String)d_securities.get(i);
            d_subscriptions.add(new Subscription(
                    security, "BID,ASK,LAST_PRICE", "",
                    new CorrelationID(security)));
        }
        return true;
    }

    private void printUsage()
    {
        System.out.println("Usage:");
        System.out.println("    Name Enumeration Example");
        System.out.println("        [-s         <security   = IBM US Equity>");
        System.out.println("        [-ip        <ipAddress  = localhost>");
        System.out.println("        [-p         <tcpPort    = 8194>");
        System.out.println("Press ENTER to quit");
    }
 }
