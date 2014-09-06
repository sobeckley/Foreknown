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
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.EventHandler;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.Subscription;
import com.bloomberglp.blpapi.SubscriptionList;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

public class SubscriptionWithEventHandlerExample
{
    private static final Name SLOW_CONSUMER_WARNING = Name.getName("SlowConsumerWarning");
    private static final Name SLOW_CONSUMER_WARNING_CLEARED =
        Name.getName("SlowConsumerWarningCleared");
    private static final Name DATA_LOSS = Name.getName("DataLoss");
    private static final Name SUBSCRIPTION_TERMINATED = Name.getName("SubscriptionTerminated");
    private static final Name SOURCE = Name.getName("source");

    private SessionOptions            d_sessionOptions;
    private Session                   d_session;
    private ArrayList<String>         d_topics;
    private ArrayList<String>         d_fields;
    private ArrayList<String>         d_options;
    private SubscriptionList          d_subscriptions;
    private SimpleDateFormat          d_dateFormat;
    private String                    d_service;
    private boolean                   d_isSlow;
    private boolean                   d_isStopped;
    private final SubscriptionList    d_pendingSubscriptions;
    private final Set<CorrelationID>  d_pendingUnsubscribe;
    private final Object              d_lock;
    /**
     * @param args
     */
    public static void main(String[] args) throws java.lang.Exception
    {
        System.out.println("Realtime Event Handler Example");
        SubscriptionWithEventHandlerExample example = new SubscriptionWithEventHandlerExample();
        example.run(args);
    }

    public SubscriptionWithEventHandlerExample()
    {
        d_sessionOptions = new SessionOptions();
        d_sessionOptions.setServerHost("localhost");
        d_sessionOptions.setServerPort(8194);
        d_sessionOptions.setMaxEventQueueSize(10000);

        d_service = "//blp/mktdata";
        d_topics = new ArrayList<String>();
        d_fields = new ArrayList<String>();
        d_options = new ArrayList<String>();
        d_subscriptions = new SubscriptionList();
        d_dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        d_isSlow = false;
        d_isStopped = false;
        d_pendingSubscriptions = new SubscriptionList();
        d_pendingUnsubscribe = new HashSet<CorrelationID>();
        d_lock = new Object();
    }

    private boolean createSession() throws Exception
    {
        if (d_session != null) d_session.stop();

        System.out.printf(
                "Connecting to %s:%d%n",
                d_sessionOptions.getServerHost(),
                d_sessionOptions.getServerPort());
        if (!"//blp/mktdata".equalsIgnoreCase(d_service)) {
            d_sessionOptions.setDefaultSubscriptionService(d_service);
        }
        d_session = new Session(d_sessionOptions, new SubscriptionEventHandler());
        if (!d_session.start()) {
            System.err.println("Failed to start session");
            return false;
        }
        System.out.println("Connected successfully\n");

        if (!d_session.openService(d_service)) {
            System.err.printf("Failed to open service: %s%n", d_service);
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
        synchronized (d_lock) {
            d_isStopped = true;
        }
        d_session.stop();
        System.out.println("Exiting.");
    }

    class SubscriptionEventHandler implements EventHandler
    {
        public void processEvent(Event event, Session session)
        {
            try {
                switch (event.eventType().intValue())
                {
                case Event.EventType.Constants.SUBSCRIPTION_DATA:
                    processSubscriptionDataEvent(event, session);
                    break;
                case Event.EventType.Constants.SUBSCRIPTION_STATUS:
                    synchronized (d_lock) {
                        processSubscriptionStatus(event, session);
                    }
                    break;
                case Event.EventType.Constants.ADMIN:
                    synchronized (d_lock) {
                        processAdminEvent(event, session);
                    }
                    break;
                default:
                    processMiscEvents(event, session);
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private boolean processSubscriptionStatus(Event event, Session session)
        throws Exception
        {
            System.out.println("Processing SUBSCRIPTION_STATUS: ");
            SubscriptionList subscriptionList = null;
            MessageIterator msgIter = event.messageIterator();
            while (msgIter.hasNext()) {
                Message msg = msgIter.next();
                CorrelationID cid = msg.correlationID();
                String topic = (String) cid.object();
                System.out.printf(
                        "%s: %s%n",
                        d_dateFormat.format(Calendar.getInstance().getTime()),
                        topic);
                System.out.println("MESSAGE: " + msg);

                if (msg.messageType() == SUBSCRIPTION_TERMINATED
                        && d_pendingUnsubscribe.remove(cid)) {
                    // If this message was due to a previous unsubscribe
                    Subscription subscription = getSubscription(cid);
                    if (d_isSlow) {
                        System.out.printf(
                                "Deferring subscription for topic = %s because session is slow.%n",
                                topic);
                        d_pendingSubscriptions.add(subscription);
                    }
                    else {
                        if (subscriptionList == null) {
                            subscriptionList = new SubscriptionList();
                        }
                        subscriptionList.add(subscription);
                    }
                }
            }

            if (subscriptionList != null && !d_isStopped) {
                session.subscribe(subscriptionList);
            }
            return true;
        }

        private boolean processSubscriptionDataEvent(Event event, Session session)
        throws Exception
        {
            System.out.println("Processing SUBSCRIPTION_DATA");
            MessageIterator msgIter = event.messageIterator();
            while (msgIter.hasNext()) {
                Message msg = msgIter.next();
                String topic = (String) msg.correlationID().object();
                System.out.printf(
                        "%s: %s%n",
                        d_dateFormat.format(Calendar.getInstance().getTime()),
                        topic);
                System.out.println(msg);
            }
            return true;
        }

        private boolean processAdminEvent(Event event, Session session)
        throws Exception
        {
            System.out.println("Processing ADMIN: ");
            ArrayList<CorrelationID> cidsToCancel = null;
            boolean previouslySlow = d_isSlow;
            MessageIterator msgIter = event.messageIterator();
            while (msgIter.hasNext()) {
                Message msg = msgIter.next();
                // An admin event can have more than one messages.
                if (msg.messageType() == SLOW_CONSUMER_WARNING) {
                    System.out.printf("MESSAGE: %s%n", msg);
                    d_isSlow = true;
                }
                else if (msg.messageType() == SLOW_CONSUMER_WARNING_CLEARED) {
                    System.out.printf("MESSAGE: %s%n", msg);
                    d_isSlow = false;
                }
                else if (msg.messageType() == DATA_LOSS) {
                    CorrelationID cid = msg.correlationID();
                    String topic = (String) cid.object();
                    System.out.printf(
                            "%s: %s%n",
                            d_dateFormat.format(Calendar.getInstance().getTime()),
                            topic);
                    System.out.printf("MESSAGE: %s%n", msg);
                    if (msg.hasElement(SOURCE)) {
                        String sourceStr = msg.getElementAsString(SOURCE);
                        if (sourceStr.compareTo("InProc") == 0
                                && !d_pendingUnsubscribe.contains(cid)) {
                            // DataLoss was generated "InProc". This can only happen if
                            // applications are processing events slowly and hence are not
                            // able to keep-up with the incoming events.
                            if (cidsToCancel == null) {
                                cidsToCancel = new ArrayList<CorrelationID>();
                            }
                            cidsToCancel.add(cid);
                            d_pendingUnsubscribe.add(cid);
                        }
                    }
                }
            }

            if (!d_isStopped) {
                if (cidsToCancel != null) {
                    session.cancel(cidsToCancel);
                }
                else if ((previouslySlow && !d_isSlow) && !d_pendingSubscriptions.isEmpty()){
                    // Session was slow but is no longer slow. subscribe to any topics
                    // for which we have previously received SUBSCRIPTION_TERMINATED
                    System.out.printf(
                            "Subscribing to topics - %s%n",
                            getTopicsString(d_pendingSubscriptions));
                    session.subscribe(d_pendingSubscriptions);
                    d_pendingSubscriptions.clear();
                }
            }
            return true;
        }

        private boolean processMiscEvents(Event event, Session session)
        throws Exception
        {
            System.out.printf("Processing %s%n", event.eventType());
            MessageIterator msgIter = event.messageIterator();
            while (msgIter.hasNext()) {
                Message msg = msgIter.next();
                System.out.printf(
                        "%s: %s%n",
                        d_dateFormat.format(Calendar.getInstance().getTime()),
                        msg.messageType());
            }
            return true;
        }

        private Subscription getSubscription(CorrelationID cid)
        {
            for (Subscription subscription : d_subscriptions) {
                if (subscription.correlationID().equals(cid)) {
                    return subscription;
                }
            }
            throw new IllegalArgumentException(
                    "No subscription found corresponding to cid = " + cid.toString());
        }

        private String getTopicsString(SubscriptionList list)
        {
            StringBuilder strBuilder = new StringBuilder();
            for (int count = 0; count < list.size(); ++count) {
                Subscription subscription = list.get(count);
                if (count != 0) {
                    strBuilder.append(", ");
                }
                strBuilder.append((String) subscription.correlationID().object());
            }
            return strBuilder.toString();
        }
    }

    private boolean parseCommandLine(String[] args)
    {
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equalsIgnoreCase("-t") && i + 1 < args.length) {
                d_topics.add(args[++i]);
            }
            else if (args[i].equalsIgnoreCase("-f") && i + 1 < args.length) {
                d_fields.add(args[++i]);
            }
            else if (args[i].equalsIgnoreCase("-s") && i + 1 < args.length) {
                d_service = args[++i];
            }
            else if (args[i].equalsIgnoreCase("-o") && i + 1 < args.length) {
                d_options.add(args[++i]);
            }
            else if (args[i].equalsIgnoreCase("-ip") && i + 1 < args.length) {
                d_sessionOptions.setServerHost(args[++i]);
            }
            else if (args[i].equalsIgnoreCase("-p") && i + 1 < args.length) {
                d_sessionOptions.setServerPort(Integer.parseInt(args[++i]));
            }
            else if (args[i].equalsIgnoreCase("-qsize") && i + 1 < args.length) {
                d_sessionOptions.setMaxEventQueueSize(Integer.parseInt(args[++i]));
            }
            else {
                printUsage();
                return false;
            }
        }

        if (d_fields.isEmpty()) {
            d_fields.add("LAST_PRICE");
        }

        if (d_topics.isEmpty()) {
            d_topics.add("IBM US Equity");
        }

        for (String topic : d_topics) {
            d_subscriptions.add(new Subscription(topic, d_fields, d_options,
                    new CorrelationID(topic)));
        }

        return true;
    }

    private void printUsage()
    {
        System.out.println("Usage:");
        System.out.println("    Retrieve realtime data ");
        System.out.println("        [-ip        <ipAddress  = localhost>");
        System.out.println("        [-p         <tcpPort    = 8194>");
        System.out.println("        [-s         <service    = //blp/mktdata>");
        System.out.println("        [-t         <topic  = IBM US Equity>");
        System.out.println("        [-f         <field      = LAST_PRICE>");
        System.out.println("        [-o         <subscriptionOptions>");
        System.out.println("        [-qsize     <qsize  = 10000>");
        System.out.println("Press ENTER to quit");
    }
}
