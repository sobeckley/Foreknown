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

import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.EventHandler;
import com.bloomberglp.blpapi.EventQueue;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.Subscription;
import com.bloomberglp.blpapi.SubscriptionList;

public class SimpleBlockingRequestExample {
    private Name LAST_PRICE = new Name("LAST_PRICE");

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        SimpleBlockingRequestExample example = new SimpleBlockingRequestExample();
        example.run(args);
    }

    private void run(String[] args) throws Exception {
        String serverHost = "localhost";
        int serverPort = 8194;

        SessionOptions sessionOptions = new SessionOptions();
        sessionOptions.setServerHost(serverHost);
        sessionOptions.setServerPort(serverPort);

        System.out.println("Connecting to " + serverHost + ":" + serverPort);
        Session session = new Session(sessionOptions, new MyEventHandler());
        if (!session.start()) {
            System.err.println("Failed to start session.");
            return;
        }
        if (!session.openService("//blp/mktdata")) {
            System.err.println("Failed to open //blp/mktdata");
            return;
        }
        if (!session.openService("//blp/refdata")) {
            System.err.println("Failed to open //blp/refdata");
            return;
        }

        System.out.println("Subscribing to IBM US Equity");
        Subscription s = new Subscription("IBM US Equity", "LAST_PRICE", "");
        SubscriptionList subscriptions = new SubscriptionList();
        subscriptions.add(s);
        session.subscribe(subscriptions);

        System.out.println("Requesting reference data IBM US Equity");
        Service refDataService = session.getService("//blp/refdata");
        Request request = refDataService.createRequest("ReferenceDataRequest");
        request.getElement("securities").appendValue("IBM US Equity");
        request.getElement("fields").appendValue("DS002");

        EventQueue eventQueue = new EventQueue();
        session.sendRequest(request, eventQueue, null);
        while (true) {
            Event event = eventQueue.nextEvent();
            MessageIterator msgIter = event.messageIterator();
            while (msgIter.hasNext()) {
                Message msg = msgIter.next();
                System.out.println(msg);
            }
            if (event.eventType() == Event.EventType.RESPONSE) {
                break;
            }
        }

        // wait for enter key to exit application
        System.in.read();
        System.out.println("Exiting");
    }

    class MyEventHandler implements EventHandler {
        // Process events using callback

        public void processEvent(Event event, Session session) {
            try {
                if (event.eventType() == Event.EventType.SUBSCRIPTION_DATA) {
                    MessageIterator msgIter = event.messageIterator();
                    while (msgIter.hasNext()) {
                        Message msg = msgIter.next();
                        if (msg.hasElement(LAST_PRICE)) {
                            Element field = msg.getElement(LAST_PRICE);
                            System.out.println(event.eventType() + ": " +
                                    field.name() + " = " +
                                    field.getValueAsString());
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
