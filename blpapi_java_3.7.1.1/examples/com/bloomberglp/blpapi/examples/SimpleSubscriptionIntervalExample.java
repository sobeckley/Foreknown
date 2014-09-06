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
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.Subscription;
import com.bloomberglp.blpapi.SubscriptionList;

public class SimpleSubscriptionIntervalExample
{
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception
    {
        SimpleSubscriptionIntervalExample example =
            new SimpleSubscriptionIntervalExample();
        example.run(args);
        System.out.println("Press ENTER to quit");
        System.in.read();
    }

    private void run(String[] args) throws Exception
    {
        String serverHost = "localhost";
        int serverPort = 8194;

        SessionOptions sessionOptions = new SessionOptions();
        sessionOptions.setServerHost(serverHost);
        sessionOptions.setServerPort(serverPort);

        System.out.println("Connecting to " + serverHost + ":" + serverPort);
        Session session = new Session(sessionOptions);
        if (!session.start()) {
            System.err.println("Failed to start session.");
            return;
        }

        System.out.println("Connected successfully.");
        if (!session.openService("//blp/mktdata")) {
            System.err.println("Failed to open //blp/mktdata");
            session.stop();
            return;
        }

        String security = new String("IBM US Equity");
        SubscriptionList subscriptions = new SubscriptionList();
        // subscribe with a 1 second interval
        subscriptions.add(new Subscription(
                security, "LAST_PRICE,BID,ASK", "interval=1.0",
                new CorrelationID(security)));

        System.out.println("Subscribing...");
        session.subscribe(subscriptions);

        while (true) {
            Event event = session.nextEvent();
            MessageIterator msgIter = event.messageIterator();
            while (msgIter.hasNext()) {
                Message msg = msgIter.next();
                if (event.eventType() == Event.EventType.SUBSCRIPTION_DATA ||
                    event.eventType() == Event.EventType.SUBSCRIPTION_STATUS) {
                    String topic = (String)msg.correlationID().object();
                    System.out.println(topic + ": " + msg.asElement());
                }
            }
        }
    }
}
