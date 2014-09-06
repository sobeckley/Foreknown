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
import java.util.List;

import com.bloomberglp.blpapi.CorrelationID;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.Subscription;
import com.bloomberglp.blpapi.SubscriptionList;

public class SubscriptionCorrelationExample {
    private Session             d_session;
    private SessionOptions      d_sessionOptions;
    private ArrayList<String>   d_securityList;
    private GridWindow          d_gridWindow;

    public class GridWindow {
        private String            d_name;
        private List<String> d_securityList;

        public GridWindow(String name, List<String> securityList) {
            d_name = name;
            d_securityList = securityList;
        }

        public void processSecurityUpdate(Message msg, long row) {
            String topic = d_securityList.get((int)row);
            System.out.println(d_name + ": row " + row +
                                " got update for " + topic);
        }
    }

    public SubscriptionCorrelationExample() {
        d_sessionOptions = new SessionOptions();
        d_sessionOptions.setServerHost("localhost");
        d_sessionOptions.setServerPort(8194);

        d_securityList = new ArrayList<String>();
        d_securityList.add("IBM US Equity");
        d_securityList.add("VOD LN Equity");

        d_gridWindow = new GridWindow("SecurityInfo", d_securityList);
    }

    private boolean createSession() throws Exception {
        System.out.println("Connecting to " + d_sessionOptions.getServerHost()
                            + ":" + d_sessionOptions.getServerPort());
        d_session = new Session(d_sessionOptions);
        if (!d_session.start()) {
            System.err.println("Failed to connect!");
            return false;
        }
        if (!d_session.openService("//blp/mktdata")) {
            System.err.println("Failed to open //blp/mktdata");
            return false;
        }
        return true;
    }

    private void run(String[] args) throws Exception {
        if (!createSession()) return;

        SubscriptionList subscriptionList = new SubscriptionList();
        for (int i = 0; i < d_securityList.size(); ++i) {
            subscriptionList.add(new Subscription(d_securityList.get(i),
                              "LAST_PRICE", new CorrelationID(i)));
        }
        d_session.subscribe(subscriptionList);

        while (true) {
            Event event = d_session.nextEvent();
            MessageIterator msgIter = event.messageIterator();
            while (msgIter.hasNext()) {
                Message msg = msgIter.next();
                if (event.eventType() == Event.EventType.SUBSCRIPTION_DATA) {
                    long row = msg.correlationID().value();
                    d_gridWindow.processSecurityUpdate(msg, row);
                }
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("SubscriptionCorrelationExample");
        SubscriptionCorrelationExample example = new SubscriptionCorrelationExample();
        try {
            example.run(args);
        } catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("Press ENTER to quit");
        try {
            System.in.read();
        } catch (IOException e) {
        }
    }

}
