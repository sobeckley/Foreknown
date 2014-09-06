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

import com.bloomberglp.blpapi.CorrelationID;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;

/**
 * An example to demonstrate use of CorrelationID.
 */
public class CorrelationExample {
    private Session             d_session;
    private SessionOptions      d_sessionOptions;
    private Service             d_refDataService;
    private Window              d_secInfoWindow;

    /**
     * A helper class to simulate a GUI window.
     */
    public class Window {
        private String  d_name;

        public Window(String name) {
            d_name = name;
        }

        public void displaySecurityInfo(Message msg) {
            System.out.println(d_name + ": " + msg);
        }
    }

    public static void main(String[] args) {
        System.out.println("CorrelationExample");
        CorrelationExample example = new CorrelationExample();
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

    public CorrelationExample() {
        d_sessionOptions = new SessionOptions();
        d_sessionOptions.setServerHost("localhost");
        d_sessionOptions.setServerPort(8194);
        d_secInfoWindow = new Window("SecurityInfo");
    }

    private void run(String[] args) throws Exception {
        if (!createSession()) return;

        Request request = d_refDataService.createRequest("ReferenceDataRequest");
        request.getElement("securities").appendValue("IBM US Equity");
        request.getElement("fields").appendValue("PX_LAST");
        request.getElement("fields").appendValue("DS002");

        d_session.sendRequest(request, new CorrelationID(d_secInfoWindow));

        while (true) {
            Event event = d_session.nextEvent();
            MessageIterator msgIter = event.messageIterator();
            while (msgIter.hasNext()) {
                Message msg = msgIter.next();
                if (event.eventType() == Event.EventType.RESPONSE ||
                    event.eventType() == Event.EventType.PARTIAL_RESPONSE) {
                    ((Window)(msg.correlationID().object())).displaySecurityInfo(msg);
                }
            }
            if (event.eventType() == Event.EventType.RESPONSE) {
                // received final response
                break;
            }
        }
    }

    private boolean createSession() throws Exception {
        System.out.println("Connecting to " + d_sessionOptions.getServerHost()
                            + ":" + d_sessionOptions.getServerPort());
        d_session = new Session(d_sessionOptions);
        if (!d_session.start()) {
            System.err.println("Failed to connect!");
            return false;
        }
        if (!d_session.openService("//blp/refdata")) {
            System.err.println("Failed to open //blp/refdata");
            d_session.stop();
            d_session = null;
            return false;
        }
        d_refDataService = d_session.getService("//blp/refdata");
        return true;
    }
}
