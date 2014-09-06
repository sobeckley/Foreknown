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

import com.bloomberglp.blpapi.Datetime;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;
import java.util.Calendar;

public class SimpleIntradayBarExample {

    public static void main(String[] args) throws Exception
    {
        SimpleIntradayBarExample example = new SimpleIntradayBarExample();
        example.run(args);
        System.out.println("Press ENTER to quit");
        System.in.read();
    }

    private Calendar getPreviousTradingDate()
    {
    Calendar rightNow = Calendar.getInstance();
    rightNow.roll(Calendar.DAY_OF_MONTH, -1);
    if (rightNow.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) {
        rightNow.roll(Calendar.DAY_OF_MONTH, -2);
    }
    else if (rightNow.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY) {
        rightNow.roll(Calendar.DAY_OF_MONTH, -1);
    }
    return rightNow;
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
        if (!session.openService("//blp/refdata")) {
            System.err.println("Failed to open //blp/refdata");
            return;
        }
        Service refDataService = session.getService("//blp/refdata");
        Request request = refDataService.createRequest("IntradayBarRequest");
        request.set("security", "IBM US Equity");
        request.set("eventType", "TRADE");
        request.set("interval", 60);    // bar interval in minutes
    Calendar tradedOn = getPreviousTradingDate() ;
        request.set("startDateTime", new Datetime(tradedOn.get(Calendar.YEAR),
                          tradedOn.get(Calendar.MONTH) + 1,
                          tradedOn.get(Calendar.DAY_OF_MONTH),
                          13, 30, 0, 0));
    request.set("endDateTime", new Datetime(tradedOn.get(Calendar.YEAR),
                        tradedOn.get(Calendar.MONTH) + 1,
                        tradedOn.get(Calendar.DAY_OF_MONTH),
                        21, 30, 0, 0));
        System.out.println("Sending Request: " + request);
        session.sendRequest(request, null);

        while (true) {
            Event event = session.nextEvent();
            MessageIterator msgIter = event.messageIterator();
            while (msgIter.hasNext()) {
                Message msg = msgIter.next();
                System.out.println(msg);
            }
            if (event.eventType() == Event.EventType.RESPONSE) {
                break;
            }
        }
    }
}
