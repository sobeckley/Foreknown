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
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;

public class SimpleRefDataExample {
    private static final Name SECURITY_DATA = new Name("securityData");
    private static final Name SECURITY = new Name("security");
    private static final Name FIELD_DATA = new Name("fieldData");
    private static final Name FIELD_EXCEPTIONS = new Name("fieldExceptions");
    private static final Name FIELD_ID = new Name("fieldId");
    private static final Name ERROR_INFO = new Name("errorInfo");

    private CorrelationID d_cid;

    public static void main(String[] args) throws Exception {
        SimpleRefDataExample example = new SimpleRefDataExample();
        example.run(args);
        System.out.println("Press ENTER to quit");
        System.in.read();
    }

    private void run(String[] args) throws Exception {
        String serverHost = "localhost";
        int serverPort = 8194;

        SessionOptions sessionOptions = new SessionOptions();
        sessionOptions.setServerHost(serverHost);
        sessionOptions.setServerPort(serverPort);

        Session session = new Session(sessionOptions);

        System.out.println("Connecting to " + serverHost + ":" + serverPort);
        if (!session.start()) {
            System.err.println("Failed to start session.");
            return;
        }
        System.out.println("Connected successfully.");

        if (!session.openService("//blp/refdata")) {
            System.err.println("Failed to open //blp/refdata");
            return;
        }

        Service refDataService = session.getService("//blp/refdata");
        Request request = refDataService.createRequest("ReferenceDataRequest");

        // append securities to request
        Element securities = request.getElement("securities");
        securities.appendValue("IBM US Equity");
        securities.appendValue("/cusip/912828GM6@BGN");

        // append fields to request
        Element fields = request.getElement("fields");
        fields.appendValue("PX_LAST");
        fields.appendValue("DS002");

        System.out.println("Sending Request: " + request);
        d_cid = session.sendRequest(request, null);

        while (true) {
            Event event = session.nextEvent();
            MessageIterator msgIter = event.messageIterator();
            while (msgIter.hasNext()) {
                Message msg = msgIter.next();
                if (msg.correlationID() == d_cid) {
                    processMessage(msg);
                }
            }
            if (event.eventType() == Event.EventType.RESPONSE) {
                break;
            }
        }
    }

    private void processMessage(Message msg) throws Exception {
        Element securityDataArray = msg.getElement(SECURITY_DATA);
        int numSecurities = securityDataArray.numValues();
        for (int i = 0; i < numSecurities; ++i) {
            Element securityData = securityDataArray.getValueAsElement(i);
            System.out.println(securityData.getElementAsString(SECURITY));
            Element fieldData = securityData.getElement(FIELD_DATA);
            for (int j = 0; j < fieldData.numElements(); ++j) {
                Element field = fieldData.getElement(j);
                if (field.isNull()) {
                    System.out.println(field.name() + " is NULL.");
                } else {
                    System.out.println(field);
                }
            }
            Element fieldExceptionArray =
                securityData.getElement(FIELD_EXCEPTIONS);
            for (int k = 0; k < fieldExceptionArray.numValues(); ++k) {
                Element fieldException =
                    fieldExceptionArray.getValueAsElement(k);
                System.out.println(
                        fieldException.getElement(ERROR_INFO).getElementAsString("category")
                        + ": " + fieldException.getElementAsString(FIELD_ID));
            }
            System.out.println("\n");
        }
    }
}
