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
import java.util.logging.Level;

import com.bloomberglp.blpapi.Datetime;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.InvalidRequestException;
import com.bloomberglp.blpapi.Logging;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;

public class RefDataExample
{
    private static final Name SECURITY_DATA = new Name("securityData");
    private static final Name SECURITY = new Name("security");
    private static final Name FIELD_DATA = new Name("fieldData");
    private static final Name RESPONSE_ERROR = new Name("responseError");
    private static final Name SECURITY_ERROR = new Name("securityError");
    private static final Name FIELD_EXCEPTIONS = new Name("fieldExceptions");
    private static final Name FIELD_ID = new Name("fieldId");
    private static final Name ERROR_INFO = new Name("errorInfo");
    private static final Name CATEGORY = new Name("category");
    private static final Name MESSAGE = new Name("message");

    private String            d_host;
    private int               d_port;
    private ArrayList<String> d_securities;
    private ArrayList<String> d_fields;

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception
    {
        System.out.println("Reference Data Example");
        RefDataExample example = new RefDataExample();
        example.run(args);

        System.out.println("Press ENTER to quit");
        System.in.read();
    }

    public RefDataExample()
    {
        d_host = "localhost";
        d_port = 8194;
        d_securities = new ArrayList<String>();
        d_fields = new ArrayList<String>();
    }

    private void run(String[] args) throws Exception
    {
        if (!parseCommandLine(args)) return;

        SessionOptions sessionOptions = new SessionOptions();
        sessionOptions.setServerHost(d_host);
        sessionOptions.setServerPort(d_port);

        System.out.println("Connecting to " + d_host + ":" + d_port);
        Session session = new Session(sessionOptions);
        if (!session.start()) {
            System.err.println("Failed to start session.");
            return;
        }
        if (!session.openService("//blp/refdata")) {
            System.err.println("Failed to open //blp/refdata");
            return;
        }

        try {
            sendRefDataRequest(session);
        } catch (InvalidRequestException e) {
            e.printStackTrace();
        }

        // wait for events from session.
        eventLoop(session);

        session.stop();
    }

    private void eventLoop(Session session) throws Exception
    {
        boolean done = false;
        while (!done) {
            Event event = session.nextEvent();
            if (event.eventType() == Event.EventType.PARTIAL_RESPONSE) {
                System.out.println("Processing Partial Response");
                processResponseEvent(event);
            }
            else if (event.eventType() == Event.EventType.RESPONSE) {
                System.out.println("Processing Response");
                processResponseEvent(event);
                done = true;
            } else {
                MessageIterator msgIter = event.messageIterator();
                while (msgIter.hasNext()) {
                    Message msg = msgIter.next();
                    System.out.println(msg.asElement());
                    if (event.eventType() == Event.EventType.SESSION_STATUS) {
                        if (msg.messageType().equals("SessionTerminated") ||
                            msg.messageType().equals("SessionStartupFailure")) {
                            done = true;
                        }
                    }
                }
            }
        }
    }

    // return true if processing is completed, false otherwise
    private void processResponseEvent(Event event)
    throws Exception
    {
        MessageIterator msgIter = event.messageIterator();
        while (msgIter.hasNext()) {
            Message msg = msgIter.next();
            if (msg.hasElement(RESPONSE_ERROR)) {
                printErrorInfo("REQUEST FAILED: ", msg.getElement(RESPONSE_ERROR));
                continue;
            }

            Element securities = msg.getElement(SECURITY_DATA);
            int numSecurities = securities.numValues();
            System.out.println("Processing " + numSecurities + " securities:");
            for (int i = 0; i < numSecurities; ++i) {
                Element security = securities.getValueAsElement(i);
                String ticker = security.getElementAsString(SECURITY);
                System.out.println("\nTicker: " + ticker);
                if (security.hasElement("securityError")) {
                    printErrorInfo("\tSECURITY FAILED: ",
                                   security.getElement(SECURITY_ERROR));
                    continue;
                }

                if (security.hasElement(FIELD_DATA)) {
                    Element fields = security.getElement(FIELD_DATA);
                    if (fields.numElements() > 0) {
                        System.out.println("FIELD\t\tVALUE");
                        System.out.println("-----\t\t-----");
                        int numElements = fields.numElements();
                        for (int j = 0; j < numElements; ++j) {
                            Element field = fields.getElement(j);
                            System.out.println(field.name() + "\t\t" +
                                               field.getValueAsString());
                        }
                    }
                }
                System.out.println("");
                Element fieldExceptions = security.getElement(FIELD_EXCEPTIONS);
                if (fieldExceptions.numValues() > 0) {
                    System.out.println("FIELD\t\tEXCEPTION");
                    System.out.println("-----\t\t---------");
                    for (int k = 0; k < fieldExceptions.numValues(); ++k) {
                        Element fieldException =
                            fieldExceptions.getValueAsElement(k);
                        printErrorInfo(fieldException.getElementAsString(FIELD_ID) +
                                "\t\t", fieldException.getElement(ERROR_INFO));
                    }
                }
            }
        }
    }

    private void sendRefDataRequest(Session session) throws Exception
    {
        Service refDataService = session.getService("//blp/refdata");
        Request request = refDataService.createRequest("ReferenceDataRequest");

        // Add securities to request
        Element securities = request.getElement("securities");

        for (String security : d_securities) {
            securities.appendValue(security);
        }

        // Add fields to request
        Element fields = request.getElement("fields");
        for (String field : d_fields) {
            fields.appendValue(field);
        }

        System.out.println("Sending Request: " + request);
        session.sendRequest(request, null);
    }

    private void registerCallback(int verbosity)
    {
        Logging.Callback loggingCallback = new Logging.Callback() {
            public void onMessage(long threadId, Level level, Datetime dateTime,
                                  String loggerName, String message) {
                System.out.println(dateTime + "  " + loggerName + " [" + level.toString() + "] Thread ID = "
                                   + threadId + " " + message);
                }
        };

        Level logLevel = Level.OFF;
        if (verbosity > 0) {
            switch (verbosity) {
            case 1:
                logLevel = Level.INFO;
                break;
            case 2:
                logLevel = Level.FINE;
                break;
            case 3:
                logLevel = Level.FINER;
                break;
            default:
                logLevel = Level.FINEST;
                break;
            }
        }
        Logging.registerCallback(loggingCallback, logLevel);
    }

    private boolean parseCommandLine(String[] args)
    {
        int verbosity = 0;
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equalsIgnoreCase("-s")) {
                d_securities.add(args[i+1]);
            }
            else if (args[i].equalsIgnoreCase("-f")) {
                d_fields.add(args[i+1]);
            }
            else if (args[i].equalsIgnoreCase("-ip")) {
                d_host = args[i+1];
            }
            else if (args[i].equalsIgnoreCase("-p")) {
                d_port = Integer.parseInt(args[i+1]);
            }
            else if (args[i].equalsIgnoreCase("-v")) {
                ++verbosity;
            }
            else if (args[i].equalsIgnoreCase("-h")) {
                printUsage();
                return false;
            }
        }

        if (verbosity > 0) {
            registerCallback(verbosity);
        }
        // handle default arguments
        if (d_securities.isEmpty()) {
            d_securities.add("IBM US Equity");
        }

        if (d_fields.isEmpty()) {
            d_fields.add("PX_LAST");
        }

        return true;
    }

    private void printErrorInfo(String leadingStr, Element errorInfo)
    throws Exception
    {
        System.out.println(leadingStr + errorInfo.getElementAsString(CATEGORY) +
                           " (" + errorInfo.getElementAsString(MESSAGE) + ")");
    }

    private void printUsage()
    {
        System.out.println("Usage:");
        System.out.println("\tRetrieve reference data ");
        System.out.println("\t\t[-s \t<security = IBM US Equity>");
        System.out.println("\t\t[-f \t<field = PX_LAST>");
        System.out.println("\t\t[-ip\t<ipAddress = localhost>");
        System.out.println("\t\t[-p \t<tcpPort = 8194>");
        System.out.println("\t\t[-v \tVerbose");
    }
}
