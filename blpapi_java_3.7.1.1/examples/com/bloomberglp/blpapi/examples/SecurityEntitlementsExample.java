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

import com.bloomberglp.blpapi.CorrelationID;

import com.bloomberglp.blpapi.Event;

import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.EventQueue;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;

public class SecurityEntitlementsExample
{
    private static final String API_AUTH_SVC_NAME = "//blp/apiauth";
    private static final Name SECURITYENTITLEMENTS_RESPONSE = Name.getName(
       "SecurityEntitlementsResponse");
    private static final Name RESPONSE_ERROR = Name.getName("ResponseError");
    private String d_serverHost;
    private int d_serverPort;
    private ArrayList<String> d_securities;

    public SecurityEntitlementsExample()
    {
        d_serverHost = "localhost";
        d_serverPort = 8194;
        d_securities = new ArrayList<String>();
    }

    public static void main(String[] args) throws java.lang.Exception
    {
        SecurityEntitlementsExample example = new SecurityEntitlementsExample();
        example.run(args);
        System.out.println("Press ENTER to quit");
        System.in.read();
    }
    private void run(String[] args) throws Exception
    {
        if (!parseCommandLine(args))
        {
            printUsage();
            System.exit(-1);
        }
        SessionOptions sessionOptions = new SessionOptions();
        sessionOptions.setServerHost(d_serverHost);
        sessionOptions.setServerPort(d_serverPort);

        System.out.println("Connecting to " + d_serverHost + ":" + d_serverPort);
        Session session = new Session(sessionOptions);
        if (!session.start())
        {
            System.err.println("Failed to start session.");
            return;
        }
        if (!session.openService(API_AUTH_SVC_NAME))
        {
            System.out.println("Failed to open service: " + API_AUTH_SVC_NAME);
            System.exit(-1);
        }
        Service apiAuthSvc = session.getService(API_AUTH_SVC_NAME);
        EventQueue eventQueue = new EventQueue();
        CorrelationID correlator = new CorrelationID(10);
        Request request = apiAuthSvc.createRequest("SecurityEntitlementsRequest");
        Element securities = request.getElement("securities");
        for (String security : d_securities) {
            securities.appendValue(security);
        }

        Name SEQ_NUM = Name.getName("sequenceNumber");
        session.sendRequest(request, eventQueue, correlator);
        boolean notdone = true;
        while (notdone)
        {
            Event event = eventQueue.nextEvent(5000 * 60);
            if (event.eventType() == Event.EventType.TIMEOUT)
            {
                printEvent(event);
                notdone = false;
            }
            else if (event.eventType() == Event.EventType.RESPONSE ||
                        event.eventType() == Event.EventType.PARTIAL_RESPONSE ||
                        event.eventType() == Event.EventType.REQUEST_STATUS)
            {
                MessageIterator msgIter = event.messageIterator();
                if (msgIter.hasNext())
                {
                    Message msg = msgIter.next();
                    if (msg.messageType().equals(RESPONSE_ERROR))
                    {
                        printEvent(event);
                        notdone = false;
                    }
                    else if (msg.messageType().equals(SECURITYENTITLEMENTS_RESPONSE))
                    {
                        Element eidData = msg.getElement("eidData");
                        if (eidData.numValues() == 0)
                        {
                            printEvent(event);
                        }
                        else
                        {
                            for (int i = 0; i < eidData.numValues(); ++i)
                            {
                                Element item = eidData.getValueAsElement(i);
                                int secoffset = item.getElementAsInt32(SEQ_NUM);
                                int status = item.getElementAsInt32("status");
                                System.out.print(d_securities.get(secoffset) + "\t:\t");
                                if (0 == status)
                                {
                                    Element eids = item.getElement("eids");
                                    for (int j = 0; j < eids.numValues(); ++j)
                                    {
                                        System.out.print(eids.getValueAsInt32(j) + " ");
                                    }
                                }
                                else
                                {
                                    System.out.print("Failed (" + status + ")");
                                }
                                System.out.println();
                            }
                        }
                        if (event.eventType() == Event.EventType.RESPONSE)
                        {
                            notdone = false;
                        }
                    }
                }
            }
        }
        session.cancel(correlator);
    }

    private boolean parseCommandLine(String[] args) throws Exception
    {
        for (int i = 0; i < args.length; ++i)
        {
            if (args[i].equalsIgnoreCase("-s"))
            {
                d_securities.add(args[i + 1]);
            }
            else if (args[i].equalsIgnoreCase("-ip"))
            {
                d_serverHost = args[i + 1];
            }
            else if (args[i].equalsIgnoreCase("-p"))
            {
                d_serverPort = Integer.parseInt(args[i + 1]);
            }
            else if (args[i].equalsIgnoreCase("-h"))
            {
                return false;
            }
        }
        if (d_securities.size() <= 0)
            d_securities.add("IBM US Equity");
        return true;
    }
    private void printEvent(Event event) throws Exception
    {
        MessageIterator msgIter = event.messageIterator();
        while (msgIter.hasNext())
        {
            Message msg = msgIter.next();

            CorrelationID correlationId = msg.correlationID();
            if (correlationId != null)
            {
                System.out.println("Correlator: " + correlationId);
            }

            Service service = msg.service();
            if (service != null)
            {
                System.out.println("Service: " + service.name());
            }
            System.out.println(msg);
        }
    }
    private void printUsage()
    {
        System.out.println("Usage:");
        System.out.println("    SecurityEntitlements Example");
        System.out.println("        [-ip <ipAddress = localhost>   ]");
        System.out.println("        [-p  <tcpPort   = 8194>        ]");
        System.out.println("        [-s  <security = IBM US Equity>]");

        System.out.println("Press ENTER to quit");
    }
}
