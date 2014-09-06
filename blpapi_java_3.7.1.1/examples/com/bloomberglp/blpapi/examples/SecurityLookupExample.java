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
import com.bloomberglp.blpapi.EventQueue;
import com.bloomberglp.blpapi.Identity;
import com.bloomberglp.blpapi.InvalidConversionException;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.NotFoundException;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;
import java.util.HashMap;
import java.util.Map.Entry;

public class SecurityLookupExample
{
    private static final Name AUTHORIZATION_SUCCESS = Name.getName("AuthorizationSuccess");
    private static final Name TOKEN_SUCCESS = Name.getName("TokenGenerationSuccess");
    private static final Name SESSION_TERMINATED = Name.getName("SessionTerminated");
    private static final Name SESSION_FAILURE = Name.getName("SessionStartupFailure");
    private static final Name TOKEN_ELEMENT = Name.getName("token");
    private static final Name DESCRIPTION_ELEMENT = Name.getName("description");
    private static final Name QUERY_ELEMENT = Name.getName("query");
    private static final Name RESULTS_ELEMENT = Name.getName("results");
    private static final Name MAX_RESULTS_ELEMENT = Name.getName("maxResults");

    private static final Name SECURITY_ELEMENT = Name.getName("security");

    private static final Name ERROR_RESPONSE = Name.getName("ErrorResponse");
    private static final Name INSTRUMENT_LIST_RESPONSE = Name.getName("InstrumentListResponse");
    private static final Name CURVE_LIST_RESPONSE = Name.getName("CurveListResponse");
    private static final Name GOVT_LIST_RESPONSE = Name.getName("GovtListResponse");

    private static final Name INSTRUMENT_LIST_REQUEST = Name.getName("instrumentListRequest");

    private static final String AUTH_USER = "AuthenticationType=OS_LOGON";
    private static final String AUTH_APP_PREFIX =
            "AuthenticationMode=APPLICATION_ONLY;ApplicationAuthenticationType=APPNAME_AND_KEY;"
            + "ApplicationName=";
    private static final String AUTH_USER_APP_PREFIX =
            "AuthenticationMode=USER_AND_APPLICATION;AuthenticationType=OS_LOGON;"
            + "ApplicationAuthenticationType=APPNAME_AND_KEY;ApplicationName=";
    private static final String AUTH_DIR_PREFIX =
            "AuthenticationType=DIRECTORY_SERVICE;DirSvcPropertyName=";
    private static final String AUTH_OPTION_NONE = "none";
    private static final String AUTH_OPTION_USER = "user";
    private static final String AUTH_OPTION_APP = "app=";
    private static final String AUTH_OPTION_USER_APP = "userapp=";
    private static final String AUTH_OPTION_DIR = "dir=";
    private static final String INSTRUMENT_SERVICE = "//blp/instruments";
    private static final String AUTH_SERVICE = "//blp/apiauth";
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 8194;
    private static final String DEFAULT_QUERY_STRING = "IBM";
    private static final int DEFAULT_MAX_RESULTS = 10;
    private static final int WAIT_TIME_MS = 10 * 1000; // 10 seconds

    private static final String[] FILTERS_INSTRUMENTS = {
        "yellowKeyFilter",
        "languageOverride"
    };

    private static final String FILTERS_GOVT[] = {
        "ticker",
        "partialMatch"
    };

    private static final String FILTERS_CURVE[] = {
        "countryCode",
        "currencyCode",
        "type",
        "subtype",
        "curveid",
        "bbgid"
    };

    private static final Name CURVE_ELEMENT = Name.getName("curve");
    private static final Name[] CURVE_RESPONSE_ELEMENTS = {
        Name.getName("country"),
        Name.getName("currency"),
        Name.getName("curveid"),
        Name.getName("type"),
        Name.getName("subtype"),
        Name.getName("publisher"),
        Name.getName("bbgid")
    };

    private static final Name PARSEKY_ELEMENT = Name.getName("parseky");
    private static final Name NAME_ELEMENT = Name.getName("name");
    private static final Name TICKER_ELEMENT = Name.getName("ticker");

    private String d_queryString = DEFAULT_QUERY_STRING;
    private String d_host = DEFAULT_HOST;
    private Name d_requestType = INSTRUMENT_LIST_REQUEST;
    private int d_port = DEFAULT_PORT;
    private int d_maxResults = DEFAULT_MAX_RESULTS;
    private String d_authOptions = null;
    private HashMap<String, String> d_filters = new HashMap<String, String>();

    private void printUsage() {
        System.out.println("Usage:");
        System.out.println(" Instruments Lookup service Example.");
        System.out.println("\t\t[-r \t<requestType> = instrumentListRequest]" +
                "\trequestType: instrumentListRequest|curveListRequest|govtListRequest");
        System.out.println("\t\t[-ip\t<ipAddress = localhost>");
        System.out.println("\t\t[-p \t<tcpPort = 8194>");
        System.out.println("\t\t[-s \t<Query string = IBM>");
        System.out.println("\t\t[-m \t<Max Results = 10>");
        System.out.println(
                "\t\t[-auth <option>]\tauthentication option: "
                + "user|none|app=<app>|userapp=<app>|dir=<property> (default: none)");
        System.out.println(
                "\t\t[-f <filter=value>]\tFollowing are the filters for each request: ");

        System.out.print("\t\t\tinstrumentListRequest:");
        printFilters(FILTERS_INSTRUMENTS);
        System.out.print("\t\t\tgovtListRequest:");
        printFilters(FILTERS_GOVT);
        System.out.print("\t\t\tcurveListRequest:");
        printFilters(FILTERS_CURVE);
    }

    private void printFilters(String [] filters) {
        System.out.print("\t");
        for (int i = 0; i < filters.length - 1; i++) {
            System.out.print(filters[i] + "|");
        }
        System.out.print(filters[filters.length - 1] + " (default: none)\n");
    }

    private void parseCommandLine(String[] args) throws Exception {
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equalsIgnoreCase("-r") && i + 1 < args.length) {
                // The fist argument is always the request type
                d_requestType = Name.getName(args[++i]);
            }
            else if (args[i].equalsIgnoreCase("-ip") && i + 1 < args.length) {
                d_host = args[++i];
            }
            else if (args[i].equalsIgnoreCase("-p") && i + 1 < args.length) {
                d_port = Integer.parseInt(args[++i]);
            }
            else if (args[i].equalsIgnoreCase("-s") && i + 1 < args.length) {
                d_queryString = args[++i];
            }
            else if (args[i].equalsIgnoreCase("-m") && i + 1 < args.length) {
                d_maxResults = Integer.parseInt(args[++i]);
            }
            else if (args[i].equalsIgnoreCase("-f") && i + 1 < args.length) {
                String[] tokens = args[++i].split("=");
                if (tokens.length == 2) {
                    d_filters.put(tokens[0].trim(), tokens[1].trim());
                }
            }
            else if (args[i].equalsIgnoreCase("-auth") && i + 1 < args.length) {
                ++i;
                if (args[i].equalsIgnoreCase(AUTH_OPTION_NONE)) {
                    d_authOptions = null;
                }
                else if (args[i].equalsIgnoreCase(AUTH_OPTION_USER)) {
                    d_authOptions = AUTH_USER;
                }
                else if (args[i].regionMatches(
                        true,
                        0,
                        AUTH_OPTION_APP,
                        0,
                        AUTH_OPTION_APP.length())) {
                    d_authOptions = AUTH_APP_PREFIX
                            + args[i].substring(AUTH_OPTION_APP.length());
                }
                else if (args[i].regionMatches(
                        true,
                        0,
                        AUTH_OPTION_DIR,
                        0,
                        AUTH_OPTION_DIR.length())) {
                    d_authOptions = AUTH_DIR_PREFIX
                            + args[i].substring(AUTH_OPTION_DIR.length());
                }
                else if (args[i].regionMatches(
                        true,
                        0,
                        AUTH_OPTION_USER_APP,
                        0,
                        AUTH_OPTION_USER_APP.length())) {
                    d_authOptions = AUTH_USER_APP_PREFIX
                            + args[i].substring(AUTH_OPTION_USER_APP.length());
                }
                else {
                    throw new Exception(String.format("Invalid -auth option: %1$s", args[i]));
                }
            }
            else {
                throw new Exception(String.format("Unknown option: %1$s", args[i]));
            }
        }
    }

    // Authorize should be called before any requests are sent.
    public static void authorize(Identity identity, Session session) throws Exception {
        if (!session.openService(AUTH_SERVICE)) {
            throw new Exception(
                    String.format("Failed to open auth service: %1$s",
                                  AUTH_SERVICE));
        }
        Service authService = session.getService(AUTH_SERVICE);

        EventQueue tokenEventQueue = new EventQueue();
        session.generateToken(new CorrelationID(tokenEventQueue), tokenEventQueue);
        String token = null;
        // Generate token responses will come on the dedicated queue. There would be no other
        // messages on that queue.
        Event event = tokenEventQueue.nextEvent(WAIT_TIME_MS);

        if (event.eventType() == Event.EventType.TOKEN_STATUS
                || event.eventType() == Event.EventType.REQUEST_STATUS) {
            for (Message msg: event) {
                System.out.println(msg);
                if (msg.messageType() == TOKEN_SUCCESS) {
                    token = msg.getElementAsString(TOKEN_ELEMENT);
                }
            }
        }
        if (token == null) {
            throw new Exception("Failed to get token");
        }

        Request authRequest = authService.createAuthorizationRequest();
        authRequest.set(TOKEN_ELEMENT, token);

        session.sendAuthorizationRequest(authRequest, identity, null);

        long waitDuration = WAIT_TIME_MS;
        for (long startTime = System.currentTimeMillis();
                waitDuration > 0;
                waitDuration -= (System.currentTimeMillis() - startTime)) {
            event = session.nextEvent(waitDuration);
            // Since no other requests were sent using the session queue, the response can
            // only be for the Authorization request
            if (event.eventType() != Event.EventType.RESPONSE
                    && event.eventType() != Event.EventType.PARTIAL_RESPONSE
                    && event.eventType() != Event.EventType.REQUEST_STATUS) {
                continue;
            }

            for (Message msg: event) {
                System.out.println(msg);
                if (msg.messageType() != AUTHORIZATION_SUCCESS) {
                    throw new Exception("Authorization Failed");
                }
            }
            return;
        }
        throw new Exception("Authorization Failed");
    }

    private void processInstrumentListResponse(Message msg) {
        Element results = msg.getElement(RESULTS_ELEMENT);
        int numResults = results.numValues();
        System.out.println("Processing " + numResults + " results:");
        for (int i = 0; i < numResults; ++i) {
            Element result = results.getValueAsElement(i);
            System.out.printf(
                    "\t%1$d %2$s - %3$s\n",
                    i + 1,
                    result.getElementAsString(SECURITY_ELEMENT),
                    result.getElementAsString(DESCRIPTION_ELEMENT));
        }
    }

    private void processCurveListResponse(Message msg) {
        Element results = msg.getElement(RESULTS_ELEMENT);
        int numResults = results.numValues();
        System.out.println("Processing " + numResults + " results:");
        for (int i = 0; i < numResults; ++i) {
            Element result = results.getValueAsElement(i);
            StringBuilder sb = new StringBuilder();
            for (Name n: CURVE_RESPONSE_ELEMENTS) {
                if (sb.length() != 0) {
                    sb.append(" ");
                }
                sb.append(n).append("=").append(result.getElementAsString(n));
            }
            System.out.printf(
                    "\t%1$d %2$s - %3$s '%4$s'\n",
                    i + 1,
                    result.getElementAsString(CURVE_ELEMENT),
                    result.getElementAsString(DESCRIPTION_ELEMENT),
                    sb.toString());
        }
    }

    private void processGovtListResponse(Message msg) {
        Element results = msg.getElement(RESULTS_ELEMENT);
        int numResults = results.numValues();
        System.out.println("Processing " + numResults + " results:");
        for (int i = 0; i < numResults; ++i) {
            Element result = results.getValueAsElement(i);
            System.out.printf(
                    "\t%1$d %2$s, %3$s - %4$s\n",
                    i + 1,
                    result.getElementAsString(PARSEKY_ELEMENT),
                    result.getElementAsString(NAME_ELEMENT),
                    result.getElementAsString(TICKER_ELEMENT));
        }
    }

    private void processResponseEvent(Event event) {
        MessageIterator msgIter = event.messageIterator();
        while (msgIter.hasNext()) {
            Message msg = msgIter.next();
            if (msg.messageType() == ERROR_RESPONSE) {
                String description = msg.getElementAsString(DESCRIPTION_ELEMENT);
                System.out.println("Received error: " + description);
            }
            else if (msg.messageType() == INSTRUMENT_LIST_RESPONSE) {
                processInstrumentListResponse(msg);
            }
            else if (msg.messageType() == CURVE_LIST_RESPONSE) {
                processCurveListResponse(msg);
            }
            else if (msg.messageType() == GOVT_LIST_RESPONSE) {
                processGovtListResponse(msg);
            }
            else {
                System.err.println("Unknown MessageType received");
            }
        }
    }

    private void eventLoop(Session session) throws InterruptedException {
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
            }
            else {
                MessageIterator msgIter = event.messageIterator();
                while (msgIter.hasNext()) {
                    Message msg = msgIter.next();
                    System.out.println(msg.asElement());
                    if (event.eventType() == Event.EventType.SESSION_STATUS) {
                        if (msg.messageType() == SESSION_TERMINATED
                                || msg.messageType() == SESSION_FAILURE) {
                            done = true;
                        }
                    }
                }
            }
        }
    }

    private void sendRequest(Session session, Identity identity) throws Exception {
        System.out.println("Sending Request: " + d_requestType.toString());
        Service instrumentService = session.getService(INSTRUMENT_SERVICE);
        Request request;
        try {
            request = instrumentService.createRequest(d_requestType.toString());
        }
        catch (NotFoundException e) {
            throw new Exception(
                    String.format("Request type not found: %1$s", d_requestType),
                    e);
        }
        request.set(QUERY_ELEMENT, d_queryString);
        request.set(MAX_RESULTS_ELEMENT, d_maxResults);

        for (Entry<String, String> entry: d_filters.entrySet()) {
            try {
                request.set(entry.getKey(), entry.getValue());
            }
            catch (NotFoundException e) {
                throw new Exception(String.format("Filter not found: %1$s", entry.getKey()), e);
            }
            catch (InvalidConversionException e) {
                throw new Exception(
                        String.format(
                        "Invalid value: %1$s for filter: %2$s",
                        entry.getValue(),
                        entry.getKey()),
                        e);
            }
        }

        System.out.println(request);
        session.sendRequest(request, identity, null);
    }

    private static void stopSession(Session session) {
        if (session != null) {
            boolean done = false;
            while (!done) {
                try {
                    session.stop();
                    done = true;
                }
                catch (InterruptedException e) {
                    System.out.println("InterrupedException caught (ignoring)");
                }
            }
        }
    }

    private void run(String[] args) {
        Session session = null;
        try {
            parseCommandLine(args);
            SessionOptions sessionOptions = new SessionOptions();
            sessionOptions.setServerHost(d_host);
            sessionOptions.setServerPort(d_port);
            sessionOptions.setAuthenticationOptions(d_authOptions);

            System.out.println("Connecting to " + d_host + ":" + d_port);
            session = new Session(sessionOptions);
            if (!session.start()) {
                System.err.println("Failed to start session.");
                return;
            }

            Identity identity = session.createIdentity();
            if (d_authOptions != null) {
                authorize(identity, session);
            }

            if (!session.openService(INSTRUMENT_SERVICE)) {
                System.err.println("Failed to open " + INSTRUMENT_SERVICE);
                return;
            }

            sendRequest(session, identity);
            eventLoop(session);
        }
        catch (Exception e) {
            System.err.printf("Exception: %1$s\n", e.getMessage());
            System.err.println();
            printUsage();
        }
        finally {
            if (session != null) {
                stopSession(session);
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("SecurityLookupExample");
        SecurityLookupExample example = new SecurityLookupExample();
        example.run(args);
    }
}
