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
import com.bloomberglp.blpapi.Identity;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Name;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;

public class GenerateTokenExample {
    private static final Name AUTHORIZATION_SUCCESS = Name
            .getName("AuthorizationSuccess");
    private static final Name AUTHORIZATION_FAILURE = Name
            .getName("AuthorizationFailure");
    private static final Name TOKEN_SUCCESS = Name
            .getName("TokenGenerationSuccess");
    private static final Name TOKEN_FAILURE = Name
            .getName("TokenGenerationFailure");

    private static final String AUTH_USER        = "AuthenticationType=OS_LOGON";
    private static final String AUTH_APP_PREFIX  = "AuthenticationMode=APPLICATION_ONLY;ApplicationAuthenticationType=APPNAME_AND_KEY;ApplicationName=";
    private static final String AUTH_DIR_PREFIX  = "AuthenticationType=DIRECTORY_SERVICE;DirSvcPropertyName=";
    private static final String AUTH_OPTION_NONE = "none";
    private static final String AUTH_OPTION_USER = "user";
    private static final String AUTH_OPTION_APP  = "app=";
    private static final String AUTH_OPTION_DIR  = "dir=";

    private String d_authOptions = AUTH_USER;

    private String d_serverHost;
    private int d_serverPort;

    private Session d_session;
    private Identity d_userIdentity;

    public GenerateTokenExample() {
        d_serverHost = "localhost";
        d_serverPort = 8194;
        d_session = null;
    }

    private void printUsage() {
        System.out.println("Generate a token for authorization. Usage: ");
        System.out.println("\t[-ip<ipAddress      = localhost>");
        System.out.println("\t[-p <tcpPort        = 8194>");
        System.out.println("\t[-auth <option>]     \tauthentication option: user|none|app=<app>|dir=<property> (default: user)");
    }

    public static void main(String[] args) throws java.lang.Exception {
        System.out.println("GenerateToken");
        GenerateTokenExample example = new GenerateTokenExample();
        example.run(args);
        System.out.println("Press ENTER to quit");
        System.in.read();
    }

    private void run(String[] args) throws Exception {
        if (!parseCommandLine(args))
            return;

        SessionOptions sessionOptions = new SessionOptions();
        sessionOptions.setServerHost(d_serverHost);
        sessionOptions.setServerPort(d_serverPort);

        sessionOptions.setAuthenticationOptions(d_authOptions);
        System.out.println("Authentication Options = " + sessionOptions.authenticationOptions());
        System.out.println("Connecting to " + d_serverHost + ":" + d_serverPort);
        d_session = new Session(sessionOptions);
        if (!d_session.start()) {
            System.err.println("Failed to start session.");
            return;
        }

        if (!d_session.openService("//blp/refdata")) {
            System.err.println("Failed to open //blp/refdata");
            return;
        }
        if (!d_session.openService("//blp/apiauth")) {
            System.err.println("Failed to open //blp/apiauth");
            return;
        }

        CorrelationID tokenReqId = new CorrelationID(99);
        d_session.generateToken(tokenReqId);

        int time = 0;
        while (true) {
            Event event = d_session.nextEvent(1000);
            if (event.eventType() == Event.EventType.TOKEN_STATUS) {
                time = 0;
                if (!processTokenStatus(event)) {
                    break;
                }
            } else if (event.eventType() == Event.EventType.TIMEOUT) {
                if (++ time > 20) {
                    System.err.println("Request timeout");
                    break;
                }
            } else {
                if (!processEvent(event)) {
                    break;
                }
            }
        }

        d_session.stop();
    }

    void sendRequest() throws Exception {
        Service refDataService = d_session.getService("//blp/refdata");
        Request request = refDataService.createRequest("ReferenceDataRequest");

        // append securities to request
        request.append("securities", "IBM US Equity");
        request.append("securities", "MSFT US Equity");

        // append fields to request
        request.append("fields", "PX_LAST");
        request.append("fields", "LAST_UPDATE");
        request.append("fields", "UUID");

        System.out.println("Sending Request: " + request);
        d_session.sendRequest(request, d_userIdentity, new CorrelationID(2));
    }

    boolean processTokenStatus(Event event) throws Exception {
        System.out.println("processTokenEvents");
        MessageIterator msgIter = event.messageIterator();
        while (msgIter.hasNext()) {
            Message msg = msgIter.next();

            if (msg.messageType() == TOKEN_SUCCESS) {
                System.out.println(msg);

                Service authService = d_session.getService("//blp/apiauth");
                Request authRequest = authService.createAuthorizationRequest();
                authRequest.set("token", msg.getElementAsString("token"));

                d_userIdentity = d_session.createIdentity();
                d_session.sendAuthorizationRequest(authRequest, d_userIdentity, new CorrelationID(1));
            } else if (msg.messageType() == TOKEN_FAILURE) {
                System.out.println(msg);
                return false;
            }
        }

        return true;
    }

    boolean processEvent(Event event) throws Exception {
        System.out.println("processEvent type = " + event.eventType());
        MessageIterator msgIter = event.messageIterator();
        while (msgIter.hasNext()) {
            Message msg = msgIter.next();
            if (msg.messageType() == AUTHORIZATION_SUCCESS) {
                System.out.println("Authorization SUCCESS");
                sendRequest();
            } else if (msg.messageType() == AUTHORIZATION_FAILURE) {
                System.out.println("Authorization FAILED");
                System.out.println(msg);
                return false;
            } else {
                System.out.println(msg);
                if (event.eventType() == Event.EventType.RESPONSE) {
                    System.out.println("Got Final Response");
                    return false;
                }
            }
        }
        return true;
    }

    private boolean parseCommandLine(String[] args) throws Exception {
        for (int i = 0; i < args.length; ++i) {
            if (args[i].equalsIgnoreCase("-ip")) {
                d_serverHost = args[++i];
            } else if (args[i].equalsIgnoreCase("-p")) {
                d_serverPort = Integer.parseInt(args[++i]);
            } else if (args[i].equalsIgnoreCase("-auth") && i + 1 < args.length) {
                ++i;
                if (args[i].equalsIgnoreCase(AUTH_OPTION_NONE)) {
                    d_authOptions = null;
                } else if (args[i].equalsIgnoreCase(AUTH_OPTION_USER)) {
                    d_authOptions = AUTH_USER;
                } else if (args[i].regionMatches(true, 0, AUTH_OPTION_APP,
                                                0, AUTH_OPTION_APP.length())) {
                    d_authOptions = AUTH_APP_PREFIX
                            + args[i].substring(AUTH_OPTION_APP.length());
                } else if (args[i].regionMatches(true, 0, AUTH_OPTION_DIR,
                                                0, AUTH_OPTION_DIR.length())) {
                    d_authOptions = AUTH_DIR_PREFIX
                            + args[i].substring(AUTH_OPTION_DIR.length());
                } else {
                    printUsage();
                    return false;
                }
            } else {
                printUsage();
                return false;
            }
        }
        return true;
    }
}
