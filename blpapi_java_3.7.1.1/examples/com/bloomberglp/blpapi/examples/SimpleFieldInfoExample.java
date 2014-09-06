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

import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.MessageIterator;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.Name;

public class SimpleFieldInfoExample {

    private static final String APIFLDS_SVC = "//blp/apiflds";
    private static final int ID_LEN         = 13;
    private static final int MNEMONIC_LEN   = 36;
    private static final int DESC_LEN       = 40;
    private static final String PADDING     =
        "                                            ";
    private static final Name FIELD_ID       = new Name("id");
    private static final Name FIELD_MNEMONIC = new Name("mnemonic");
    private static final Name FIELD_DATA     = new Name("fieldData");
    private static final Name FIELD_DESC     = new Name("description");
    private static final Name FIELD_INFO     = new Name("fieldInfo");
    private static final Name FIELD_ERROR    = new Name("fieldError");
    private static final Name FIELD_MSG      = new Name("message");

    private String d_serverHost ;
    private int    d_serverPort ;

    public static void main(String[] args) throws Exception
    {
        SimpleFieldInfoExample example = new SimpleFieldInfoExample();

        example.run(args);
    }

    private void run(String[] args) throws Exception
    {
    d_serverHost = "localhost";
    d_serverPort = 8194;

    if (!parseCommandLine(args)) {
        return ;
    }

        SessionOptions sessionOptions = new SessionOptions();
        try {
            sessionOptions.setServerHost(d_serverHost);
            sessionOptions.setServerPort(d_serverPort);
        }
        catch (Exception eip) {
            // Ignoring
        }

        System.out.println("Connecting to " + d_serverHost
                                + ":" + d_serverPort);
        Session session = new Session(sessionOptions);
        if (!session.start()) {
            System.err.println("Failed to start session.");
            return;
        }

        if (!session.openService(APIFLDS_SVC)) {
            System.out.println("Failed to open service: " + APIFLDS_SVC);
            return;
        }

        Service fieldInfoService = session.getService(APIFLDS_SVC);
        Request request = fieldInfoService.createRequest("FieldInfoRequest");
    request.append("id", "LAST_PRICE");
    request.append("id", "pq005");
    request.append("id", "zz0002");

        request.set("returnFieldDocumentation", false);

        System.out.println("Sending Request: " + request);
        session.sendRequest(request, null);

        while (true) {
            try {
                Event event             = session.nextEvent();
                MessageIterator msgIter = event.messageIterator();

                while (msgIter.hasNext()) {
                    Message msg = msgIter.next();

                    if (event.eventType() != Event.EventType.RESPONSE &&
                        event.eventType() != Event.EventType.PARTIAL_RESPONSE) {
                        continue;
                    }

            Element fields = msg.getElement(FIELD_DATA);
            int numElements = fields.numValues();

                    printHeader();
                    for (int i=0; i < numElements; i++) {
                        printField (fields.getValueAsElement(i));
                    }
                    System.out.println();
                }
        if (event.eventType() == Event.EventType.RESPONSE) break;
            }
            catch (Exception ex) {
                System.out.println ("Got Exception:" + ex);

            }
        }
    }

    private void printField (Element field)
    {
    String fldId, fldMnemonic, fldDesc;

    fldId       = field.getElementAsString(FIELD_ID);
    if (field.hasElement(FIELD_INFO)) {
        Element fldInfo     = field.getElement (FIELD_INFO) ;
        fldMnemonic = fldInfo.getElementAsString(FIELD_MNEMONIC);
        fldDesc     = fldInfo.getElementAsString(FIELD_DESC);

        System.out.println( padString(fldId, ID_LEN) +
                padString (fldMnemonic, MNEMONIC_LEN) +
                padString (fldDesc, DESC_LEN));
    }
    else {
        Element fldError = field.getElement(FIELD_ERROR) ;
        fldDesc = fldError.getElementAsString(FIELD_MSG) ;

        System.out.println ("\n ERROR: " + fldId + " - " + fldDesc) ;
    }
    }

    private void printHeader ()
    {
        System.out.println( padString("FIELD ID", ID_LEN) +
                            padString("MNEMONIC", MNEMONIC_LEN) +
                            padString("DESCRIPTION", DESC_LEN));
        System.out.println( padString("-----------", ID_LEN) +
                            padString("-----------", MNEMONIC_LEN) +
                            padString("-----------", DESC_LEN));
    }

    private static String padString(String str, int width)
    {
        if (str.length() >= width || str.length() >= PADDING.length() ) return str;
        else return str + PADDING.substring(0, width-str.length());
    }

    private boolean parseCommandLine(String[] args)
    {
        for (int i = 0; i < args.length; ++i) {
        if (args[i].equalsIgnoreCase("-ip")) {
                d_serverHost = args[i+1];
        ++i ;
            }
            else if (args[i].equalsIgnoreCase("-p")) {
                d_serverPort = Integer.parseInt(args[i+1]) ;
        ++i ;
        }
            else if (args[i].equalsIgnoreCase("-h")) {
        printUsage() ;
                return(false) ;
        }
        else {
        System.out.println ("Ignoring unknown option:" + args[i]) ;
        }
        }
    return(true);
    }

    private void printUsage()
    {
        System.out.println("Usage:");
        System.out.println(
         "  Retrieve field information in categorized form ");
        System.out.println("        [-ip <ipAddress> default = "
                                          + d_serverHost + " ]");
        System.out.println("        [-p  <tcpPort>   default = "
                                          + d_serverPort + " ]");
        System.out.println("        [-h  print this message and quit]\n");
    }
}
