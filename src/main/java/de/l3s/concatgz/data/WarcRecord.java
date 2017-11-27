/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 Helge Holzmann (L3S)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package de.l3s.concatgz.data;

import org.apache.commons.io.IOUtils;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.archive.format.http.HttpHeader;
import org.archive.format.http.HttpResponse;
import org.archive.format.http.HttpResponseParser;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class WarcRecord {
    public static final String MIME_TYPE_HTTP_HEADER = "Content-Type";
    public static final String DEFAULT_HTTP_STRING_CHARSET = "UTF-8";

    /*
    public static WarcRecord get(String filename, InputStream stream) throws IOException {
        ArchiveReader reader = null;
        try {
            reader = ArchiveReaderFactory.get(filename, stream, false);
            ArchiveRecord record = reader.get();
            if (record == null) {
                throw new IOException("Record is null!");
            }
            return new WarcRecord(record);
        } catch (IOException readException) {
            System.err.println("In WarcRecord.get(...): " + readException.getMessage());
        } finally {
            if (reader != null && reader.isValid()) {
                reader.close();
            }
        }
        return null;
    }
     */

    public static WarcRecord get(String filename, InputStream stream) throws IOException {
        ArchiveReader reader = null;
        try {
            reader = ArchiveReaderFactory.get(filename, stream, false);
            ArchiveRecord record = reader.get();
            return new WarcRecord(record);
        } catch (Exception readException) {
            System.err.println("Exception while creating ArchiveRecord: " + readException.getMessage());
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception closeException) {
                    System.err.println("Exception while closing reader: " + closeException.getMessage());
                }
            }
        } finally {
            if (reader != null && reader.isValid()) {
                reader.close();
            }
        }
        return null;
    }

    public static WarcRecord get(String filename, InputStream stream, boolean atFirstRecord) throws IOException {
        ArchiveReader reader = null;
        try {
            reader = ArchiveReaderFactory.get(filename, stream, atFirstRecord);
            ArchiveRecord record = reader.get();
            return new WarcRecord(record);
        } catch (Exception readException) {
            System.err.println("Exception while creating ArchiveRecord: " + readException.getMessage());
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception closeException) {
                    System.err.println("Exception while closing reader: " + closeException.getMessage());
                }
            }
        } finally {
            if (reader != null && reader.isValid()) {
		//System.out.println("Closing reader! " + stream.available());
		//                reader.close();
            }
        }
        return null;
    }

    private ArchiveRecord record;
    private boolean isHttp = true;
    private HttpResponse http = null;
    private Map<String, String> httpHeaders = null;
    private byte[] httpBody = null;

    private WarcRecord(ArchiveRecord record) {
        this.record = record;
    }

    public ArchiveRecord getRecord() {
        return record;
    }

    public ArchiveRecordHeader getHeader() {
        return record.getHeader();
    }

    private void readHttp() {
        if (isHttp && http == null) {
            HttpResponseParser parser = new HttpResponseParser();
            try {
                http = parser.parse(record);
            } catch (Exception e) {
                isHttp = false;
            }
        }
    }

    public HttpResponse getHttpResponse() {
        readHttp();
        return http;
    }

    public boolean isHttp() {
        readHttp();
        return isHttp;
    }

    public Map<String, String> getHttpHeaders() {
        if (httpHeaders != null) return httpHeaders;

        httpHeaders = new HashMap<>();

        readHttp();
        if (http != null) {
            for (HttpHeader header : http.getHeaders()) {
                httpHeaders.put(header.getName(), header.getValue());
            }
        }

        return httpHeaders;
    }

    public String getHttpMimeType() {
        Map<String, String> headers = getHttpHeaders();
        String type = headers.get(MIME_TYPE_HTTP_HEADER);
        if (type == null) return null;
        String[] split = type.split(";");
        return split.length == 0 ? "" : split[0].trim();
    }

    public byte[] getHttpBody() throws IOException {
        if (httpBody != null) return httpBody;

        readHttp();
        if (http != null) httpBody = IOUtils.toByteArray(http);
        return httpBody;
    }

    public String getHttpStringBody() throws IOException {
        Map<String, String> headers = getHttpHeaders();
        String charset = DEFAULT_HTTP_STRING_CHARSET;
        String type = headers.get(MIME_TYPE_HTTP_HEADER);
        if (type != null) {
            String[] split = type.split("[ =]");
            for (int i = 0; i < split.length - 1; i++) {
                if (split[i].equalsIgnoreCase("charset")) {
                    charset = split[i + 1];
                    break;
                }
            }
        }
        return EntityUtils.toString(new ByteArrayEntity(getHttpBody()), charset);
    }

    public void close() {
        /*
        try {
            record.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NumberFormatException e) {
        */
            /* This shit should not happen. Anyway. It happens when close() is called
            on an ArchiveRecord that then calls into WARCRecord (this is the one from
            the Internet Archive library, not this one, duh) to actually check if there
            is more data available. There is a parseLong in there and sometimes the length
            of an archive is utter garbage. End of story, please go cry in a corner now. */

            /* Addendum: Yes, this should not be the ArchiveRecords responsibility, but
            rather the Readers */
            /*
            e.printStackTrace();
        }
        */
    }
}
