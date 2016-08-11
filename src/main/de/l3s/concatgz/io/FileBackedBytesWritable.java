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

package de.l3s.concatgz.io;

import com.google.common.io.ByteSource;
import com.google.common.io.FileBackedOutputStream;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

public class FileBackedBytesWritable implements Writable {
    static final int BUFFER_SIZE = 4096;

    private FileBackedOutputStream stream = null;
    private boolean setExternally = false;

    public ByteSource getBytes() {
        if (stream == null) return null;
        return stream.asByteSource();
    }

    public void set(FileBackedOutputStream stream) throws IOException {
        set(stream, !setExternally);
    }

    public void set(FileBackedOutputStream stream, boolean closeCurrentStream) throws IOException {
        if (closeCurrentStream) closeStream();
        this.stream = stream;
        setExternally = true;
    }

    public void set(FileBackedBytesWritable writable) throws IOException {
        set(writable, !setExternally);
    }

    public void set(FileBackedBytesWritable writable, boolean closeCurrentStream) throws IOException {
        if (closeCurrentStream) closeStream();
        stream = writable.stream;
        setExternally = writable.setExternally;
    }

    public void closeStream() throws IOException {
        if (stream == null) return;
        stream.close();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if (stream == null) return;
        ByteSource bytes = stream.asByteSource();
        long size = bytes.size();
        dataOutput.writeLong(size);
        byte[] buffer = new byte[BUFFER_SIZE];
        InputStream stream = bytes.openBufferedStream();
        int read = stream.read(buffer);
        while (read != -1) {
            if (read > 0) dataOutput.write(buffer, 0, read);
            read = stream.read(buffer);
        }
        stream.close();
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        if (!setExternally) closeStream();
        long remaining = dataInput.readLong();
        stream = new FileBackedOutputStream(BUFFER_SIZE * BUFFER_SIZE);
        byte[] buffer = new byte[BUFFER_SIZE];
        while (remaining > 0) {
            int read = BUFFER_SIZE < remaining ? BUFFER_SIZE : (int)remaining;
            dataInput.readFully(buffer, 0, read);
            stream.write(buffer, 0, read);
        }
        setExternally = false;
    }
}
