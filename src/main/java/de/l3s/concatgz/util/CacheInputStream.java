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

package de.l3s.concatgz.util;

import com.google.common.io.ByteSource;
import com.google.common.io.FileBackedOutputStream;

import java.io.*;

public class CacheInputStream extends InputStream {
    private FileBackedOutputStream cache;

    private ByteArrayInputStream buffer = new ByteArrayInputStream(new byte[0]);

    private InputStream in;
    private int bufferSize;
    private int bufferLeft = 0;
    private boolean eof = false;

    public CacheInputStream(InputStream in, int bufferSize) {
        this.in = in;
        this.bufferSize = bufferSize;
        this.cache = new FileBackedOutputStream(bufferSize * bufferSize);
    }

    public FileBackedOutputStream getCache() {
        return cache;
    }

    public void setCache(FileBackedOutputStream cache) {
        this.cache = cache;
    }

    public InputStream getCacheStream() throws IOException {
        ByteSource cachedBytes = cache.asByteSource();
        InputStream cachedStream = cachedBytes.slice(0, cachedBytes.size() - bufferLeft).openBufferedStream();
        return new SequenceInputStream(cachedStream, this);
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int read = 0;
        while (read == 0) read = fillBuffer(len);
        if (read == -1) return -1;
        bufferLeft -= read;
        return buffer.read(b, off, read);
    }

    @Override
    public int read() throws IOException {
        int read = 0;
        while (read == 0) read = fillBuffer(1);
        if (read == -1) return -1;
        bufferLeft -= 1;
        return buffer.read();
    }

    private int fillBuffer(int len) throws IOException {
        if (bufferLeft == 0 && eof) return -1;
        if (bufferLeft >= len) return len;

        byte[] left;
        if (bufferLeft > 0) {
            left = new byte[bufferLeft];
            buffer.read(left);
        } else {
            if (eof) return -1;
            left = new byte[0];
        }

        byte[] bytes = new byte[Math.max(bufferSize, len - bufferLeft)];
        int read = in.read(bytes);
        if (read == -1) {
            eof = true;
            if (bufferLeft == 0) return -1;
            read = 0;
        } else {
            cache.write(bytes, 0, read);
        }

        bufferLeft += read;

        ByteArrayOutputStream merged = new ByteArrayOutputStream();
        merged.write(left);
        merged.write(bytes, 0, read);
        buffer = new ByteArrayInputStream(merged.toByteArray());

        return Math.min(bufferLeft, len);
    }

    @Override
    public void close() throws IOException {
        cache.reset();
        cache.close();
    }

    @Override
    public boolean markSupported() {
        return false;
    }
}
