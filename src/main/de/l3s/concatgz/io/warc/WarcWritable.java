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

package de.l3s.concatgz.io.warc;

import com.google.common.io.FileBackedOutputStream;
import de.l3s.concatgz.data.WarcRecord;
import de.l3s.concatgz.io.FileBackedBytesWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WarcWritable extends FileBackedBytesWritable {
    private boolean valid = true;
    private WarcRecord record = null;
    private String filename = null;
    private long offset = -1;

    public void clear() {
        if (record != null) record.close();
        record = null;
        valid = true;
    }

    public void setLocation(String filename, long offset) {
        this.filename = filename;
        this.offset = offset;
    }

    public String getFilename() {
        return filename;
    }

    public long getOffset() {
        return offset;
    }

    private void readRecord() throws IOException {
        if (record != null || !valid) return;
        record = WarcRecord.get(filename, getBytes().openBufferedStream());
        valid = record != null;
    }

    public WarcRecord getRecord() throws IOException {
        readRecord();
        return record;
    }

    public boolean isValid() throws IOException {
        readRecord();
        return valid;
    }

    @Override
    public void set(FileBackedOutputStream newData) throws IOException {
        clear();
        super.set(newData);
    }

    @Override
    public void set(FileBackedBytesWritable newData) throws IOException {
        clear();
        super.set(newData);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        clear();
        setLocation(in.readUTF(), in.readLong());
        super.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(filename);
        out.writeLong(offset);
        super.write(out);
    }
}
