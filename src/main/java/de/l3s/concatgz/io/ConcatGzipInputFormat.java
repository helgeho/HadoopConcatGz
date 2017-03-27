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

import com.google.common.io.FileBackedOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.*;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

public class ConcatGzipInputFormat extends FileInputFormat<Text, FileBackedBytesWritable> {
    public static class ConcatGzipRecordReader extends RecordReader<Text, FileBackedBytesWritable> {
        static final int BUFFER_SIZE = 8192;
        static final byte FIRST_GZIP_BYTE = (byte) 0x1f;
        static final byte SECOND_GZIP_BYTE = (byte) 0x8b;
        static final byte[] GZIP_MAGIC = new byte[]{FIRST_GZIP_BYTE, SECOND_GZIP_BYTE};

        private TaskAttemptContext context = null;
        private long start = 0L;
        private long end = 0L;
        private long pos = 0L;
        private long lastRecordOffset = 0L;
        private String filename = null;
        private InputStream in = null;

        private Text key = new Text();
        private FileBackedBytesWritable value = new FileBackedBytesWritable();

        private PushbackInputStream cache = null;
        private boolean hasNext = false;

        public String getFilename() {
            return filename;
        }

        public long getPos() {
            return pos;
        }

        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            this.context = context;

            FileSplit split = (FileSplit) genericSplit;
            Configuration job = context.getConfiguration();

            start = split.getStart();
            pos = start;
            end = start + split.getLength();
            //System.out.println("Start: " + start + " length: " + split.getLength() + " end: " + end);
            Path file = split.getPath();
            filename = file.getName();

            FileSystem fs = file.getFileSystem(job);
            FSDataInputStream fsIn = fs.open(file, BUFFER_SIZE);
	    fsIn.seek(start);
            in = fsIn;

            hasNext = true; // skipToNextRecord(null);
        }

        public void initialize(String path) throws IOException, InterruptedException {
            Path file = new Path(path);
            String filename = file.getName();

            FileInputStream in = new FileInputStream(path);
            in.skip(start);

            initialize(in, filename);
        }

        public void initialize(InputStream in, String filename) throws IOException, InterruptedException {
            start = 0;
            pos = start;
            end = Long.MAX_VALUE;

            hasNext = true;
            this.filename = filename;
            this.in = in;
        }

        private boolean skipToNextRecord(FileBackedOutputStream record) throws IOException {
            byte[] buffer = new byte[BUFFER_SIZE];
            int read;
            int gzipBytesLocation;
            long bytesRead = pos;
            int startOffset;

            if (cache == null) { // pos == start && pos > 0
                cache = new PushbackInputStream(in, BUFFER_SIZE);
            }

	    /* read until we find a GZIP location or have no more data */
            do {
                read = cache.read(buffer);
                if (read <= 0) {
                    return false;
                }
                bytesRead += read;
            } while ((gzipBytesLocation = findAndCheckGZIP(buffer, 0, read)) == -1);
            /* Save start of the found GZIP stream to set key value later */
            lastRecordOffset = bytesRead - (read - gzipBytesLocation);
            //System.out.println("lastRecOff: " + lastRecordOffset + " bytesRead: " + bytesRead + " pos: " + pos);
	    /* offset of the found GZIP location in the current buffer */
            startOffset = gzipBytesLocation;

	    /* is there enough data left to look for another GZIP location in the current buffer? */
            int from = gzipBytesLocation + 2 < read - 2 ? gzipBytesLocation + 2 : read;

            while (read > 0 && (gzipBytesLocation = findAndCheckGZIP(buffer, from, read)) == -1) {
                if (read <= 0) {
                    return false;
                }

                record.write(buffer, startOffset, read - startOffset);
                if (startOffset != 0)
                    startOffset = 0;
                if (from != 0)
                    from = 0;
                read = cache.read(buffer);
                bytesRead += read;
            }

	    /* write out the current buffer up to the next GZIP location */
            record.write(buffer, startOffset, Math.max(0, gzipBytesLocation) - startOffset);
	    /* accounting, remove the number of bytes read and add number of bytes to the next GZIP location*/
            bytesRead -= (Math.max(0, read) - Math.max(0, gzipBytesLocation));
            //System.out.println("read: " + read + " gzipBytes: " + gzipBytesLocation + " startOFfset: " + startOffset + " bytesRead: " + bytesRead);
            pos = bytesRead;
            cache.unread(buffer, Math.max(0, gzipBytesLocation), Math.max(0, read) - Math.max(0, gzipBytesLocation));

            return pos <= end;
        }

        private int findAndCheckGZIP(byte[] data, int start, int length) throws IOException {
            for (int i = start; i < length - 1; i++) {
                if (data[i] == FIRST_GZIP_BYTE && data[i+1] == SECOND_GZIP_BYTE) {
                    //System.out.println("Possible GZIp at: " + i);
                    if (checkGzip(data, i, length - i)) {
                        //System.out.println("Actual GZIp at: " + i);
                        return i;
                    }
                }
            }

            if (data[length-1] == FIRST_GZIP_BYTE) {
                //System.out.println("Found possible GZIp byte at last location");
                if (checkGzip(data, length - 1, 1)) {
                    //System.out.println("returned: " + (length - 1));
                    return length - 1;
                }
            }

            return -1;
        }

        private boolean     checkGzip(byte[] data, int offset, int length) throws IOException {
            /* this buffer is only used if we are near the end of the given data,
             * since checkGzip needs some minimum of bytes to be sure that the data
             * is valid or not */
            byte[] buffer = null;
            ByteArrayInputStream dataStream = new ByteArrayInputStream(data, offset, length);
            InputStream is;

            /* GZIP header has at least 20 bytes, so use a few more to be on the save side */
            //System.out.println("length: " + length + " offset: " + offset);
            if (length < 128) {
                buffer = new byte[BUFFER_SIZE / 8];
                cache.read(buffer);
                is = new SequenceInputStream(dataStream, new ByteArrayInputStream(buffer));
            } else {
                is = dataStream;
            }

            GZIPInputStream gzip;
            try {
                gzip = new GZIPInputStream(is);
                gzip.read();
                gzip.close();
                return true;
            } catch (Exception e) {
                return false;
            } finally {
                if (buffer != null) {
                    is.close();
                    cache.unread(buffer);
                }

                dataStream.close();
            }
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (float) ((double) (Math.min(pos, end) - start) / (end - start));
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!hasNext) return false;

            FileBackedOutputStream record = new FileBackedOutputStream(BUFFER_SIZE * BUFFER_SIZE);
            hasNext = skipToNextRecord(record);
            key.set(filename + ":" + lastRecordOffset);
            hasNext = hasNext && pos < end;

            value.closeStream();
            value.set(record);

            return hasNext;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public FileBackedBytesWritable getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public synchronized void close() throws IOException {
            value.closeStream();
            cache.close();
            in.close();
        }
    }

    @Override
    public RecordReader<Text, FileBackedBytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new ConcatGzipRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return true;
    }

    public static void main(String[] args) throws Exception {
        ConcatGzipRecordReader reader = new ConcatGzipRecordReader();
        reader.initialize("/home/gothos/warc/DOTUK-HISTORICAL-1996-2010-GROUP-AC-XAAUMV-20110428000000-00000.arc.gz");

        int count = 0;
        while (reader.nextKeyValue()) {
            count++;
            System.out.println(count);
        }

        System.out.println(count);
    }
}
