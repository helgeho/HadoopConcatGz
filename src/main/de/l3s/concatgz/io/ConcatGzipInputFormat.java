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
import de.l3s.concatgz.util.CacheInputStream;
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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public class ConcatGzipInputFormat extends FileInputFormat<Text, FileBackedBytesWritable> {
    public static class ConcatGzipRecordReader extends RecordReader<Text, FileBackedBytesWritable> {
        static final int BUFFER_SIZE = 4096;
        static final byte FIRST_GZIP_BYTE = (byte) GZIPInputStream.GZIP_MAGIC;
        static final byte SECOND_GZIP_BYTE = (byte)(GZIPInputStream.GZIP_MAGIC >> 8);

        private TaskAttemptContext context = null;
        private long start = 0L;
        private long end = 0L;
        private long pos = 0L;
        private String filename = null;
        private InputStream in = null;

        private Text key = new Text();
        private FileBackedBytesWritable value = new FileBackedBytesWritable();

        private CacheInputStream cache = null;
        private InputStream cacheIn = null;
        private long cachePos = 0;
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

            FileSplit split = (FileSplit)genericSplit;
            Configuration job = context.getConfiguration();

            start = split.getStart();
            pos = start;
            end = start + split.getLength();
            Path file = split.getPath();
            filename = file.getName();

            FileSystem fs = file.getFileSystem(job);
            FSDataInputStream fsIn = fs.open(file, BUFFER_SIZE);
            fsIn.seek(start);
            in = fsIn;

            cache = new CacheInputStream(in, BUFFER_SIZE);
            cacheIn = cache.getCacheStream();
            cachePos = 0;

            hasNext = skipToNextRecord(null);
        }

        public void initialize(String path) throws IOException, InterruptedException {
            start = 0;
            pos = start;
            end = Long.MAX_VALUE;
            Path file = new Path(path);
            filename = file.getName();

            in = new FileInputStream(path);
            in.skip(start);

            cache = new CacheInputStream(in, BUFFER_SIZE);
            cacheIn = cache.getCacheStream();
            cachePos = 0;

            hasNext = skipToNextRecord(null);
        }

        private int debugCount = 0;
        private boolean skipToNextRecord(FileBackedOutputStream record) throws IOException {
            boolean findFirst = record == null;
            boolean beyondSplit = !findFirst;
            byte[] buffer = new byte[BUFFER_SIZE];
            int read = cacheIn.read(buffer);
            int bufferPos = 2;
            if (!findFirst) {
                bufferPos++;
                pos++;
                cachePos++;
            }
            if (read < bufferPos) return false;
            byte first = buffer[bufferPos - 2];
            byte second = buffer[bufferPos - 1];
            boolean isGzip = checkGzipMagic(first, second);
            do {
                debugCount++;
                if (context != null) context.progress();
                if (!isGzip) {
                    if (!beyondSplit && pos >= end) return false;
                    first = second;
                    if (bufferPos < read) {
                        second = buffer[bufferPos];
                    } else {
                        read = cacheIn.read(buffer);
                        if (read < 1) return false;
                        second = buffer[0];
                        bufferPos = 0;
                    }
                    bufferPos++;
                    pos++;
                    cachePos++;
                    isGzip = checkGzipMagic(first, second);
                }
                if (isGzip) {
                    FileBackedOutputStream cached = cache.getCache();
                    ByteSource cachedBytes = cached.asByteSource();
                    if (record != null) cachedBytes.slice(0, cachePos).copyTo(record);

                    FileBackedOutputStream newCache = new FileBackedOutputStream(BUFFER_SIZE * BUFFER_SIZE);
                    cachedBytes.slice(cachePos, cachedBytes.size() - cachePos).copyTo(newCache);
                    cache.setCache(newCache);
                    cached.close();

                    isGzip = checkGzip(cache.getCacheStream());
                    try {
                        cacheIn = cache.getCacheStream();
                    } catch (Exception e) {
                        System.out.println("debug: " + debugCount);
                        throw e;
                    }
                    cachePos = 0;
                    if (!isGzip) {
                        read = cacheIn.read(buffer);
                        bufferPos = 2;
                    }
                }
            } while (!isGzip);
            return true;
        }

        private boolean checkGzipMagic(byte first, byte second) throws IOException
        {
            return (first == FIRST_GZIP_BYTE && second == SECOND_GZIP_BYTE);
        }

        private boolean checkGzip(InputStream stream) {
            GZIPInputStream gzip = null;
            try {
                gzip = new GZIPInputStream(stream);
                gzip.read();
                return true;
            } catch (Exception e) {
                return false;
            }
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (float)((double)(Math.min(pos, end) - start) / (end - start));
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!hasNext) return false;

            key.set(filename + ":" + pos);

            FileBackedOutputStream record = new FileBackedOutputStream(BUFFER_SIZE * BUFFER_SIZE);
            hasNext = skipToNextRecord(record);
            if (!hasNext) cache.getCache().asByteSource().copyTo(record); // write remaining cache out to record
            hasNext = hasNext && pos < end;

            value.closeStream();
            value.set(record);

            return true;
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
        reader.initialize("C:\\Users\\holzmann\\DOTUK-HISTORICAL-1996-2010-PHASE2WARCS-XAABDC-20111115000000-000000.warc.gz");

        int count = 0;
        while (reader.nextKeyValue()) {
            count++;
            System.out.println(count);
        }

        System.out.println(count);
    }
}
