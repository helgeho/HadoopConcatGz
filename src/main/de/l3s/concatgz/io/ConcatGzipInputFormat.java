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

import de.l3s.concatgz.util.CacheInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

public class ConcatGzipInputFormat extends FileInputFormat<Text, BytesWritable> {
    public static class ConcatGzipRecordReader extends RecordReader<Text, BytesWritable> {
        private int bufferSize = 4096;
        private byte firstGzip = (byte)GZIPInputStream.GZIP_MAGIC;
        private byte secondGzip = (byte)(GZIPInputStream.GZIP_MAGIC >> 8);

        private TaskAttemptContext context = null;
        private long start = 0L;
        private long end = 0L;
        private long pos = 0L;
        private String filename = null;
        private InputStream in = null; //FSDataInputStream in = null;

        private Text key = new Text();
        private BytesWritable value = new BytesWritable();

        private ByteArrayOutputStream record = null;
        private CacheInputStream cache = null;
        private InputStream cacheIn = null;
        private int cachePos = 0;
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
            FSDataInputStream fsIn = fs.open(file, bufferSize);
            fsIn.seek(start);
            in = fsIn;

            cache = new CacheInputStream(in, bufferSize);
            cacheIn = cache.getCacheStream();
            cachePos = 0;

            hasNext = skipToNextRecord(true);
        }

        public void initialize(String path) throws IOException, InterruptedException {
            start = 0;
            pos = start;
            end = Long.MAX_VALUE;
            Path file = new Path(path);
            filename = file.getName();

            in = new FileInputStream(path);
            in.skip(start);

            cache = new CacheInputStream(in, bufferSize);
            cacheIn = cache.getCacheStream();
            cachePos = 0;

            hasNext = skipToNextRecord(true);
        }

        private boolean skipToNextRecord(boolean findFirst) throws IOException {
            boolean beyondSplit = !findFirst;
            byte[] buffer = new byte[bufferSize];
            int read = cacheIn.read(buffer);
            int bufferPos = 2;
            if (!findFirst) {
                bufferPos += 1;
                pos += 1;
                cachePos += 1;
            }
            if (read < bufferPos) return false;
            byte first = buffer[bufferPos - 2];
            byte second = buffer[bufferPos - 1];
            boolean isGzip = checkGzipMagic(first, second);
            do {
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
                    bufferPos += 1;
                    pos += 1;
                    cachePos += 1;
                    isGzip = checkGzipMagic(first, second);
                }
                if (isGzip) {
                    byte[] cacheBytes = cache.getCache().toByteArray();
                    if (record != null) record.write(cacheBytes, 0, cachePos);
                    ByteArrayOutputStream newCache = new ByteArrayOutputStream();
                    newCache.write(cacheBytes, cachePos, cacheBytes.length - cachePos);
                    cache.setCache(newCache);
                    isGzip = checkGzip(cache.getCacheStream());
                    cacheIn = cache.getCacheStream();
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
            return (first == firstGzip && second == secondGzip);
        }

        private boolean checkGzip(InputStream stream) {
            GZIPInputStream gzip = null;
            try {
                gzip = new GZIPInputStream(stream);
                gzip.read();
                return true;
            } catch (Exception e) {
                return false;
            } finally {
                if (gzip != null) {
                    try {
                        gzip.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
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

            record = new ByteArrayOutputStream();
            hasNext = skipToNextRecord(false);
            if (!hasNext) record.write(cache.getCache().toByteArray());
            hasNext = hasNext && pos < end;

            byte[] bytes = record.toByteArray();
            value.set(bytes, 0, bytes.length);
            record = null;

            return true;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public BytesWritable getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new ConcatGzipRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return true;
    }

    public static void main(String[] args) throws Exception {
        ConcatGzipRecordReader reader = new ConcatGzipRecordReader();
        reader.initialize("C:\\Users\\holzmann\\DOTUK-HISTORICAL-1996-2010-GROUP-WARC-XAAAAA-20110706000000-000000.warc.gz");

        int count = 0;
        while (reader.nextKeyValue()) {
            count += 1;
        }

        System.out.println(count);
    }
}
