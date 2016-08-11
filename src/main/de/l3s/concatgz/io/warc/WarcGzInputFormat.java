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

import de.l3s.concatgz.io.ConcatGzipInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class WarcGzInputFormat extends FileInputFormat<NullWritable, WarcWritable> {
    private static class WarcRecordReader extends RecordReader<NullWritable, WarcWritable> {
        private ConcatGzipInputFormat.ConcatGzipRecordReader gzip = new ConcatGzipInputFormat.ConcatGzipRecordReader();

        private WarcWritable value = new WarcWritable();

        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            gzip.initialize(genericSplit, context);
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return gzip.getProgress();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            boolean hasNext = gzip.nextKeyValue();
            while (hasNext) {
                value.set(gzip.getCurrentValue());
                value.setLocation(gzip.getFilename(), gzip.getPos());
                if (value.isValid()) break;
                hasNext = gzip.nextKeyValue();
            }
            return hasNext;
        }

        @Override
        public NullWritable getCurrentKey() throws IOException, InterruptedException {
            return NullWritable.get();
        }

        @Override
        public WarcWritable getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public void close() throws IOException {
            gzip.close();
        }
    }

    @Override
    public RecordReader<NullWritable, WarcWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new WarcRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return true;
    }
}
