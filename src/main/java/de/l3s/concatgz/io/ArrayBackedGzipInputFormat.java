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

import de.l3s.concatgz.data.GzipRecord;
import de.l3s.concatgz.io.ConcatGzipInputFormat.ConcatGzipRecordReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.*;

public class ArrayBackedGzipInputFormat extends FileInputFormat<Text, GzipRecord> {
    public static class ArrayBackedRecordReader extends RecordReader<Text, GzipRecord> {
        private Text key = new Text();

        byte[] value = null;
        private ConcatGzipRecordReader concatGzipIF = null;

        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
            concatGzipIF = new ConcatGzipRecordReader();
            concatGzipIF.initialize(genericSplit, context);
        }

        public void initialize(String path) throws IOException, InterruptedException {
            concatGzipIF.initialize(path);
        }

        public void initialize(InputStream in, String filename) throws IOException, InterruptedException {
            concatGzipIF.initialize(in, filename);
        }

        public float getProgress() throws IOException, InterruptedException {
            return concatGzipIF.getProgress();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!concatGzipIF.getHasNext()) return false;

            concatGzipIF.nextKeyValue();
            value = concatGzipIF.getCurrentValue().getBytes().read();
            key = concatGzipIF.getCurrentKey();

            return concatGzipIF.getHasNext();
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public GzipRecord getCurrentValue() throws IOException, InterruptedException {
            return new GzipRecord(concatGzipIF.getPos(), concatGzipIF.getFilename(), value);
        }

        @Override
        public synchronized void close() throws IOException {
            concatGzipIF.close();
        }
    }

    @Override
    public RecordReader<Text, GzipRecord> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new ArrayBackedRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return true;
    }

    public static void main(String[] args) throws Exception {
        ArrayBackedRecordReader reader = new ArrayBackedRecordReader();
        reader.initialize(args[1]);

        int count = 0;
        while (reader.nextKeyValue()) {
            count++;

            System.out.println("Count:" + count + " pos: " + reader.concatGzipIF.getPos());
        }
    }
}
