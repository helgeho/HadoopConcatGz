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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

public class ImmediateOutput {
    private TaskInputOutputContext context;
    private boolean flushOnWrite;
    private String dir;
    private String file;
    private FileSystem fs;
    private int bufferSize;
    private short replication;
    private Map<String, OutputStream> streams = new HashMap<>();

    private static final String CONFIG_KEY_BASE = "de.l3s.concatgz.io.immediateoutput";
    public static final String ID_PREFIX_KEY = CONFIG_KEY_BASE + ".idprefix";
    public static final String EXTENSION_KEY = CONFIG_KEY_BASE + ".extension";
    public static final String PATH_KEY = CONFIG_KEY_BASE + ".path";
    public static final String REPLICATION_KEY = CONFIG_KEY_BASE + ".replication";

    public ImmediateOutput(TaskInputOutputContext context) throws IOException {
        this(context, false);
    }

    public ImmediateOutput(TaskInputOutputContext context, boolean flushOnWrite) throws IOException {
        this.context = context;
        this.flushOnWrite = flushOnWrite;
        Configuration conf = context.getConfiguration();
        this.dir = getPath(conf);
        this.fs = FileSystem.newInstance(conf);
        this.bufferSize = conf.getInt("io.file.buffer.size", 4096);
        this.replication = getReplication(conf);

        String idPrefix = getIdPrefix(conf);
        file = "" + context.getTaskAttemptID().getTaskID().getId();
        while (file.length() < 5) file = "0" + file;
        if (idPrefix.length() > 0) file = idPrefix + "-" + file;
        file = "-" + file;
    }

    public static void initialize(Job job) {
        job.setOutputFormatClass(NullOutputFormat.class);
    }

    public static String getIdPrefix(Job job) { return getIdPrefix(job.getConfiguration()); }
    public static String getIdPrefix(Configuration conf) {
        return conf.get(ID_PREFIX_KEY, "");
    }

    public static void setIdPrefix(Job job, String idPrefix) {
        job.getConfiguration().set(ID_PREFIX_KEY, idPrefix);
    }

    public static String getExtension(Job job) { return getExtension(job.getConfiguration()); }
    public static String getExtension(Configuration conf) {
        return conf.get(EXTENSION_KEY, "");
    }

    public static void setExtension(Job job, String extension) {
        job.getConfiguration().set(EXTENSION_KEY, extension);
    }

    public static String getPath(Job job) { return getPath(job.getConfiguration()); }
    public static String getPath(Configuration conf) {
        return conf.get(PATH_KEY, "");
    }

    public static void setPath(Job job, Path path) {
        setPath(job, path.toString());
    }
    public static void setPath(Job job, String path) {
        job.getConfiguration().set(PATH_KEY, path);
    }

    public static short getReplication(Job job) { return getReplication(job.getConfiguration()); }
    public static short getReplication(Configuration conf) {
        return (short)conf.getInt(REPLICATION_KEY, 2);
    }

    public static void setReplication(Job job, short replication) {
        job.getConfiguration().setInt(REPLICATION_KEY, replication);
    }

    public void write(byte[] out) throws IOException {
        write(out, "");
    }

    public void write(byte[] out, String base) throws IOException {
        write(out, base, getExtension(context.getConfiguration()));
    }

    public void write(byte[] out, String base, String extension) throws IOException {
        OutputStream stream = stream(base, extension);
        stream.write(out);
        if (flushOnWrite) stream.flush();
    }

    public OutputStream stream(String base, String extension) throws IOException {
        Path path = new Path(dir, base + file + extension);
        OutputStream stream = streams.get(path.toString());
        if (stream == null) {
            fs.mkdirs(path.getParent());
            stream = fs.create(path, true, bufferSize, replication, fs.getDefaultBlockSize(path));
            streams.put(path.toString(), stream);
        }
        return stream;
    }

    public PrintStream printStream(String base, String extension) throws IOException {
        return new PrintStream(stream(base, extension));
    }

    public DataOutputStream dataStream(String base, String extension) throws IOException {
        return new DataOutputStream(stream(base, extension));
    }

    public void close() throws IOException {
        for (OutputStream stream : streams.values()) {
            stream.flush();
            stream.close();
        }
        fs.close();
    }
}
