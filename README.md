# A Splitable Hadoop InputFormat for Concatenated GZIP Files

GZIP compressed files are usually considered to be non-splitable.
This makes them a rather impractical input format for distributed processing / [Hadoop](http://hadoop.apache.org), which stores its files in a distributed filesystem and exploits data locality to gain efficiency.
However, a GZIP file may be comprised of multiple concatenated GZIP records, which constitutes a valid GZIP file again.
Such compressed files can be split by identifying the start of a new record and this way, can be used by Hadoop much more efficiently.

One application that heavily uses concatenated GZIP files is Web archives.
The standardized data format [WARC](https://en.wikipedia.org/wiki/Web_ARChive) is typically stored in GZIP files with every record being a separate GZIP record.

We provide both, one general input format for concatenated GZIP files (*ConcatGzipInputFormat*) as well as a specialized one for \*.warc.gz files, i.e., WARC records stored in concatenated GZIP files (*WarcGzInputFormat*). The *WarcGzInputFormat* also serves as an example to show how to easily implement custom input formats based on concatenated GZIP files.

### [ConcatGzipInputFormat](src/main/de/l3s/concatgz/io/ConcatGzipInputFormat.java)

If configured as input format in your Hadoop job, the key in your mapper will be of *Text* and the value of type *BytesWritable*.
The key contains the filname as well as the position of the current value in the form *filename:position*.
The value contains the currnet GZIP record as bytes, which can be accessed by calling *value.copyBytes()*.
To decompress and read the contents, you can load the byte array into an input stream:

```java
ByteArrayInputStream in = new ByteArrayInputStream(value.copyBytes());
GZIPInputStream decompressed = new GZIPInputStream(in);
...
decompressed.read();
...
decompressed.close();
```

### [WarcGzInputFormat](src/main/de/l3s/concatgz/io/warc/WarcGzInputFormat.java)

This input format gives you only a value of type *WarcWritable*, with the key being of type *NullWritable*.
*WarcWritable* provides access to the raw (compressed) bytes, the filename, the offset as well as wrapped *[WarcRecord](src/main/de/l3s/concatgz/data/WarcRecord.java)* with convenient getter methods to read headers, contents and parsed HTTP responses.

```java
...
String file = value.getFilename();
long offset = value.getOffset();
WarcRecord warc = value.getRecord();
...
ArchiveRecord record = warc.getRecord();
ArchiveRecordHeader header = warc.getHeader();
if (warc.isHttp()) {
    Map<String, String> httpHeaders = warc.getHttpHeaders();
    byte[] body = warc.getHttpBody();
    String bodyString = warc.getHttpStringBody();
    String mime = warc.getHttpMimeType();
    ...
}
...
```

### Output

To facilitate the output with Hadoop and allow for easy creation of GZIP records and concatenated GZIP files, we provide some helper classes:

[*ImmediateOutput*](src/main/de/l3s/concatgz/io/ImmediateOutput.java) enables you to write out data directly to HDFS without using Hadoop output mechanisms.
It takes care of a unique naming per task and overwrites files in case of failed and rerun tasks.
This is how you configure your Hadoop job to use *ImmediateOutput*:
```java
Job job = Job.getInstance(config);
ImmediateOutput.initialize(job);
ImmediateOutput.setPath(job, outPath);
ImmediateOutput.setExtension(job, ".gz");
ImmediateOutput.setReplication(job, (short) 2);
```

Now, to use it in your mapper / reducer, you need to create an instance as follows:

```java
private ImmediateOutput output;

@Override
protected void setup(Context context) throws IOException, InterruptedException {
    output = new ImmediateOutput(context, true);
}

@Override
public void cleanup(Context context) throws IOException, InterruptedException {
    output.close();
}
```

[*GZipBytes*](src/main/de/l3s/concatgz/util/GZipBytes.java) lets you output any kind of data into a GZIP compressed byte array, that you can write out using your *ImmediateOutput*:

```java
GZipBytes gzip = new GZipBytes();
...
OutputStream out = gzip.open();
out.write(...);
...
byte[] bytes = out.close();
output.write(bytes, "location/relative/to/outPath/raw");
...
DataOutputStream data = gzip.openData();
data.writeInt(123);
...
bytes = data.close();
output.write(bytes, "location/relative/to/outPath/data");
...
PrintStream print = gzip.openPrint();
print.println("text");
...
bytes = print.close();
output.write(bytes, "location/relative/to/outPath/text");
...
```

## Build

The easiest way to build a JAR file of this project that you can add to your Hadoop classpath is to use Maven:

`mvn package`

## License

The MIT License (MIT)

Copyright (c) 2016 Helge Holzmann (L3S)

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
