package de.l3s.concatgz.data;

public class GzipRecord {
    public long pos;
    public String filename;
    public byte[] data;

    public GzipRecord(long pos, String filename, byte[] data) {
        this.pos = pos;
        this.filename = filename;
        this.data = data;
    }
}
