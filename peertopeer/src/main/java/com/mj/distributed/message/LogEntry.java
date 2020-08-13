package com.mj.distributed.message;


import java.io.*;
import java.nio.ByteBuffer;

public class LogEntry {

    private int index;
    private byte[] entry;

    public LogEntry(int index, int value) {
        this.index = index;
        this.entry = ByteBuffer.allocate(4).putInt(value).array();
    }

    public LogEntry(int index, byte[] val) {
        this.index = index;
        this.entry = val;
    }

    byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(out);
        d.writeInt(index);
        d.write(entry);
        byte[] ret = out.toByteArray();
        out.close();
        d.close();
        return ret;
    }

    public static LogEntry fromBytes(byte[] bytes) throws IOException {

        ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
        DataInputStream din = new DataInputStream(bin);
        int index = din.readInt() ;
        // byte[] v = din.readAllBytes() ;
        int v = din.readInt();

        return new LogEntry(index, v);

    }

    public int getIndex() {
        return index;
    }

    public byte[] getEntry() {
        return entry;
    }
}
