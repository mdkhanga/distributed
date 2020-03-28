package com.mj.distributed.peertopeer.server;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class PeerData {

    private String hostString ;
    private int port ;
    private int serverId ;
    private int lastSeqAcked ;
    private AtomicInteger seq = new AtomicInteger(0) ;


    Queue<ByteBuffer> writeQueue = new ConcurrentLinkedDeque<>() ;

    public PeerData(String hostString) {
        this.hostString = hostString ;
    }

    public PeerData(String hostString, int port) {

        this.hostString = hostString ;
        this.port = port ;

    }

    public int getNextSeq() {

        return seq.incrementAndGet() ;
    }

    public ByteBuffer getNextWriteBuffer() {
        return writeQueue.poll() ;
    }

    public ByteBuffer peekWriteBuffer() {
        return writeQueue.peek() ;
    }

    public void addWriteBuffer(ByteBuffer b) {
        writeQueue.add(b) ;
    }

    public String getHostString() {
        return hostString ;
    }

    public void setHostString(String s) {
        hostString = s ;
    }

    public int getPort() {
        return port ;
    }

    public void setPort(int p) {
        port = p ;
    }

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int i) {
        serverId = i ;
    }
}
