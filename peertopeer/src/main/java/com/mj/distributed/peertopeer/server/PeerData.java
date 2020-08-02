package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.AppendEntriesMessage;
import com.mj.distributed.message.LogEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class PeerData {

    private String hostString ;
    private int port ;
    private int serverId ;
    private int lastSeqAcked ;
    private AtomicInteger seq = new AtomicInteger(0) ;
    private ConcurrentHashMap<Integer, Integer> seqIdLogIndexMap = new ConcurrentHashMap() ;
    private int lastIndexReplicated = -1;
    private int lastIndexCommitted = -1;


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

    public void addMessageForPeer(AppendEntriesMessage msg) throws Exception {

       // List<LogEntry> entries = msg.getLogEntries() ;
        LogEntry e = msg.getLogEntry() ;

        if (e != null) {
            seqIdLogIndexMap.put(msg.getSeqId(), e.getIndex());
        }
        ByteBuffer b = msg.serialize() ;
        addWriteBuffer(b);
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

    public int getLastIndexCommitted() {
        return lastIndexCommitted ;
    }

    public void setLastIndexCommitted(int i) {
        lastIndexCommitted = i ;
    }

    public int getLastIndexReplicated() {
        return lastIndexReplicated ;
    }

    public void setLastIndexReplicated(int i) {
        lastIndexReplicated = i;
    }

    public int getNextIndexToReplicate() {

        int ret = lastIndexReplicated + 1 ;
        return ret ;
    }

    public int getIndexAcked(int seqId) {
        return seqIdLogIndexMap.getOrDefault(seqId, -1);
    }


    public int getServerId() {
        return serverId;
    }

    public void setServerId(int i) {
        serverId = i ;
    }
}
