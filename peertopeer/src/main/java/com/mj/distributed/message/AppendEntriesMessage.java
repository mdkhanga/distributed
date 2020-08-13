package com.mj.distributed.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class AppendEntriesMessage {

    private static int messageType = 4;

    private int leaderId = 1;

    private int term = 0 ;
    private List<LogEntry> entries = new ArrayList<>();

    private int prevTerm = -1 ;
    private int prevIndex = -1 ;

    private int leaderCommitIndex = -1;

    private int seqId ;

    private static Logger LOG  = LoggerFactory.getLogger(AppendEntriesMessage.class);

    public AppendEntriesMessage(int leaderId, int seqId) {
        this.leaderId = leaderId;
        this.seqId = seqId;
    }

    public AppendEntriesMessage(int leaderId, int seqId, int prevIndex, int leaderCommitIndex) {

        this.leaderId = leaderId;
        this.seqId = seqId;
        this.prevIndex = prevIndex;
        this.leaderCommitIndex = leaderCommitIndex;
    }

    public void addLogEntry(LogEntry e) {
        entries.add(e);
    }

    public List<LogEntry> getLogEntries() {
        return entries;
    }

    public LogEntry getLogEntry() {
        return entries.size() == 1 ? entries.get(0) : null;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public int getSeqId() {
        return seqId;
    }

    public int getPrevIndex() {
        return prevIndex ;
    }

    public void setPrevIndex(int i) {
        prevIndex = i;
    }

    public int getLeaderCommitIndex() {
        return leaderCommitIndex;
    }

    public ByteBuffer serialize() throws Exception {

        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);

        d.writeInt(messageType);
        d.writeInt(leaderId);
        d.writeInt(seqId);
        d.writeInt(prevIndex);
        d.writeInt(leaderCommitIndex);

        // LOG.info("Ser Entries size "+ entries.size());
        d.writeInt(entries.size());
        if (entries.size() > 0) {

            // d.writeInt(entries.size());

            entries.forEach((e)->{
                try {
                    byte[] ebytes = e.toBytes();
                    d.writeInt(ebytes.length);
                    d.write(ebytes);
                } catch(Exception e1) {
                    throw new RuntimeException(e1);
                }
            });
        }

        byte[] bytestoWrite = b.toByteArray();
        ByteBuffer ret = ByteBuffer.allocate(bytestoWrite.length+4);
        ret.putInt(bytestoWrite.length);
        ret.put(bytestoWrite);
        ret.flip();
        return ret ;
    }

    public static AppendEntriesMessage deserialize(ByteBuffer b) throws Exception {
        int messagesize = b.getInt() ;
        // LOG.info("Received message of size " + messagesize) ;
        int type = b.getInt() ;
        if (messageType != type) {
            throw new RuntimeException("Message is not the expected type AppendEntriesMessage") ;
        }

        int leaderId = b.getInt();
        int seqId = b.getInt();
        int prevIndex = b.getInt();
        int leaderCommitIndex = b.getInt();

        AppendEntriesMessage newMsg = new AppendEntriesMessage(leaderId, seqId, prevIndex, leaderCommitIndex);

        int numEntries = b.getInt() ;
        // LOG.info("Deser num log entries " + numEntries) ;

        while (numEntries > 0) {
            // LOG.info("reading entries") ;
            int size = b.getInt();
            byte[] entrybytes = new byte[size];
            int position = b.position() ;
            b = b.get(entrybytes, 0, size);
            newMsg.addLogEntry(LogEntry.fromBytes(entrybytes));
            --numEntries;
        }

        // LOG.info("returning deserialized appendEntriesMsg") ;
        return newMsg ;
    }
}
