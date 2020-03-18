package com.mj.distributed.message;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by manoj on 6/18/17.
 */
public class PingMessage {

    private static int messageType = 2 ;
    private int serverId ; // server sending ping
    private int seqId ;

    public long getMessageTypeId() {
        return messageType ;
    }

    public PingMessage() {

    }

    public PingMessage(int id, int s) {
        serverId = id ;
        seqId = s ;
    }

    public ByteBuffer serialize() throws IOException {
        ByteBuffer b = ByteBuffer.allocate(size()) ;

        b.putInt(size()) ;
        b.putInt(messageType) ;
        b.putInt(serverId) ;
        b.putInt(seqId) ;

        return b ;
     }

    public static PingMessage deserialize(ByteBuffer readBuffer) throws IOException {

        int size = readBuffer.getInt() ;


        int mType = readBuffer.getInt() ;
        if (mType != messageType) {
            throw new RuntimeException("Not a ping message "+ mType) ;
        }

        int server = readBuffer.getInt() ;
        int seq = readBuffer.getInt() ;

        PingMessage p = new PingMessage(server, seq) ;

        return p ;

    }

    public int getServerId() {
        return serverId ;
    }

    public void setServerId(int id) {
        serverId = id ;
    }

    public int getSeqId() {
        return seqId ;
    }

    public String print() {

        return "Ping message from server :" + serverId ;
    }

    public int size() {

        // int s = 0 ;
        // s=+ 4 ; // size
        // s=+4 ; // message type
        // s=+4 ; // from server
        // s=+4 ; // seq

        return 16 ;
    }
}
