package com.mj.distributed.message;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by manoj on 6/18/17.
 */
public class AckMessage  {

    private static int messageType = 3 ;
    private int seqOfMessageAcked ;

    public int getMessageTypeId() {
        return messageType ;
    }


    public AckMessage() {

    }

    public AckMessage(int id) {
        seqOfMessageAcked= id ;
    }

    public int getSeqOfMessageAcked() {
        return seqOfMessageAcked;
    }

    public ByteBuffer serialize() {

        ByteBuffer b = ByteBuffer.allocate(size()) ;
        b.putInt(size()) ;
        b.putInt(messageType) ;
        b.putInt(seqOfMessageAcked) ;

        return b ;
    }

    public static AckMessage deserialize(ByteBuffer b) {

        int size = b.getInt() ;

        int type = b.getInt() ;

        if (type != messageType) {
            throw new RuntimeException("Not a ping message "+ type) ;
        }

        int seq = b.getInt() ;

        AckMessage r = new AckMessage(seq) ;

        return r ;
    }

    public int size() {

        // length 4
        // messageTypeId 4
        // int seqOfMessageAcked

        return 12 ;

    }


}
