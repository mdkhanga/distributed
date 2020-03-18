package com.mj.distributed.message;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by manoj on 6/18/17.
 */
public class AckMessage  {

    private static int messageTypeId = 3 ;
    private int seqOfMessageAcked ;

    public int getMessageTypeId() {
        return messageTypeId ;
    }


    public AckMessage() {

    }

    public AckMessage(int id) {
        seqOfMessageAcked= id ;
    }

    public ByteBuffer serialize() {


        return null ;
    }

    public static AckMessage deserialize(ByteBuffer b) {

        return null ;
    }

    public int size() {

        // length 4
        // messageTypeId 4
        // int seqOfMessageAcked

        return 12 ;

    }


}
