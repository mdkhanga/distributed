package com.mj.distributed.message;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by manoj on 6/18/17.
 */
public class AckMessage extends AbstractMessage {

    private static int messageTypeId = 2 ;
    private long idOfMessageAcked ;

    public long getMessageTypeId() {
        return messageId ;
    }


    public AckMessage() {

    }

    public AckMessage(int id) {
        idOfMessageAcked= id ;
    }

    public void serialize(DataOutputStream out) throws IOException {
        out.writeInt(messageTypeId) ;
        out.writeLong(idOfMessageAcked) ;
    }

    public void deserialize(DataInputStream in) throws IOException {
        idOfMessageAcked = in.readLong() ;

    }



    public String print() {

        return "Ack for message :" + idOfMessageAcked ;
    }
}
