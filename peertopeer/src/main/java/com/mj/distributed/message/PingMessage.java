package com.mj.distributed.message;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by manoj on 6/18/17.
 */
public class PingMessage implements Message {

    private static long messageId = 1L ;
    private int serverId ;

    public long getMessageId() {
        return messageId ;
    }

    public void serialize(DataOutputStream out) throws IOException {
        out.writeLong(messageId) ;
        out.writeInt(serverId) ;
    }

    public Message deserialize(DataInputStream in) throws IOException {
        serverId = in.readInt() ;
        PingMessage message = new PingMessage() ;
        return message ;
    }

    public int getServerId() {
        return serverId ;
    }

    public void setServerId(int id) {
        serverId = id ;
    }
}
