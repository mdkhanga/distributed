package com.mj.distributed.message;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by manoj on 6/18/17.
 */
public class PingMessage extends AbstractMessage {

    private static int messageTypeId = 1 ;
    private int serverId ;

    public long getMessageTypeId() {
        return messageTypeId ;
    }

    public PingMessage() {

    }

    public PingMessage(int id) {
        serverId = id ;
    }

    public void serialize(DataOutputStream out) throws IOException {
        out.writeInt(messageTypeId) ;
        out.writeLong(getMessageId());
        out.writeInt(serverId) ;
     }

    public void deserialize(DataInputStream in) throws IOException {
        messageId = in.readLong() ;
        serverId = in.readInt() ;

    }

    public int getServerId() {
        return serverId ;
    }

    public void setServerId(int id) {
        serverId = id ;
    }

    public String print() {

        return "Ping message from server :" + serverId ;
    }
}
