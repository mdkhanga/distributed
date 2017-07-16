package com.mj.distributed.message;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by manoj on 6/18/17.
 */
public class ServerConfigMessage extends AbstractMessage {

    private static int messageTypeId = 3 ;
    // private int serverId ;
    private String ipAddress ;
    private int listenport ;

    public long getMessageTypeId() {
        return messageTypeId ;
    }

    public ServerConfigMessage() {

    }



    public void serialize(DataOutputStream out) throws IOException {
        out.writeInt(messageTypeId) ;
        out.writeUTF(ipAddress);
        out.writeInt(listenport) ;
     }

    public void deserialize(DataInputStream in) throws IOException {
        ipAddress = in.readUTF() ;
        messageId = in.readInt() ;

    }


    public String print() {

        return "Config message from server :" + ipAddress + ":" + listenport ;
    }
}
