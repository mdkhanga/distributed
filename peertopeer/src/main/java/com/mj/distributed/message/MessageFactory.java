package com.mj.distributed.message;


import java.io.DataInputStream;
import java.io.IOException;

public class MessageFactory {

    public static Message deserialize(int messageId, DataInputStream dis) throws IOException {

        if (messageId == 1) {
            PingMessage m = new PingMessage() ;
            m.deserialize(dis);
            return m ;
        }

        return null ;
    }



}
