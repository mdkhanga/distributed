package com.mj.distributed.message;

import com.mj.distributed.peertopeer.server.PeerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class HelloMessage {

    private int messageType = 1 ;
    private String greeting = "Hello" ;
    private String hostString ;
    private int hostPort ;
    private int recordsize ;

    private Logger LOG  = LoggerFactory.getLogger(HelloMessage.class) ;

    public HelloMessage(String host,int port) {

        this.hostString = host ;
        this.hostPort = port ;

        try {
            recordsize = 4 + greeting.getBytes("UTF-8").length + 4+ "111.111.111.111".getBytes("UTF-8").length + 4;
        } catch(UnsupportedEncodingException e) {
          LOG.error("Error getting size",e)  ;
          recordsize = 62 ; // 4+10+4+30+4
        }

    }

    public String getHostString() {
        return hostString ;
    }

    public int getHostPort() {
        return hostPort ;
    }

    public ByteBuffer serialize() throws Exception {

        int messagesize = recordsize+4 ; // len of record + 4. we write len in message
        ByteBuffer b = ByteBuffer.allocate(messagesize) ;

        b.putInt(messagesize) ;
        b.putInt(messageType) ;

        byte[] greetingBytes = greeting.getBytes("UTF-8") ;
        b.putInt(greetingBytes.length) ;
        b.put(greetingBytes) ;

        byte[] hostStringBytes = hostString.getBytes("UTF-8") ;
        b.putInt(hostStringBytes.length) ;
        b.put(hostStringBytes) ;

        b.putInt(hostPort) ;

        b.flip() ; // make it ready for reading

        return b ;
    }

}
