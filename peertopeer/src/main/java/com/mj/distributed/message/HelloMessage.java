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

    private static Logger LOG  = LoggerFactory.getLogger(HelloMessage.class) ;

    public HelloMessage(String host,int port) {

        this.hostString = host ;
        this.hostPort = port ;

        /*
        try {
            recordsize = 4 + greeting.getBytes("UTF-8").length + 4+ "111.111.111.111".getBytes("UTF-8").length + 4;
        } catch(UnsupportedEncodingException e) {
          LOG.error("Error getting size",e)  ;
          recordsize = 66 ; // 4+4+10+4+30+4
        } */

    }

    public String getHostString() {
        return hostString ;
    }

    public int getHostPort() {
        return hostPort ;
    }


    /**
     *
     *
     *
     */
    public ByteBuffer serialize() throws Exception {

        // int messagesize = recordsize+4 ; // len of record + 4. we write len in message

        byte[] greetingBytes = greeting.getBytes("UTF-8") ;
        byte[] hostStringBytes = hostString.getBytes("UTF-8") ;

        // messagesize+messagetype+greeting.len+greeting+hoststring.len+hoststring+port
        int messagesize = 4+4+4+greetingBytes.length+4+hostStringBytes.length+4 ;

        ByteBuffer b = ByteBuffer.allocate(messagesize) ;

        b.putInt(messagesize) ;
        b.putInt(messageType) ;


        b.putInt(greetingBytes.length) ;
        b.put(greetingBytes) ;


        b.putInt(hostStringBytes.length) ;
        b.put(hostStringBytes) ;

        b.putInt(hostPort) ;

        b.flip() ; // make it ready for reading

        return b ;
    }

    public static HelloMessage deserialize(ByteBuffer readBuffer) {

        int messagesize = readBuffer.getInt() ;
        LOG.info("Received message of size " + messagesize) ;
        int messageType = readBuffer.getInt() ;
        if (messageType != 1) {

            throw new RuntimeException("Message is not the expected type HelloMessage") ;
        }

        LOG.info("Received a hello message") ;
        int greetingSize = readBuffer.getInt() ;
        byte[] greetingBytes = new byte[greetingSize] ;
        readBuffer.get(greetingBytes,0,greetingSize) ;
        LOG.info("text greeing "+new String(greetingBytes)) ;


        int hostStringSize = readBuffer.getInt() ;
        byte[] hostStringBytes = new byte[hostStringSize] ;
        readBuffer.get(hostStringBytes,0,hostStringSize) ;
        String hostString = new String(hostStringBytes) ;
        LOG.info("from host "+hostString) ;

        int port = readBuffer.getInt() ;
        LOG.info("and port "+port) ;



        return new HelloMessage(hostString,port) ;
    }

}
