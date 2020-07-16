package com.mj.distributed.message;

import com.mj.distributed.peertopeer.server.PeerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
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

        byte[] greetingBytes = greeting.getBytes("UTF-8") ;
        byte[] hostStringBytes = hostString.getBytes("UTF-8") ;

        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);
        d.writeInt(messageType);
        d.writeInt(greetingBytes.length);
        d.write(greetingBytes);
        d.writeInt(hostStringBytes.length);
        d.write(hostStringBytes);
        d.writeInt(hostPort);

        byte[] helloMsgArray = b.toByteArray();

        ByteBuffer retBuffer = ByteBuffer.allocate(helloMsgArray.length+4);//

        retBuffer.putInt(helloMsgArray.length);
        retBuffer.put(helloMsgArray);

        retBuffer.flip() ; // make it ready for reading

        return retBuffer ;
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
