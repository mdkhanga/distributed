package com.mj.distributed.peertopeer.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class InBoundMessageCreator {


    ExecutorService  inBoundMessageParser = Executors.newFixedThreadPool(1) ;

    // partial messages
    // who message did not arrive in the last read
    ConcurrentHashMap<SocketChannel,ByteBuffer> partialMessagesforChannel = new ConcurrentHashMap() ; // value could be a queue/list of bufferes

    Logger LOG  = LoggerFactory.getLogger(InBoundMessageCreator.class) ;

    PeerServer peerServer ;
    InBoundMessageHandler inBoundMessageHandler ;

    public InBoundMessageCreator() {

        inBoundMessageHandler = new InBoundMessageHandler() ;

    }

    public void submit(SocketChannel s, ByteBuffer b, int bytesinbuffer,Callable c) {

        // make a copy of the ByteBuffer
        ByteBuffer newBuffer = ByteBuffer.allocate(bytesinbuffer);
        System.arraycopy(b.array(),0,newBuffer.array(),0,bytesinbuffer);

        inBoundMessageParser.submit(new InBoundMessageReader(s,newBuffer,bytesinbuffer,c)) ;
    }

    public class InBoundMessageReader implements Callable {

        SocketChannel socketChannel ;
        ByteBuffer readBuffer ;
        int numbytes ;
        Callable handler ;

        public InBoundMessageReader(SocketChannel s, ByteBuffer b,int numbyt,Callable c) {

            socketChannel = s ;
            readBuffer = b ;
            numbytes = numbyt ;
            handler = c ;

        }

        public Void call() {

            boolean prevreadpartial = false ;
            int messagesize ;


            if (!prevreadpartial) {
                messagesize = readBuffer.getInt();
                int messageBytesRead = numbytes -4 ;


                if (messagesize == messageBytesRead) {

                    // We have the full message

                    readBuffer.rewind();
                    inBoundMessageHandler.submit(handler);
                } else if (messageBytesRead < messagesize) {
                    LOG.info("Did not receive full message received:" + messageBytesRead + " expected: "+messagesize);
                } else {

                    LOG.info("more than 1 message " + numbytes);
                }
            } else {

                // start reading and adding to partial message from last read


            }



            return null ;
        }

    }


}
