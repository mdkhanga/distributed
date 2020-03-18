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

        inBoundMessageParser.submit(new InBoundMessageReader(s,b,bytesinbuffer,c)) ;
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
                // LOG.info("Received message of size " + messagesize);


                if (messagesize == numbytes) {

                    // We have the full message

                    readBuffer.rewind();
                    // inBoundMessageHandler.submit(new ServerMessageHandlerCallable(socketChannel,readBuffer));
                    inBoundMessageHandler.submit(handler);
                } else if (messagesize < numbytes) {
                    LOG.info("Did not receive full message " + numbytes);
                } else {

                    LOG.info("more than 1 message " + numbytes);
                }
            } else {

                // start reading and adding to partial message from last read


            }

            /*
            int messageType = readBuffer.getInt() ;
            if (messageType == 1) {

                LOG.info("Received a hello message") ;
            }

            int greetingSize = readBuffer.getInt() ;
            byte[] greetingBytes = new byte[greetingSize] ;
            readBuffer.get(greetingBytes,0,greetingSize) ;
            LOG.info("text greeing "+new String(greetingBytes)) ;


            int hostStringSize = readBuffer.getInt() ;
            byte[] hostStringBytes = new byte[hostStringSize] ;
            readBuffer.get(hostStringBytes,0,hostStringSize) ;
            LOG.info("from host "+new String(hostStringBytes)) ;

            LOG.info("and port "+readBuffer.getInt()) ;
            */

            return null ;
        }

    }


}
