package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.HelloMessage;
import com.mj.distributed.message.PingMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Callable;

public class ClientMessageHandlerCallable implements Callable {

    SocketChannel socketChannel ;
    ByteBuffer readBuffer ;

    Logger LOG  = LoggerFactory.getLogger(ClientMessageHandlerCallable.class) ;

    public ClientMessageHandlerCallable(SocketChannel s , ByteBuffer b) {

        socketChannel = s ;
        readBuffer = b ;

    }


    public Void call() throws Exception{

        int messagesize = readBuffer.getInt() ;
        // LOG.info("Received message of size " + messagesize) ;
        int messageType = readBuffer.getInt() ;

        // LOG.info("Received message type " + messageType) ;
        if (messageType == 2) {

            // LOG.info("Received a Ping message") ;
            PingMessage message = PingMessage.deserialize(readBuffer.rewind()) ;

            LOG.info("Received ping message from "+message.getServerId() + " seq :" + message.getSeqId()) ;


        }




        return null ;
    }
}
