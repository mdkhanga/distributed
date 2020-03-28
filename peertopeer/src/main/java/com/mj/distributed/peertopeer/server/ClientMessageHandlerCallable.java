package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.AckMessage;
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
    PeerClient peerClient ;

    Logger LOG  = LoggerFactory.getLogger(ClientMessageHandlerCallable.class) ;

    public ClientMessageHandlerCallable(PeerClient p, SocketChannel s , ByteBuffer b) {

        socketChannel = s ;
        readBuffer = b ;
        peerClient = p ;

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

            AckMessage resp = new AckMessage(message.getSeqId()) ;

            ByteBuffer b = resp.serialize() ;
            b.flip() ;

            peerClient.queueSendMessage(b);

        }




        return null ;
    }
}
