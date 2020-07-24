package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.*;
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


    public Void call() {

        int messagesize = readBuffer.getInt() ;
        // LOG.info("Received message of size " + messagesize) ;
        int messageType = readBuffer.getInt() ;

        // LOG.info("Received message type " + messageType) ;
        try {
            if (messageType == 2) {

                PingMessage message = PingMessage.deserialize(readBuffer.rewind());

                LOG.info("Received ping message from " + message.getServerId() + " seq :" + message.getSeqId());

                AckMessage resp = new AckMessage(message.getSeqId());

                ByteBuffer b = resp.serialize();

                peerClient.queueSendMessage(b);
            } else if (messageType == 4) {

                AppendEntriesMessage message = AppendEntriesMessage.deserialize(readBuffer.rewind());
                LOG.info("Received AppendEntries message from " + message.getLeaderId() + " seq :" + message.getSeqId());

                AppendEntriesResponse resp = new AppendEntriesResponse(message.getSeqId(), 1, true);
                ByteBuffer b = resp.serialize();
                peerClient.queueSendMessage(b);
            }

        } catch(Exception e) {
            LOG.error("Error processing message",e);
        }


        return null ;
    }
}
