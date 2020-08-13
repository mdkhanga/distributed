package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.AckMessage;
import com.mj.distributed.message.AppendEntriesResponse;
import com.mj.distributed.message.HelloMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Callable;

public class ServerMessageHandlerCallable implements Callable {

    SocketChannel socketChannel ;
    ByteBuffer readBuffer ;

    Logger LOG  = LoggerFactory.getLogger(ServerMessageHandlerCallable.class) ;

    public ServerMessageHandlerCallable(SocketChannel s , ByteBuffer b) {

        socketChannel = s ;
        readBuffer = b ;

    }


    public Void call() {

        int messagesize = readBuffer.getInt() ;
        // LOG.info("Received message of size " + messagesize) ;
        int messageType = readBuffer.getInt() ;
        if (messageType == 1) {

            LOG.info("Received a hello message") ;
            HelloMessage message = HelloMessage.deserialize(readBuffer.rewind()) ;
            PeerServer.peerServer.addPeer(message.getHostString()+":"+message.getHostPort());
            PeerData d = PeerServer.peerServer.getPeerData(socketChannel) ;
            d.setHostString(message.getHostString());
            d.setPort(message.getHostPort());

        } else if (messageType == 3) {

            // LOG.info("Received a Ack message") ;
            AckMessage message = AckMessage.deserialize(readBuffer.rewind());
            PeerData d = PeerServer.peerServer.getPeerData(socketChannel);
            LOG.info("Received ack message from " + d.getHostString() + ":" + d.getPort() + " with seq " + message.getSeqOfMessageAcked());
        } else if (messageType == 5) {
            AppendEntriesResponse message = AppendEntriesResponse.deserialize(readBuffer.rewind());
                PeerData d = PeerServer.peerServer.getPeerData(socketChannel);
                int index = d.getIndexAcked(message.getSeqOfMessageAcked());
                if (index >= 0) {
                    // LOG.info("updating ack count") ;
                    PeerServer.peerServer.updateIndexAckCount(index);
                } else {
                    // LOG.info("Not updating ack count") ;
                }

            LOG.info("Received an appendEntriesResponse message from " + d.getHostString() + ":" + d.getPort() + " with seq " + message.getSeqOfMessageAcked());
        } else {
            LOG.info("Received message of unknown type " + messageType);
        }

        return null ;
    }
}
