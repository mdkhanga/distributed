package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.*;
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
        int messageType = readBuffer.getInt() ;

        // LOG.info("Received message of size " + messagesize) ;
        // LOG.info("Received message type " + messageType) ;

        try {

            if (messageType == MessageType.Hello.value()) {

                LOG.info("Received a hello message");
                HelloMessage message = HelloMessage.deserialize(readBuffer.rewind());
                // PeerServer.peerServer.addPeer(message.getHostString()+":"+message.getHostPort());
                PeerData d = PeerServer.peerServer.getPeerData(socketChannel);
                d.setHostString(message.getHostString());
                d.setPort(message.getHostPort());
                PeerServer.peerServer.addPeer(message.getHostString(), message.getHostPort());

            } else if (messageType == MessageType.Ack.value()) {

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

                // LOG.info("Received an appendEntriesResponse message from " + d.getHostString() + ":" + d.getPort()
                //
                //+ " with seq " + message.getSeqOfMessageAcked());
            } else if (messageType == MessageType.RequestVote.value()) {

                RequestVoteMessage message = RequestVoteMessage.deserialize(readBuffer.rewind());

                LOG.info("Received a request vote message from "+ message.getCandidateHost() + ":" + message.getCandidatePort());

                /* RequestVoteResponseMessage requestVoteResponseMessage = new RequestVoteResponseMessage(
                        message.getTerm(),
                        message.getCandidateId(),
                        true);

                PeerServer.peerServer.queueSendMessage(socketChannel, requestVoteResponseMessage); */

            } else {
                LOG.info("Received message of unknown type " + messageType);
            }

        } catch(Exception e) {
            LOG.error("Error deserializing message ",e);
        }

        return null ;
    }
}
