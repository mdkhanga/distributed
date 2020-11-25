package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.*;
import com.mj.distributed.model.LogEntry;
import com.mj.distributed.model.RaftState;
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

        // WARNING : 11142020
        // MIGHT BREAK CODE
        // commented read because rewind in InBoundMessage Creator was commented
        int messagesize = readBuffer.getInt() ;
        int messageType = readBuffer.getInt() ;

        // LOG.info("Received message of size " + messagesize) ;
        // LOG.info("Received message type " + messageType) ;

        try {

            if (messageType == MessageType.Hello.value()) {

                LOG.info("Received a hello message");
                HelloMessage message = HelloMessage.deserialize(readBuffer.rewind());
                /* PeerData d = PeerServer.peerServer.getPeerData(socketChannel);
                d.setHostString(message.getHostString());
                d.setPort(message.getHostPort()); */
                PeerServer.peerServer.addPeer(socketChannel, message.getHostString(), message.getHostPort());
                LOG.info("Registered peer " + message.getHostString() +":" + message.getHostPort());

            } else if (messageType == MessageType.Ack.value()) {

                AckMessage message = AckMessage.deserialize(readBuffer.rewind());
                Peer d = PeerServer.peerServer.getPeer(socketChannel);
                // LOG.info("Received ack message from " + d.member().getHostString() + ":" + d.member().getPort() + " with seq " + message.getSeqOfMessageAcked());
            } else if (messageType == 5) {
                AppendEntriesResponse message = AppendEntriesResponse.deserialize(readBuffer.rewind());
                PeerData d = PeerServer.peerServer.getPeerData(socketChannel);
                int index = d.getIndexAcked(message.getSeqOfMessageAcked());
                LOG.info("Got AppendEntries response from" + d.getHostString() + "  " + d.getPort()) ;


                if (index >= 0) {
                   // LOG.info("got index for seqId " + message.getSeqOfMessageAcked()) ;
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

                RequestVoteResponseMessage requestVoteResponseMessage = new RequestVoteResponseMessage(
                        message.getTerm(),
                        message.getCandidateId(),
                        true);

                LOG.info("Queueing response");
                PeerServer.peerServer.queueSendMessage(socketChannel, requestVoteResponseMessage);

            } else if (messageType == MessageType.AppendEntries.value()) {
                AppendEntriesMessage message = AppendEntriesMessage.deserialize(readBuffer.rewind());
                PeerData d = PeerServer.peerServer.getPeerData(socketChannel);

                LOG.info("Got append entries message "+ message.getLeaderId() + " " + d.getHostString() + " " + d.getPort());
                PeerServer.peerServer.setLastLeaderHeartBeatTs(System.currentTimeMillis());
                boolean entryResult = true ;
                LogEntry e = message.getLogEntry() ;
                entryResult = PeerServer.peerServer.processLogEntry(e,message.getPrevIndex(),message.getLeaderCommitIndex()) ;
                AppendEntriesResponse resp = new AppendEntriesResponse(message.getSeqId(), 1, entryResult);
                ByteBuffer b = resp.serialize();
                PeerServer.peerServer.queueSendMessage(socketChannel, resp);
            }  else if (messageType == MessageType.ClusterInfo.value()) {

                ClusterInfoMessage message = ClusterInfoMessage.deserialize(readBuffer.rewind()) ;
                LOG.info("Received clusterInfoMsg:" + message.toString());
                PeerServer.peerServer.setClusterInfo(message.getClusterInfo());
            } else if (messageType == MessageType.RequestVoteResponse.value()) {

                LOG.info("Received RequestVoteResponse Message") ;
                RequestVoteResponseMessage message = RequestVoteResponseMessage.deserialize(readBuffer.rewind());

                if (message.getVote()) {
                    LOG.info("Got vote. Won the election") ;
                    PeerServer.peerServer.setRaftState(RaftState.leader);
                } else {
                    LOG.info("Did not get vote. Lost the election");
                }


            }
            else {
                LOG.info("Received message of unknown type " + messageType);
            }

        } catch(Exception e) {
            LOG.error("Error deserializing message ",e);
        }

        return null ;
    }
}
