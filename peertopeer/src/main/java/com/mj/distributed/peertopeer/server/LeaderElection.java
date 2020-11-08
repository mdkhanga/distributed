package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.HelloMessage;
import com.mj.distributed.message.RequestVoteMessage;
import com.mj.distributed.message.RequestVoteResponseMessage;
import com.mj.distributed.model.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LeaderElection implements Runnable {

    PeerServer server;

    long electionStartTime ;
    Logger LOG = LoggerFactory.getLogger(LeaderElection.class);
    LeaderElection(PeerServer p) {
        server = p ;
    }

    public void run()  {

        // start election timer
        electionStartTime = System.currentTimeMillis() ;

        LOG.info("Started leader election at "+ electionStartTime);

        // get the list of available servers
        List<Member> members = server.getClusterInfo().getMembers() ;

        synchronized (members) {
            members.forEach((m) -> {

                try {


                    if (m.isLeader()) {
                        LOG.info("skipping "+m.getHostString() + ":" + m.getPort()) ;
                        return ;
                    }

                    if (m.getHostString().equals(server.getBindHost()) && m.getPort() == server.getBindPort()) {
                        LOG.info("skipping self") ;
                        return ;
                    }

                    if (m.getPort() == 5002) {
                        return ;
                    }

                    LOG.info("Sending request vote message to "+m.getHostString()+":"+m.getPort()) ;
                    PeerClient pc = new PeerClient(m.getHostString(), m.getPort(), server);
                    pc.start();

                    HelloMessage hm = new HelloMessage(server.getBindHost(), server.getBindPort());
                    pc.queueSendMessage(hm.serialize());

                    RequestVoteMessage rv = new RequestVoteMessage(
                            server.incrementTerm(),
                            server.getServerId(),
                            server.getBindHost(),
                            server.getBindPort(),
                            server.getLastCommittedEntry());
                    pc.queueSendMessage(rv.serialize());
                } catch (Exception e) {
                    LOG.error("Error starting client in leader election", e);
                }

            });
        }


        // start/get peerclients for each if necessary and connect

        // send request vote message to each

        // while( election not timed out && some else did not become leader)

            // if (checkVoteCount > majority)

                // done we

                // become leader and start sending heartbeat


                // exit



    }

    public void requestVote(RequestVoteMessage message) {

        // if term < currentTerm
        // response = false

        // if votedFor == null or candidate Id

                //  candidate lastLogIndex == this.lastlogIndex and candidate.lastEntry = this.lastEntry

                // or

                // candiated.lastLogIndex > this.lastLogIndex

                // response = true
                // setVotedFor = candidate

        // response to candiate

    }

    public void requestVoteResponse(RequestVoteResponseMessage message) {

        // if term == election term and votedforId = this
        // increment vote count
    }

}
