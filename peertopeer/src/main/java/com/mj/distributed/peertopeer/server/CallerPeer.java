package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.Message;
import com.mj.distributed.model.Member;

public class CallerPeer {

    private final Member member ; // member that we are connected to
    private final PeerClient peerClient ;
    private final boolean active = true ;

    public CallerPeer(Member m, PeerClient p) {
        member = m;
        peerClient = p;
    }

    public void queueSendMessage(Message m) {

    }

    public void onReceiveMessage(Message m) {

    }

    public boolean active() {

        return active;
    }

    public Member member() {

        return member;
    }

    public void shutdown() {


    }
}