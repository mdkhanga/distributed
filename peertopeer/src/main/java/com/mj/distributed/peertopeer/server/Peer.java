package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.Message;
import com.mj.distributed.model.Member;

public interface Peer {

    void queueSendMessage(Message m);

    void onReceiveMessage(Message m) ;

    boolean active() ;

    Member member() ;

    void shutdown() ;

}
