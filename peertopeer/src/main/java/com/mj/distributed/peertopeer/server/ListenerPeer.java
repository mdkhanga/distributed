package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.Message;
import com.mj.distributed.model.Member;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

public class ListenerPeer implements Peer {

    private final Member member ; // member we are are connected to
    private final SocketChannel socketChannel ;
    private final boolean active = true ;
    private final Queue<ByteBuffer> writeQueue = new ConcurrentLinkedDeque<>() ;

    public ListenerPeer(Member m, SocketChannel sc) {
        member = m ;
        socketChannel = sc ;
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
