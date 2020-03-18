package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.HelloMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class InBoundMessageHandler {


    ExecutorService messageHandlers = Executors.newCachedThreadPool() ;

    Logger LOG  = LoggerFactory.getLogger(InBoundMessageHandler.class) ;

    public InBoundMessageHandler() {


    }

    public void submit(Callable c) {

        // messageHandlers.submit(new InBoundMessageHandlerCallable(s,b)) ;
        messageHandlers.submit(c) ;

    }

    public class InBoundMessageHandlerCallable implements Callable {



        SocketChannel socketChannel ;
        ByteBuffer readBuffer ;

        public InBoundMessageHandlerCallable(SocketChannel s , ByteBuffer b) {

            socketChannel = s ;
            readBuffer = b ;

        }


        public Void call() {

            int messagesize = readBuffer.getInt() ;
            LOG.info("Received message of size " + messagesize) ;
            int messageType = readBuffer.getInt() ;
            if (messageType == 1) {

                LOG.info("Received a hello message") ;
                HelloMessage message = HelloMessage.deserialize(readBuffer.rewind()) ;
                PeerServer.peerServer.addPeer(message.getHostString(),message.getHostPort());
            }




            return null ;
        }

    }


}
