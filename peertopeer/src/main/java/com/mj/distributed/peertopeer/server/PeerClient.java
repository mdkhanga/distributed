package com.mj.distributed.peertopeer.server;

import com.mj.distributed.com.mj.distributed.tcp.nio.NioCaller;
import com.mj.distributed.com.mj.distributed.tcp.nio.NioCallerConsumer;
import com.mj.distributed.message.HelloMessage;
import com.mj.distributed.model.ClusterInfo;
import com.mj.distributed.model.LogEntry;
import com.mj.distributed.message.Message;
import com.mj.distributed.model.Member;
import org.slf4j.LoggerFactory ;
import org.slf4j.Logger ;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;



/**
 * Created by Manoj Khangaonkar on 10/5/2016.
 */
public class PeerClient implements NioCallerConsumer {

    // Socket peer;
    PeerServer peerServer;
    DataOutputStream dos;
    Integer peerServerId = -1;
    int remotePort; // port returned remote initiates the connection - other end of socket after our accept
    int remoteListenPort ; // if we initate connection, this is where we connect to
    InetAddress remoteIpAddress;

    String remoteHost ;
    SocketChannel clientChannel ;

    Selector selector ;

    ExecutorService peerClientExecutor = Executors.newFixedThreadPool(3) ;

    public volatile Queue<ByteBuffer> writeQueue = new ConcurrentLinkedDeque<ByteBuffer>() ;

    private ByteBuffer readBuf = ByteBuffer.allocate(8192)  ;

    // List<byte[]> rlog = Collections.synchronizedList(new ArrayList<>());
    // int lastComittedIndex  = -1 ;

    Logger LOG = LoggerFactory.getLogger(PeerClient.class);

    private NioCaller nioCaller ;


    private PeerClient() {

    }

    public PeerClient(String host, int port, PeerServer p) {

       this.remoteHost = host ;
       this.remotePort = port ;
       this.peerServer = p ;



    }


    public void start() throws Exception {

        nioCaller = new NioCaller(remoteHost, remotePort, this);
        nioCaller.start();

        PeerClientStatusCallable peerClientStatusCallable = new PeerClientStatusCallable();
        peerClientExecutor.submit(peerClientStatusCallable);

    }

    public boolean processLogEntry(LogEntry e, int prevIndex, int lastComittedIndex) {

        return peerServer.processLogEntry(e, prevIndex, lastComittedIndex);
    }

    public void sendMessage(Message m) {

        // message needs to be queue and sent by a writer thread
    }

    public void setLeaderHeartBeatTs(long ts) {
        peerServer.setLastLeaderHeartBeatTs(ts);
    }

    public void queueSendMessage(ByteBuffer b) {
        nioCaller.queueSendMessage(b);
    }

    public void addedConnection(SocketChannel s) {
        peerServer.addPeer(s, new CallerPeer(new Member(remoteHost, remotePort), this));
    }

    public void droppedConnection(SocketChannel s) {

    }

    public void consumeMessage(SocketChannel s, int numBytes, ByteBuffer b) {
        PeerServer.inBoundMessageCreator.submit(s, b, numBytes);
    }


    @Override
    public boolean equals(Object obj) {

        if (null == obj) {
            return false;
        }

        if (!(obj instanceof PeerClient)) {
            return false;
        }

        return peerServerId.equals(((PeerClient) obj).peerServerId);

    }

    @Override
    public int hashCode() {
        return peerServerId.hashCode();
    }

    public String getPeerServer() {
        return remoteIpAddress.toString()+":"+remoteListenPort;
    }


    public class PeerClientStatusCallable implements Callable<Void> {

        public Void call() throws Exception {

            int count = 0 ;

            boolean leaderElectionStarted = false ;

            LeaderElection leaderElection = null ;

            while(true) {

                Thread.sleep(10000);


                peerServer.logRlog();


            }
        }

    }

}