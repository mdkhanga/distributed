package com.mj.distributed.peertopeer.server;

import com.mj.distributed.com.mj.distributed.tcp.nio.NioListener;
import com.mj.distributed.com.mj.distributed.tcp.nio.NioListenerConsumer;
import com.mj.distributed.message.AppendEntriesMessage;
import com.mj.distributed.message.ClusterInfoMessage;
import com.mj.distributed.message.HelloMessage;
import com.mj.distributed.model.LogEntry;
import com.mj.distributed.message.Message;
import com.mj.distributed.model.ClusterInfo;
import com.mj.distributed.model.Member;
import com.mj.distributed.model.RaftState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.net.InetSocketAddress;
import java.nio.*;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PeerServer implements NioListenerConsumer {

    private AtomicInteger serverId ;


    // private volatile ConcurrentHashMap<SocketChannel,PeerData> channelPeerMap = new ConcurrentHashMap<>() ;

    private final ConcurrentMap<Member, Peer> connectedMembersMap = new ConcurrentHashMap<>() ;
    private volatile ConcurrentMap<Member, PeerData> memberPeerDataMap = new ConcurrentHashMap<>() ;
    // reverse index
    private final ConcurrentMap<SocketChannel, Peer> socketChannelPeerMap = new ConcurrentHashMap<>() ;

   // private ConcurrentSkipListSet<String> members = new ConcurrentSkipListSet<>() ;

    private volatile ClusterInfo clusterInfo = new ClusterInfo();

    Integer x = 0 ;

    private Logger LOG  = LoggerFactory.getLogger(PeerServer.class) ;

    private String bindHost = "localhost" ;
    private int bindPort ;

    Selector selector ;
    private NioListener listener;

    public static InBoundMessageCreator inBoundMessageCreator = new InBoundMessageCreator() ;

    public static PeerServer peerServer ;

    List<byte[]> rlog = Collections.synchronizedList(new ArrayList<>());
    volatile AtomicInteger lastComittedIndex  = new AtomicInteger(-1) ;

    volatile ConcurrentHashMap<Integer,ConcurrentLinkedQueue<Integer>> ackCountMap =
            new ConcurrentHashMap<>(); // key = index, value = queue of commit responses

    AtomicInteger currentTerm = new AtomicInteger(0);

    Long lastLeaderHeartBeatTs = 0L ;

    volatile boolean leaderElection = false ;

    volatile RaftState raftState = RaftState.follower ;

    ExecutorService peerServerExecutor = Executors.newFixedThreadPool(3) ;

    public PeerServer(int id, RaftState state) {

        serverId = new AtomicInteger(id) ;
        bindPort = 5000+serverId.get() ;
        // if (id == 1) {
        //    leader = true ;
        // }

        raftState = state ;
    }

    public void start(String[] seed) throws Exception {



        if (raftState.equals(RaftState.leader)) {

            // initiate connect to peers
            clusterInfo.addMember(new Member(bindHost, bindPort, true));

        }


        if (seed != null) {
            for (String s : seed) {


                String[] remoteaddrAndPort = s.split(":") ;

                LOG.info("Connecting to " + seed) ;
                PeerClient peer = new PeerClient(remoteaddrAndPort[0],Integer.parseInt(remoteaddrAndPort[1]),this);
                peer.start();
                HelloMessage m = new HelloMessage(peerServer.getBindHost(),peerServer.getBindPort());
                peer.queueSendMessage(m.serialize());
                LOG.info("Done write hello to q") ;

            }
        }

            Thread writerThread = new Thread(new ServerWriteRunnable());
            writerThread.start();

            Thread clientThread = new Thread(new ClientSimulator());
            clientThread.start();

            // accept() ;
        listener = new NioListener(bindHost, bindPort, this);
        listener.start();


    }

    public String getBindHost() {
        return bindHost ;
    }

    public int getBindPort() {
        return bindPort ;
    }

    public int getTerm() {
        return currentTerm.get();
    }

    public int incrementTerm() {
        return currentTerm.incrementAndGet();
    }

    public int getServerId() {
        return serverId.get();
    }

    public LogEntry getLastCommittedEntry() {
        if (lastComittedIndex.get() >= 0) {
            return new LogEntry(lastComittedIndex.get(),rlog.get(lastComittedIndex.get()));
        } else {
            return new LogEntry(lastComittedIndex.get(), new byte[1]);
        }
    }

    public boolean isLeaderElection() {
        return leaderElection;
    }

    public void setLeaderElection(boolean s) {
        leaderElection = true ;
    }

    public ClusterInfo getClusterInfo() {
            return clusterInfo ;
    }

    public void setClusterInfo(ClusterInfo c) {
        clusterInfo = c ;
    }

    public void queueSendMessage(SocketChannel c, Message m) throws Exception {

        Peer p = socketChannelPeerMap.get(c) ;


        p.queueSendMessage(m);


        /* synchronized(x) {
            x = 1 ;
        }

        selector.wakeup() ; */

    }

    public void setRaftState(RaftState state) {
        raftState = state ;
    }

    /*
    public void accept() throws IOException {


        LOG.info("Server :" + serverId + " listening on port :" + bindPort) ;

        // addPeer(bindHost+":"+bindPort) ;
        // ServerSocket s = new ServerSocket(port) ;
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open() ;
        serverSocketChannel.configureBlocking(false) ;

        serverSocketChannel.socket().bind(new InetSocketAddress("localhost",bindPort));

         selector = Selector.open();

        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT) ;


         try {

            while (true) {

                int y ;
            synchronized (x) {
                y = x ;
            }

                if (y == 1) {


                  connectedMembersMap.forEach((k,v) -> {

                       if (!(v instanceof ListenerPeer)) {
                           // from here (Listener) we can only send messages to CallerPeers
                           return ;
                       }

                       ListenerPeer listenerPeer = (ListenerPeer) v ;

                       try {
                           if (listenerPeer.peekMessageQueue() != null) {
                               SocketChannel s = listenerPeer.socketChannel() ;
                               s.register(selector, SelectionKey.OP_WRITE);
                           }
                           // selector.wakeup();
                       } catch (Exception e) {
                           LOG.error("error", e);
                       }


                   }) ;



                   synchronized (x) {
                       x = 0;
                   }
               }


                selector.select();
                // LOG.info("after select") ;

                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();

                while (keysIterator.hasNext()) {

                    SelectionKey key = keysIterator.next();
                    keysIterator.remove();


                    if (key.isAcceptable()) {

                        // LOG.info("Something to accept");
                        accept(key) ;


                    } else if (key.isReadable()) {

                        // LOG.info("Something to read") ;
                        read(key) ;


                    } else if (key.isWritable()) {

                        // LOG.info("Something to write") ;
                        write(key) ;


                    }


                }


            }

        } catch(IOException e) {
             LOG.error("Exception :",e) ;
        }

    }

    */
    private void accept(SelectionKey key) throws IOException {

        ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
        SocketChannel sc = ssc.accept();
        sc.configureBlocking(false);

        InetSocketAddress socketAddress = (InetSocketAddress)sc.getRemoteAddress() ;

        LOG.info("accepted connection from " + socketAddress.getHostString() +":" + socketAddress.getPort());

        // delay this to after we get hello
        //channelPeerMap.put(sc,new PeerData(socketAddress.getHostString())) ;

        sc.register(this.selector, SelectionKey.OP_READ);
    }

    private void read(SelectionKey key) throws IOException {

        SocketChannel sc = (SocketChannel) key.channel();
        ByteBuffer readBuffer = ByteBuffer.allocate(8192);

        int numread = 0;
        int totalread = 0;
        try {

            numread = sc.read(readBuffer);
            totalread = numread;
            while (numread > 0) {
                numread = sc.read(readBuffer);

                totalread = totalread + numread;


            }

            if (numread == -1) {
                // Remote entity shut the socket down cleanly. Do the
                // same from our end and cancel the channel.
                // key.channel().close();
                // key.cancel();
                throw new IOException("Read returned -1. Channel closed by client.") ;

            }
        } catch(IOException e) {

            sc.close() ;
            key.cancel() ;

            /* PeerData p = channelPeerMap.get(sc) ;

            LOG.info(p.getHostString() +":" +p.getPort() + " has left the cluster") ;
            channelPeerMap.remove(sc) ;
            removePeer(p.getHostString(), p.getPort()); */
            removePeer(sc);
        }

        // System.out.println("Read :" + numread + " " + new String(readBuffer.array()));
        readBuffer.rewind() ;

        /*  inBoundMessageCreator.submit(sc,readBuffer,totalread,new ServerMessageHandlerCallable(sc,
                readBuffer)); */

        inBoundMessageCreator.submit(sc,readBuffer,totalread);

    }

    /*
    private void write(SelectionKey key) throws IOException {

        SocketChannel sc = (SocketChannel) key.channel();

        // ByteBuffer towrite = channelPeerMap.get(sc).getNextWriteBuffer() ;
        ByteBuffer towrite = socketChannelPeerMap.get(sc).getNextQueuedMessage() ;

        if (towrite == null) {
            LOG.warn("Write queue is emptyy") ;
            key.interestOps(SelectionKey.OP_READ);
            return ;
        }


        int n = sc.write(towrite);
        while (n > 0 && towrite.remaining() > 0) {
            n = sc.write(towrite);
           // LOG.info("Server wrote bytes "+n) ;
        }

        key.interestOps(SelectionKey.OP_READ);

    } */

    public InBoundMessageCreator getInBoundMessageCreator() {
        return inBoundMessageCreator;
    }

    public boolean processLogEntry(LogEntry e, int prevIndex, int lastComittedIndex) {


        boolean ret = true ;

        if (e != null) {

            // LOG.info("We have an entry") ;

            int position = e.getIndex();
            byte[] data = e.getEntry();

            // LOG.info("Received log entry " + ByteBuffer.wrap(data).getInt());

            int expectedNextEntry = rlog.size();

            // LOG.info("prev = " + prevIndex + " expectedNext = " + expectedNextEntry) ;
            if (prevIndex + 1 == expectedNextEntry) {
                synchronized (rlog) {
                    rlog.add(data);
                    // LOG.info("added to rlog") ;
                }
                ret = true ;
                if (lastComittedIndex <= expectedNextEntry) {
                    this.lastComittedIndex.set(lastComittedIndex);
                }
            } else {
                ret = false ;
                // LOG.info("did not add to rlog return false") ;
            }
        } else {
            // LOG.info("No entry") ;
        }

        if (lastComittedIndex < rlog.size()) {
            this.lastComittedIndex.set(lastComittedIndex);
        }

        return ret ;
    }


    public static void main(String args[]) throws Exception {

        if (args.length == 0 ) {
            System.out.println("Need at least 1 argurment") ;
        }

        int serverId = Integer.parseInt(args[0]) ;

        int size = args.length   ;

        RaftState state = RaftState.follower ;

        String[] seeds = null ;

        if (size > 1) {
            seeds = new String[args.length-1] ;
            int j = 0 ;
            for (int i = 1 ; i < size ; i++) {
                seeds[j] = args[i] ;
                ++j ;
            }

        } else {
            state = RaftState.leader;
        }

        System.out.println("Starting server with serverId:" + serverId) ;

        peerServer = new PeerServer(serverId, state) ;
        peerServer.start(seeds) ;
    }

    public void addedConnection(SocketChannel s) {

        // do nothing this we get hello message

    }

    public void droppedConnection(SocketChannel s) {

        removePeer(s);

    }

    public void consumeMessage(SocketChannel s, int numBytes, ByteBuffer b) {

        inBoundMessageCreator.submit(s, b, numBytes);
    }

    public void setLastLeaderHeartBeatTs(long ts) {
        synchronized (lastLeaderHeartBeatTs) {
            lastLeaderHeartBeatTs = ts;
        }
    }

    public long getlastLeaderHeartBeatts() {
        synchronized (lastLeaderHeartBeatTs) {
            return lastLeaderHeartBeatTs ;
        }
    }


    public class ServerWriteRunnable implements Runnable {


        public void run() {

            AtomicInteger count = new AtomicInteger(1);

            while (true) {

                try {
                    Thread.sleep(200);
                    // Thread.sleep(1000);


                    if (raftState.equals(RaftState.leader)) {

                        // channelPeerMap.forEach((k, v) -> {
                        connectedMembersMap.forEach((k, v) -> {

                            try {

                                sendAppendEntriesMessage(v);

                                if (count.get() % 60 == 0) {
                                    logRlog();
                                    sendClusterInfoMessage(v);
                                }


                            } catch (Exception e) {
                                LOG.error("error", e);
                            }

                        });

                        synchronized (x) {
                            x = 1;
                        }

                        selector.wakeup();

                    }
                     else if (raftState.equals(RaftState.candidate)) {

                        // test
                        // LOG.info("We need a leader Election. No heartBeat in ") ;
                        if (!peerServer.isLeaderElection()) {
                            LOG.info("Starting a leader election ") ;
                            peerServer.setLeaderElection(true);

                            LOG.info("Starting leader election");
                            LeaderElection leaderElection = new LeaderElection(peerServer);
                            peerServerExecutor.submit(leaderElection);

                        }



                } else if (raftState.equals(RaftState.follower)) {


                    long timeSinceLastLeadetBeat = System.currentTimeMillis() -
                            peerServer.getlastLeaderHeartBeatts();
                    if (peerServer.getlastLeaderHeartBeatts() > 0 && timeSinceLastLeadetBeat > 1000) {
                        LOG.info("We need a leader Election. No heartBeat in ") ;
                        raftState = RaftState.candidate;
                    }

                }

                    count.incrementAndGet();

                } catch (Exception e) {
                    // System.out.println(e) ;
                }
            }

        }

    }

    // public void sendAppendEntriesMessage(PeerData v) throws Exception {
    public void sendAppendEntriesMessage(Peer peer) throws Exception {

        Member m = peer.member();
        PeerData v = memberPeerDataMap.get(m);

        AppendEntriesMessage p = new AppendEntriesMessage(serverId.get(),
                v.getNextSeq(),
                rlog.size()-1,
                lastComittedIndex.get());

        int index = getIndexToReplicate(v) ;

        // LOG.info("Index to replicate "+ index);

        if (index >= 0 && index < rlog.size()) {
            byte[] data = rlog.get(index);
            // LOG.info("Replicating ..." + ByteBuffer.wrap(data).getInt());
            LogEntry entry = new LogEntry(index, data);
            p.addLogEntry(entry);
            p.setPrevIndex(index-1);
        }

        // v.addMessageForPeer(p);

        v.addToSeqIdIndexMap(p);


        peer.queueSendMessage(p);
        ackCountMap.put(index, new ConcurrentLinkedQueue<Integer>());

    }

    // public void sendClusterInfoMessage(PeerData v) throws Exception {
    public void sendClusterInfoMessage(Peer peer) throws Exception {
        ClusterInfoMessage cm = new ClusterInfoMessage(clusterInfo);
        // v.addMessageForPeer(cm);
        peer.queueSendMessage(cm);

    }


    public void removePeer(SocketChannel sc) {

        Peer p = socketChannelPeerMap.get(sc) ;

        Member m = null ;

        if (p != null) {
            m = p.member() ;
        }

        socketChannelPeerMap.remove(sc) ;
        connectedMembersMap.remove(m);
        memberPeerDataMap.remove(m);
        clusterInfo.removeMember(m.getHostString(), m.getPort());

    }

    public void addPeer(SocketChannel sc, Peer p) {

        socketChannelPeerMap.put(sc, p) ;
        connectedMembersMap.put(p.member(), p);
        memberPeerDataMap.put(p.member(), new PeerData(p.member().getHostString(), p.member().getPort()));

    }

    public void addPeer(SocketChannel sc, String hostString, int port) {
        Member m = new Member(hostString, port, false);
        ListenerPeer l = new ListenerPeer(listener, m, sc) ;
        clusterInfo.addMember(m);
        connectedMembersMap.put(m, l) ;
        memberPeerDataMap.put(m, new PeerData(hostString, port));
        socketChannelPeerMap.put(sc, l);

    }

    public Peer getPeer(SocketChannel s) {
        return socketChannelPeerMap.get(s) ;
    }

    public PeerData getPeerData(SocketChannel s) {
        Peer p = getPeer(s) ;
        return memberPeerDataMap.get(p.member());
    }

    /* public void logCluster() throws Exception {

        StringBuilder sb = new StringBuilder("Cluster members [") ;

        LOG.info("number of members "+members.size()) ;

        members.forEach((k)->{

            try {

                sb.append(k) ;
                sb.append(",") ;


            } catch(Exception e) {
                LOG.error("Error getting remote address ",e) ;
            }

        });

        sb.append("]") ;

        LOG.info(sb.toString()) ;

    } */

    public void logRlog() throws Exception {

        StringBuilder sb = new StringBuilder("Replicated Log [") ;

        // LOG.info("number of entries "+rlog.size()) ;

        rlog.forEach((k)->{

            try {

                sb.append(ByteBuffer.wrap(k).getInt()) ;
                sb.append(",") ;


            } catch(Exception e) {
                LOG.error("Error getting remote address ",e) ;
            }

        });

        sb.append("]") ;

        LOG.info(sb.toString()) ;
        LOG.info("Committed index = " + String.valueOf(lastComittedIndex));

    }

    public void consumeMessage(Message message) {


    }

    public class ClientSimulator implements Runnable {

        Random r = new Random() ;

        public void run()  {

            if (!raftState.equals(RaftState.leader)) {
                LOG.info("Not a leader not starting client thread");
                return ;
            }

            try {

                Thread.sleep(30000);
                int i = 0;

                while (i < 10) {

                    Thread.sleep(10000);
                    int value = (int)(Math.random()*100);
                    rlog.add(ByteBuffer.allocate(4).putInt(value).array());
                    i++;

                }

            } catch(Exception e) {
                LOG.error("Error in client simulator thread",e);
            }
        }

    }

    private int getIndexToReplicate(PeerData d) {

        int maxIndex = rlog.size() - 1  ;

        return d.getNextIndexToReplicate(maxIndex) ;

    }

    public void updateIndexAckCount(int index) {

        if (lastComittedIndex.get() >= index) {
            ackCountMap.remove(index);
        }

        ConcurrentLinkedQueue<Integer> indexQueue = ackCountMap.get(index) ;
        if (indexQueue == null) {
            // already committed
            // LOG.info("No Ack queue. Already comitted") ;
            return ;
        }

        indexQueue.add(1) ;
        // LOG.info("Incrementing index q") ;

        // if (indexQueue.size() >= members.size()/2 ) {
        if (indexQueue.size() >= connectedMembersMap.size()/2 ) {
            lastComittedIndex.set(index) ;
            ackCountMap.remove(index) ;
            // LOG.info("Got enough votes " + indexQueue.size() + "---" + connectedMembersMap.size()/2 );
        } else {
           //  LOG.info("Not enough votes " + indexQueue.size() + "---" + connectedMembersMap.size()/2 );
        }
    }
}
