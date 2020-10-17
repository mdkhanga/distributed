package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.AppendEntriesMessage;
import com.mj.distributed.message.ClusterInfoMessage;
import com.mj.distributed.message.HelloMessage;
import com.mj.distributed.model.LogEntry;
import com.mj.distributed.message.Message;
import com.mj.distributed.model.ClusterInfo;
import com.mj.distributed.model.Member;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class PeerServer {

    private AtomicInteger serverId ;
    private boolean leader ;
    private Set<PeerClient> peerSet = new HashSet<PeerClient>();
    private ConcurrentHashMap<SocketChannel,PeerData> channelPeerMap = new ConcurrentHashMap<>() ;


   // private ConcurrentSkipListSet<String> members = new ConcurrentSkipListSet<>() ;

    ClusterInfo clusterInfo = new ClusterInfo();

    Integer x = 0 ;

    private Logger LOG  = LoggerFactory.getLogger(PeerServer.class) ;

    private String bindHost = "localhost" ;
    private int bindPort ;

    Selector selector ;

    public static InBoundMessageCreator inBoundMessageCreator = new InBoundMessageCreator() ;

    public static PeerServer peerServer ;

    List<byte[]> rlog = Collections.synchronizedList(new ArrayList<>());
    AtomicInteger lastComittedIndex  = new AtomicInteger(-1) ;

    ConcurrentHashMap<Integer,ConcurrentLinkedQueue<Integer>> ackCountMap =
            new ConcurrentHashMap<>(); // key = index, value = queue of commit responses

    AtomicInteger currentTerm = new AtomicInteger(0);

    Long lastLeaderHeartBeatTs = 0L ;

    public PeerServer(int id) {

        serverId = new AtomicInteger(id) ;
        bindPort = 5000+serverId.get() ;
        if (id == 1) {
            leader = true ;
        }


    }

    public void start(String[] seed) throws Exception {



        if (leader) {

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

        if (leader) {
            Thread writerThread = new Thread(new ServerWriteRunnable());
            writerThread.start();

            Thread clientThread = new Thread(new ClientSimulator());
            clientThread.start();
        }

        accept() ;


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

                if (x == 1) {
                    channelPeerMap.forEach((k, v) -> {

                        try {
                            if (v.peekWriteBuffer() != null) {
                                k.register(selector, SelectionKey.OP_WRITE);
                            }
                            // selector.wakeup();
                        } catch (Exception e) {
                            LOG.error("error", e);
                        }

                    });

                    synchronized (x) {
                        x = 0;
                    }
                }

                selector.select();

                Iterator<SelectionKey> keysIterator = selector.selectedKeys().iterator();

                while (keysIterator.hasNext()) {

                    SelectionKey key = keysIterator.next();
                    keysIterator.remove();


                    if (key.isAcceptable()) {

                        accept(key) ;


                    } else if (key.isReadable()) {

                        read(key) ;


                    } else if (key.isWritable()) {

                        write(key) ;


                    }


                }


            }

        } catch(IOException e) {
            LOG.error("Exception :",e) ;
        }

    }

    private void accept(SelectionKey key) throws IOException {

        ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
        SocketChannel sc = ssc.accept();
        sc.configureBlocking(false);

        InetSocketAddress socketAddress = (InetSocketAddress)sc.getRemoteAddress() ;

        LOG.info("accepted connection from " + socketAddress.getHostString());
        channelPeerMap.put(sc,new PeerData(socketAddress.getHostString())) ;


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

            PeerData p = channelPeerMap.get(sc) ;

            LOG.info(p.getHostString() +":" +p.getPort() + " has left the cluster") ;
            channelPeerMap.remove(sc) ;
            removePeer(p.getHostString(), p.getPort());
            // members.remove(p.getHostString()+":"+p.getPort()) ;
        }

        // System.out.println("Read :" + numread + " " + new String(readBuffer.array()));
        readBuffer.rewind() ;

        inBoundMessageCreator.submit(sc,readBuffer,totalread,new ServerMessageHandlerCallable(sc,
                readBuffer));

    }

    private void write(SelectionKey key) throws IOException {

        SocketChannel sc = (SocketChannel) key.channel();

        ByteBuffer towrite = channelPeerMap.get(sc).getNextWriteBuffer() ;

        if (towrite == null) {
            LOG.warn("Write queue is emptyy") ;
            key.interestOps(SelectionKey.OP_READ);
            return ;
        }


        int n = sc.write(towrite);
        while (n > 0 && towrite.remaining() > 0) {
            n = sc.write(towrite);
            LOG.info("Server wrote bytes "+n) ;
        }

        key.interestOps(SelectionKey.OP_READ);

    }

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

        String[] seeds = null ;

        if (size > 1) {
            seeds = new String[args.length-1] ;
            int j = 0 ;
            for (int i = 1 ; i < size ; i++) {
                seeds[j] = args[i] ;
                ++j ;
            }


        }

        System.out.println("Starting server with serverId:" + serverId) ;


        peerServer = new PeerServer(serverId) ;
        peerServer.start(seeds) ;
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

            AtomicInteger count = new AtomicInteger(1) ;

            while(true) {

                try {
                    Thread.sleep(200);

                    channelPeerMap.forEach((k,v)->{

                        try {

                            sendAppendEntriesMessage(v);

                            if (count.get() % 60 == 0) {
                                logRlog() ;
                                sendClusterInfoMessage(v);
                            }


                        } catch(Exception e) {
                            LOG.error("error" ,e) ;
                        }

                    });

                    synchronized (x) {
                        x =1 ;
                    }

                    selector.wakeup() ;
                   // logRlog() ;
                    count.incrementAndGet() ;

                } catch(Exception e) {
                    // System.out.println(e) ;
                }
            }

        }


    }

    public void sendAppendEntriesMessage(PeerData v) throws Exception {

        AppendEntriesMessage p = new AppendEntriesMessage(1,
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

        v.addMessageForPeer(p);
        ackCountMap.put(index, new ConcurrentLinkedQueue<Integer>());

    }

    public void sendClusterInfoMessage(PeerData v) throws Exception {
        ClusterInfoMessage cm = new ClusterInfoMessage(clusterInfo);
        v.addMessageForPeer(cm);

    }


     public void removePeer(String hostString, int port) {
       clusterInfo.removeMember(hostString, port);
    }

    public void addPeer(String hostString, int port) {
        clusterInfo.addMember(new Member(hostString, port, false));
    }

    public PeerData getPeerData(SocketChannel s) {
        return channelPeerMap.get(s) ;
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
        if (indexQueue.size() >= channelPeerMap.size()/2 ) {
            lastComittedIndex.set(index) ;
            ackCountMap.remove(index) ;

        }
    }
}
