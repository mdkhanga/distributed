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

        /* selector = Selector.open() ;

        clientChannel = SocketChannel.open();
        clientChannel.configureBlocking(false);

        clientChannel.connect(new InetSocketAddress(remoteHost, remotePort));

        clientChannel.register(selector, SelectionKey.OP_CONNECT) ;

        PeerClientCallable peerClientCallable = new PeerClientCallable(this) ;
        peerClientExecutor.submit(peerClientCallable) ; */

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

        /* synchronized (writeQueue) {
            writeQueue.add(b);
        }

        selector.wakeup() ; */

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

    public class PeerClientCallable implements Callable<Void> {

        private PeerClient peerClient ;

        PeerClientCallable(PeerClient p) {

            peerClient = p ;
        }


        @Override
        public Void call() throws Exception {

            try {

            /* selector = Selector.open() ;

            clientChannel = SocketChannel.open();
            clientChannel.configureBlocking(false);

            clientChannel.connect(new InetSocketAddress(remoteHost, remotePort));

            clientChannel.register(selector, SelectionKey.OP_CONNECT) ; */

                int i = 0;

                // HelloMessage m = new HelloMessage(peerServer.getBindHost(),peerServer.getBindPort()) ;
                // writeQueue.add(m.serialize());

                while (true) {


                    synchronized (writeQueue) {
                        if (clientChannel.isConnected() && writeQueue.peek() != null) {
                            clientChannel.register(selector, SelectionKey.OP_WRITE);
                            // LOG.info("Connected and soemthing to write");
                        } else {
                            // LOG.info("Connected and nothing to write");
                        }
                    }

                    // LOG.info("before select");
                    selector.select();
                    // LOG.info("after select");

                    Iterator<SelectionKey> skeys = selector.selectedKeys().iterator();

                    while (skeys.hasNext()) {
                        SelectionKey key = (SelectionKey) skeys.next();
                        skeys.remove();

                        if (!key.isValid()) {
                            LOG.info("key is not valid");
                            continue;
                        }

                        // System.out.println("We have a valid key") ;
                        // Check what event is available and deal with it
                        if (key.isConnectable()) {
                            // LOG.info("trying to conect") ;
                            finishConnection(key);
                        } else if (key.isReadable()) {
                            // LOG.info("trying to read") ;
                            read(key);
                            // done = true ;
                        } else if (key.isWritable()) {
                            // LOG.info("trying to write") ;
                            write(key);
                        } else {
                            System.out.println("not handled key");

                        }
                    }

                    ++i;

                }

            } catch(Exception e) {
                LOG.error("Error in peerclient",e);
            }

            return null ;
        }

        private void finishConnection(SelectionKey key) throws IOException {

            clientChannel.finishConnect() ;
            key.interestOps(SelectionKey.OP_WRITE) ;

            // peerServer.addPeer(clientChannel, remoteHost, remotePort);
            peerServer.addPeer(clientChannel, new CallerPeer(new Member(remoteHost, remotePort), peerClient));
            LOG.info("finished connection");
        }

        private void write(SelectionKey key) throws IOException {

            ByteBuffer b ;

            synchronized (writeQueue) {
                b = writeQueue.poll();
            }

            if (b != null) {
               // LOG.info("we got b to write") ;
            } else {

               // LOG.info("b is null could not get b to write") ;
                return ;
            }

            int n = clientChannel.write(b) ;
           // LOG.info("Wrote bytes " + n) ;
            int totalbyteswritten = n ;
            while (n > 0 && b.remaining() > 0) {
                n = clientChannel.write(b) ;
             //   LOG.info("Wrote bytes " + n) ;
                totalbyteswritten = totalbyteswritten + n ;

            }


           // LOG.info("Wrote bytes " + totalbyteswritten) ;

            key.interestOps(SelectionKey.OP_READ) ;
        }

        public void read(SelectionKey key) throws IOException {

            readBuf.clear() ;

            try {

                int numread = clientChannel.read(readBuf);
                int totalread = numread;
                while (numread > 0) {

                    numread = clientChannel.read(readBuf);

                    if (numread <= 0) {
                        break;
                    }
                    totalread = totalread + numread;


                }

               if (numread < 0) {

                    clientChannel.close();
                    key.cancel();
                }

                readBuf.rewind();



                PeerServer.inBoundMessageCreator.submit(clientChannel, readBuf, totalread);

            } catch (IOException e) {
                clientChannel.close() ;
                key.cancel();
                String s = remoteHost +":"+remotePort ;
                LOG.info(s + " has left the cluster") ;
                // peerServer.removePeer(s);

            }

        }

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