package com.mj.distributed.peertopeer.server;

import com.mj.distributed.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
/*
import java.net.ServerSocket;
import java.net.Socket;
*/
import java.net.SocketException;

import java.net.InetSocketAddress;
import java.nio.*;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PeerServer {

    private int serverId ;
    private boolean leader ;
    private Set<PeerClient> peerSet = new HashSet<PeerClient>();
    // private int[] seeds ;
    // List<PeerClient> peers = new ArrayList<PeerClient>();
    // private HashMap<SocketChannel,ByteBuffer> queuedWrites = new HashMap<SocketChannel,ByteBuffer>() ;
    private HashMap<SocketChannel,AtomicInteger> queuedWrites = new HashMap<SocketChannel,AtomicInteger>() ;

    Integer x = 0 ;

    private Logger LOG  = LoggerFactory.getLogger(PeerServer.class) ;

    private String bindHost = "localhost" ;
    private int bindPort ;

    Selector selector ;

    public PeerServer(int id) {

        serverId = id ;
        bindPort = 5000+serverId ;
        if (id == 1) {
            leader = true ;
        }
        // seeds = seed ;
    }

    public void start(String[] seed) throws Exception {



        if (leader) {

            // initiate connect to peers

        }




        if (seed != null) {
            for (String s : seed) {


                String[] remoteaddrAndPort = s.split(":") ;

                LOG.info("Connecting to " + seed) ;
                // Socket p = new Socket(remoteaddrAndPort[0], Integer.parseInt(remoteaddrAndPort[1]));
                PeerClient peer = new PeerClient(remoteaddrAndPort[0],Integer.parseInt(remoteaddrAndPort[1]),this);
                peer.start();

            }
        }


        Thread writerThread = new Thread(new ServerWriteRunnable()) ;
        writerThread.start();

        accept() ;


    }

    public String getBindHost() {
        return bindHost ;
    }

    public int getBindPort() {
        return bindPort ;
    }

    public void accept() throws IOException {


        LOG.info("Server :" + serverId + " listening on port :" + bindPort) ;
        // ServerSocket s = new ServerSocket(port) ;
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open() ;
        serverSocketChannel.configureBlocking(false) ;

        serverSocketChannel.socket().bind(new InetSocketAddress("localhost",bindPort));

         selector = Selector.open();

        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT) ;


         try {

           // int x = 0 ;

            while (true) {

                if (x == 1) {
                    queuedWrites.forEach((k, v) -> {

                        try {
                            // Thread.sleep(2000);
                            k.register(selector, SelectionKey.OP_WRITE);
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
                        /*
                        ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
                        SocketChannel sc = ssc.accept();
                        sc.configureBlocking(false);

                        // queuedWrites.put(sc,ByteBuffer.wrap(("ping from server "+serverId).getBytes())) ;
                        queuedWrites.put(sc,new AtomicInteger(0)) ;

                        sc.register(selector, SelectionKey.OP_READ);
                        */

                    } else if (key.isReadable()) {

                        read(key) ;
                        /*
                        SocketChannel sc = (SocketChannel) key.channel();
                        ByteBuffer readBuffer = ByteBuffer.allocate(8192);

                        int numread;
                        numread = sc.read(readBuffer);
                        while (numread > 0) {
                            // readBuffer.clear();
                            numread = sc.read(readBuffer);

                            if (numread <= 0) {
                                break;
                            }


                        }

                        if (numread == -1) {
                            // Remote entity shut the socket down cleanly. Do the
                            // same from our end and cancel the channel.
                            key.channel().close();
                            key.cancel();
                            continue;
                        }


                        System.out.println("Read :" + numread + " " + new String(readBuffer.array()));

                        key.interestOps(SelectionKey.OP_WRITE);
                        */

                    } else if (key.isWritable()) {

                        write(key) ;

                        /*
                        SocketChannel sc = (SocketChannel) key.channel();

                        AtomicInteger i = queuedWrites.get(sc) ;

                        String stowrite = "ping " + i.get() + " from server " + serverId ;


                        // System.out.println("Sending ping :" + stowrite);

                        ByteBuffer towrite = ByteBuffer.wrap(stowrite.getBytes()) ;

                        towrite.rewind() ;

                        int n = sc.write(towrite);
                        while (n > 0 && towrite.remaining() > 0) {
                            n = sc.write(towrite);

                           
                        }

                        key.interestOps(SelectionKey.OP_READ); */
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

        // queuedWrites.put(sc,ByteBuffer.wrap(("ping from server "+serverId).getBytes())) ;
        queuedWrites.put(sc,new AtomicInteger(0)) ;


    }

    private void read(SelectionKey key) throws IOException {

        SocketChannel sc = (SocketChannel) key.channel();
        ByteBuffer readBuffer = ByteBuffer.allocate(8192);

        int numread;
        numread = sc.read(readBuffer);
        while (numread > 0) {
            // readBuffer.clear();
            numread = sc.read(readBuffer);

            if (numread <= 0) {
                break;
            }


        }

        if (numread == -1) {
            // Remote entity shut the socket down cleanly. Do the
            // same from our end and cancel the channel.
            key.channel().close();
            key.cancel();

        }


        // System.out.println("Read :" + numread + " " + new String(readBuffer.array()));
        readBuffer.rewind() ;
        int messagesize = readBuffer.getInt() ;
        LOG.info("Received message of size " + messagesize) ;
        int messageType = readBuffer.getInt() ;
        if (messageType == 1) {

            LOG.info("Received a hello message") ;
        }

        int greetingSize = readBuffer.getInt() ;
        byte[] greetingBytes = new byte[greetingSize] ;
        readBuffer.get(greetingBytes,0,greetingSize) ;
        LOG.info("text greeing "+new String(greetingBytes)) ;


        int hostStringSize = readBuffer.getInt() ;
        byte[] hostStringBytes = new byte[hostStringSize] ;
        readBuffer.get(hostStringBytes,0,hostStringSize) ;
        LOG.info("from host "+new String(hostStringBytes)) ;

        LOG.info("and port "+readBuffer.getInt()) ;

        key.interestOps(SelectionKey.OP_WRITE);


    }

    private void write(SelectionKey key) throws IOException {

        SocketChannel sc = (SocketChannel) key.channel();

        AtomicInteger i = queuedWrites.get(sc) ;

        String stowrite = "ping " + i.get() + " from server " + serverId ;


        // System.out.println("Sending ping :" + stowrite);

        ByteBuffer towrite = ByteBuffer.wrap(stowrite.getBytes()) ;

        towrite.rewind() ;

        int n = sc.write(towrite);
        while (n > 0 && towrite.remaining() > 0) {
            n = sc.write(towrite);


        }

        key.interestOps(SelectionKey.OP_READ);

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


        PeerServer server = new PeerServer(serverId) ;
        server.start(seeds) ;
    }


    public class ServerWriteRunnable implements Runnable {


        public void run() {

            while(true) {

                try {
                    Thread.sleep(2000);

                    queuedWrites.forEach((k,v)->{

                        try {
                            v.incrementAndGet() ;
                        } catch(Exception e) {
                            LOG.error("error" ,e) ;
                        }

                    });

                    synchronized (x) {
                        x =1 ;
                    }

                    selector.wakeup() ;



                    logCluster();

                } catch(Exception e) {
                    // System.out.println(e) ;
                }
            }

        }


    }


    public void addPeer(PeerClient p) {
        if (!peerSet.contains(p)) {
            peerSet.add(p);
            // logCluster();
        }
    }

    public void removePeer(PeerClient p) {
        peerSet.remove(p) ;
        // logCluster();
    }

    public void logCluster() throws Exception {

        StringBuilder sb = new StringBuilder("Cluster members ["+bindHost+":"+bindPort+",") ;

        queuedWrites.forEach((k,v)->{

            try {
                InetSocketAddress inetaddr = (InetSocketAddress) k.getRemoteAddress();


                sb.append(inetaddr.getHostString()) ;
                sb.append(":") ;
                sb.append(inetaddr.getPort()) ;
                sb.append(",") ;


            } catch(Exception e) {
                LOG.error("Error getting remote address ",e) ;
            }

        });

        sb.append("]") ;

        // System.out.println(sb) ;
        LOG.info(sb.toString()) ;

    }

    public void consumeMessage(Message message) {


    }



}
