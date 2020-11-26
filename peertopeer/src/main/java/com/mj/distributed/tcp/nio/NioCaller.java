package com.mj.distributed.tcp.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NioCaller {

    Logger LOG = LoggerFactory.getLogger(NioCaller.class);

    private String remoteHost;
    private int remotePort;
    private NioCallerConsumer nioCallerConsumer;

    private ExecutorService executorThread = Executors.newFixedThreadPool(1);

    private Queue<ByteBuffer> writeQueue = new ConcurrentLinkedDeque<ByteBuffer>() ;

    private Selector selector ;
    private SocketChannel clientChannel ;

    private ByteBuffer readBuf = ByteBuffer.allocate(8192)  ;

    public NioCaller(String host, int port, NioCallerConsumer n) {
        remoteHost = host;
        remotePort = port;
        nioCallerConsumer = n;
    }

    public void start() {

        try {

            selector = Selector.open();

            clientChannel = SocketChannel.open();
            clientChannel.configureBlocking(false);

            clientChannel.connect(new InetSocketAddress(remoteHost, remotePort));

            clientChannel.register(selector, SelectionKey.OP_CONNECT);

            executorThread.submit(this::call);
        } catch(Exception e) {

            LOG.error("Error starting NioCaller ",e) ;
        }

    }

    public Void call() {

        try {


            while (true) {

                synchronized (writeQueue) {
                    if (clientChannel.isConnected() && writeQueue.peek() != null) {
                        clientChannel.register(selector, SelectionKey.OP_WRITE);

                    }
                }
                selector.select();
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

            }

        } catch(Exception e) {
            LOG.error("Error in NioCaller",e);
        }


        return null ;
    }

    private void finishConnection(SelectionKey key) throws IOException {

        clientChannel.finishConnect() ;
        key.interestOps(SelectionKey.OP_WRITE) ;

        // peerServer.addPeer(clientChannel, new CallerPeer(new Member(remoteHost, remotePort), peerClient));
        nioCallerConsumer.addedConnection(clientChannel);
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
            nioCallerConsumer.consumeMessage(clientChannel, totalread, readBuf);

        } catch (IOException e) {
            clientChannel.close() ;
            key.cancel();
            String s = remoteHost +":"+remotePort ;
            LOG.info(s + " has left the cluster") ;
            // peerServer.removePeer(s);

        }

    }

    public void queueSendMessage(ByteBuffer b) {

        synchronized (writeQueue) {
            writeQueue.add(b);
        }

        selector.wakeup() ;

    }

}


