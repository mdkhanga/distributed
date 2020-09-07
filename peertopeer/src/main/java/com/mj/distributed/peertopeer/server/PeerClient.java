package com.mj.distributed.peertopeer.server;



import com.mj.distributed.message.HelloMessage;
import com.mj.distributed.model.LogEntry;
import com.mj.distributed.message.Message;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;



/**
 * Created by Manoj Khangaonkar on 10/5/2016.
 */
public class PeerClient {

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

    ExecutorService peerClientExecutor = Executors.newFixedThreadPool(2) ;

    public Queue<ByteBuffer> writeQueue = new ConcurrentLinkedDeque<ByteBuffer>() ;

    private ByteBuffer readBuf = ByteBuffer.allocate(8192)  ;

    List<byte[]> rlog = new ArrayList<>();
    int lastComittedIndex  = -1 ;

    Logger LOG = LoggerFactory.getLogger(PeerClient.class);


    private PeerClient() {

    }

    public PeerClient(String host, int port, PeerServer p) {

       this.remoteHost = host ;
       this.remotePort = port ;
       this.peerServer = p ;


    }




    public void start() throws Exception {

        PeerClientCallable peerClientCallable = new PeerClientCallable(this) ;
        peerClientExecutor.submit(peerClientCallable) ;
       PeerClientStatusCallable peerClientStatusCallable = new PeerClientStatusCallable();
        peerClientExecutor.submit(peerClientStatusCallable);

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
                    this.lastComittedIndex = lastComittedIndex;
                }
            } else {
                ret = false ;
                // LOG.info("did not add to rlog return false") ;
            }
        } else {
            // LOG.info("No entry") ;
        }

        if (lastComittedIndex < rlog.size()) {
            this.lastComittedIndex = lastComittedIndex;
        }

        return ret ;
    }

    public void sendMessage(Message m) {

        // message needs to be queue and sent by a writer thread
    }

    public void queueSendMessage(ByteBuffer b) {

        writeQueue.add(b) ;
        selector.wakeup() ;

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

            selector = Selector.open() ;

            clientChannel = SocketChannel.open();
            clientChannel.configureBlocking(false);

            clientChannel.connect(new InetSocketAddress(remoteHost, remotePort));

            clientChannel.register(selector, SelectionKey.OP_CONNECT) ;

            int i = 0 ;

            HelloMessage m = new HelloMessage(peerServer.getBindHost(),peerServer.getBindPort()) ;
            writeQueue.add(m.serialize());

            while(true) {


                if (clientChannel.isConnected() && writeQueue.peek() != null) {
                    clientChannel.register(selector, SelectionKey.OP_WRITE);
                }


                selector.select() ;

                Iterator<SelectionKey> skeys = selector.selectedKeys().iterator() ;

                while (skeys.hasNext()) {
                    SelectionKey key = (SelectionKey) skeys.next();
                    skeys.remove();

                    if (!key.isValid()) {
                        continue;
                    }

                    // System.out.println("We have a valid key") ;
                    // Check what event is available and deal with it
                    if (key.isConnectable()) {
                        finishConnection(key);
                    } else if (key.isReadable()) {
                        read(key);
                        // done = true ;
                    } else if (key.isWritable()) {
                        write(key);
                    } else {
                        System.out.println("not handled key") ;

                    }
                }

                ++i ;

            }

        }

        private void finishConnection(SelectionKey key) throws IOException {

            clientChannel.finishConnect() ;
            key.interestOps(SelectionKey.OP_WRITE) ;
            // peerServer.addPeer(remoteHost+":"+remotePort) ;
        }

        private void write(SelectionKey key) throws IOException {

            ByteBuffer b ;
            b = writeQueue.poll() ;

            int n = clientChannel.write(b) ;
            int totalbyteswritten = n ;
            while (n > 0 && b.remaining() > 0) {
                n = clientChannel.write(b) ;
                totalbyteswritten = totalbyteswritten + n ;

            }

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

                PeerServer.inBoundMessageCreator.submit(clientChannel, readBuf, totalread,
                        new ClientMessageHandlerCallable(peerClient, clientChannel, readBuf));
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

            while(true) {

                Thread.sleep(5000) ;

                logRlog();

            }

        }

    }

    public void logRlog() throws Exception {

        StringBuilder sb = new StringBuilder("Replicated Log [") ;



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
}