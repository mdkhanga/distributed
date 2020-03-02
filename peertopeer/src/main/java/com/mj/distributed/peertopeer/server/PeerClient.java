package com.mj.distributed.peertopeer.server;



import com.mj.distributed.message.Message;
import com.mj.distributed.message.MessageFactory;
import com.mj.distributed.message.PingMessage;

import org.slf4j.LoggerFactory ;
import org.slf4j.Logger ;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
// import java.net.Socket;


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

    public Deque<String> writeQueue = new ArrayDeque<String>() ;

    private ByteBuffer readBuf = ByteBuffer.allocate(8192) ;



    Logger LOG = LoggerFactory.getLogger(PeerClient.class);

    private PeerClient() {

    }

    public PeerClient(String host, int port) {

       this.remoteHost = host ;
       this.remotePort = port ;


    }




    public void start() throws Exception {

        Selector selector = Selector.open() ;

        clientChannel = SocketChannel.open();
        clientChannel.configureBlocking(false);

        clientChannel.connect(new InetSocketAddress(remoteHost, remotePort));

        clientChannel.register(selector, SelectionKey.OP_CONNECT) ;

        int i = 0 ;

        writeQueue.add("Hello from " + remoteHost + ":" + remotePort) ;

        while(true) {



            selector.select() ;

            // System.out.println(i) ;

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
            // if (i == 11)
            //	break ;
        }


    }

    private void finishConnection(SelectionKey key) throws IOException {

        clientChannel.finishConnect() ;
        key.interestOps(SelectionKey.OP_WRITE) ;


    }

    private void write(SelectionKey key) throws IOException {

        // System.out.println("In write") ;

        // String toWrite = writeQueue.pollFirst() ;

        String toWrite = "Hello from 5002" ;

        if (toWrite != null) {

           // System.out.println("writing :" + toWrite) ;

            ByteBuffer b ;
            b = ByteBuffer.wrap(toWrite.getBytes()) ;

            // b.flip();

            while (true) {
                int n = clientChannel.write(b) ;



                if (n == 0 || b.remaining() == 0)
                    break ;
            }

        }

        key.interestOps(SelectionKey.OP_READ) ;
    }

    public void read(SelectionKey key) throws IOException {

        // System.out.println("In read") ;

        readBuf.clear() ;

        while (true) {

            // System.out.println("before read") ;
            int numread = clientChannel.read( readBuf );
            // System.out.println("after read") ;


            if (numread <=0) {
                break;
            }


        }

        System.out.println("Read from server:" + new String(readBuf.array())) ;


        // key.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ) ;

    }



    public void ping(int i) throws IOException {
        PingMessage pingMessage = new PingMessage(i);
        pingMessage.serialize(dos);

    }

    public void sendMessage(Message m) {

        // message needs to be queue and sent by a writer thread
    }


    public class PeerReadRunnable implements Runnable {

        DataInputStream dis;

        PeerReadRunnable(InputStream i) {

            dis = new DataInputStream(i);
        }

        public void run() {

            while (true) {

                try {

                    int messageId = dis.readInt();
                    Message m = MessageFactory.deserialize(messageId, dis);

                    if (m instanceof PingMessage) {

                        peerServerId = ((PingMessage) m).getServerId();

                    }

                    LOG.info("From :" + remoteIpAddress.toString() + ":" + remotePort + "Recieved message :" + m.print());
                    peerServer.addPeer(PeerClient.this);

                } catch (IOException e) {
                    System.out.println(e);
                    peerServer.removePeer(PeerClient.this);
                    return;
                }


            }


        }

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



}