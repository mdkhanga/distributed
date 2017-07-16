package com.mj.distributed.peertopeer.server;



import com.mj.distributed.message.Message;
import com.mj.distributed.message.MessageFactory;
import com.mj.distributed.message.PingMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;

/**
 * Created by Manoj Khangaonkar on 10/5/2016.
 */
public class Peer {

    Socket peer;
    PeerServer peerServer;
    DataOutputStream dos;
    Integer peerServerId = -1;
    int remotePort; // port returned remote initiates the connection - other end of socket after our accept
    int remoteListenPort ; // if we initate connection, this is where we connect to
    InetAddress remoteIpAddress;

    Logger LOG = LogManager.getLogger(Peer.class);

    private Peer() {

    }

    public Peer(Socket s, PeerServer ps) {

        peer = s;
        peerServer = ps;
        remoteIpAddress = s.getInetAddress();
        remotePort = s.getPort();


    }

    public Peer(Integer id, PeerServer ps) {

        peerServerId = id;
        peerServer = ps;

    }


    public void start() {

        try {

            InputStream is = peer.getInputStream();
            OutputStream os = peer.getOutputStream();

            PeerReadRunnable pr = new PeerReadRunnable(is);

            Thread t = new Thread(pr);
            t.start();

            dos = new DataOutputStream(os);


        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public void ping(int i) throws IOException {
        PingMessage pingMessage = new PingMessage(i);
        pingMessage.serialize(dos);
        // dos.writeInt(i) ;
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
                    /*
                    int pingId = dis.readInt() ;
                    peerServerId = pingId ; // This is id of server this peer p
                    peerServer.addPeer(Peer.this);
                    LOG.info("Received ping " + pingId) ; */
                    if (m instanceof PingMessage) {

                        peerServerId = ((PingMessage) m).getServerId();

                    }

                    LOG.info("From :" + remoteIpAddress.toString() + ":" + remotePort + "Recieved message :" + m.print());
                    peerServer.addPeer(Peer.this);

                } catch (IOException e) {
                    System.out.println(e);
                    peerServer.removePeer(Peer.this);
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

        if (!(obj instanceof Peer)) {
            return false;
        }

        return peerServerId.equals(((Peer) obj).peerServerId);

    }

    @Override
    public int hashCode() {
        return peerServerId.hashCode();
    }

    public String getPeerServer() {
        return remoteIpAddress.toString()+":"+remoteListenPort;
    }

    public boolean isRemotePeer() {
        return peer != null;
    }

}