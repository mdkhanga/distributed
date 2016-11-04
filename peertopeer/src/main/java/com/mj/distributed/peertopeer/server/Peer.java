package com.mj.distributed.peertopeer.server;



import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;

/**
 * Created by Manoj Khangaonkar on 10/5/2016.
 */
public class Peer {

    Socket peer ;
    PeerServer peerServer ;
    DataOutputStream dos ;
    Integer peerServerId = -1 ;



    private Peer() {

    }

    public Peer(Socket s, PeerServer ps) {

        peer = s ;
        peerServer = ps ;

    }

    public Peer(Integer id, PeerServer ps) {

        peerServerId = id ;
        peerServer = ps ;

    }


    public void start() {

        try {

            InputStream is = peer.getInputStream();
            OutputStream os = peer.getOutputStream() ;

            PeerReadRunnable pr = new PeerReadRunnable(is) ;

            Thread t = new Thread(pr) ;
            t.start();

            dos = new DataOutputStream(os) ;


        } catch(IOException e) {
            System.out.println(e) ;
        }
    }

    public void ping(int i) throws IOException {
        dos.writeInt(i) ;
    }

    public class PeerReadRunnable implements Runnable {

        DataInputStream dis ;

        PeerReadRunnable(InputStream i) {

            dis = new DataInputStream(i) ;
        }

        public void run() {

            while(true) {

                try {

                    int pingId = dis.readInt() ;
                    peerServerId = pingId ; // This is id of server this peer p
                    peerServer.addPeer(Peer.this);
                    System.out.println("Received ping " + pingId) ;

                } catch(SocketException e) {
                    System.out.println(e) ;
                    peerServer.removePeer(Peer.this) ;
                    return ;
                } catch(IOException ie) {
                    System.out.println(ie) ;
                }

            }


        }

    }

    @Override
    public boolean equals(Object obj) {

        if (null == obj) {
            return false ;
        }

        if (! (obj instanceof Peer) ) {
            return false ;
        }

        return peerServerId.equals(((Peer) obj).peerServerId) ;

    }

    @Override
    public int hashCode() {
        return peerServerId.hashCode() ;
    }

    public int getPeerServerId() {
        return peerServerId ;
    }

    public boolean isRemotePeer() {
        return peer != null ;
    }

}
