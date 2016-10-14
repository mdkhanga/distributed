package com.mj.distributed.peertopeer.server;



import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Created by Manoj Khangaonkar on 10/5/2016.
 */
public class Peer {

    Socket peer ;
    DataOutputStream dos ;


    private Peer() {

    }

    public Peer(Socket s) {

        peer = s ;

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

    public static class PeerReadRunnable implements Runnable {

        DataInputStream dis ;

        PeerReadRunnable(InputStream i) {

            dis = new DataInputStream(i) ;
        }

        public void run() {

            while(true) {

                try {

                } catch(Exception e) {
                    System.out.println(e) ;
                }

            }


        }

    }


}
