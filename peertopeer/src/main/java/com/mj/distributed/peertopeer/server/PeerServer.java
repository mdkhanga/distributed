package com.mj.distributed.peertopeer.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PeerServer {

    private int serverId ;
    private boolean leader ;
    private Map<Integer,Peer> peerMap ;
    private int[] seeds ;
    List<Peer> peers = new ArrayList<Peer>();

    public PeerServer(int id,int[] seed) {

        serverId = id ;
        if (id == 1) {
            leader = true ;
        }
        seeds = seed ;
    }

    public void start(int[] seed) throws IOException {



        if (leader) {

            // initiate connect to peers

        }

        if (seed != null) {
            for (int s : seeds) {

                InetAddress address = InetAddress.getLoopbackAddress() ;
                int port = 5000 + s ;
                System.out.println("Connecting to port" +  port) ;
                Socket p = new Socket(InetAddress.getLoopbackAddress(), port);
                Peer peer = new Peer(p);
                peer.start();
                peers.add(peer);

            }
        }


        Thread writerThread = new Thread(new ServerWriteRunnable()) ;
        writerThread.start();

        accept() ;


    }

    public void accept() throws IOException {

        int port = 5000+serverId ;
        System.out.println("Server :"+serverId + " listening on port :" + port) ;
        ServerSocket s = new ServerSocket(port) ;

        while (true) {

            Socket client = s.accept() ;
            Peer p = new Peer(client) ;
            peers.add(p) ;
            p.start();

        }

    }

    public static void main(String args[]) throws Exception {

        if (args.length == 0 ) {
            System.out.println("Need at least 1 argurment") ;
        }

        int serverId = Integer.parseInt(args[0]) ;

        int size = args.length   ;

        int[] seeds = null ;

        if (size > 1) {
            seeds = new int[args.length-1] ;
            int j = 0 ;
            for (int i = 1 ; i < size ; i++) {
                seeds[j] = Integer.parseInt(args[i]) ;
                ++j ;
            }


        }

        System.out.println("Starting server with serverId:" + serverId) ;
        PeerServer server = new PeerServer(serverId,seeds) ;
        server.start(seeds) ;
    }


    public class ServerWriteRunnable implements Runnable {


        public void run() {

            while(true) {

                try {
                    Thread.sleep(5000);

                    for(Peer p : peers) {
                        p.ping(serverId);
                    }

                } catch(Exception e) {
                    System.out.println(e) ;
                }
            }

        }


    }


}
