package com.mj.distributed.model;

public class Member {

    private String hostString ;
    private int port ;

    public Member( String h, int p) {

        this.hostString = h ;
        this.port = p ;
    }

    public String getHostString() {
        return hostString;
    }

    public int getPort() {
        return port ;
    }
}
