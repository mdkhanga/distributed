package com.mj.distributed.model;


import org.junit.jupiter.api.Test;

import java.io.IOException;


public class ClusterInfoTest {

    @Test
    public void fromBytes() throws IOException {

        ClusterInfo c = new ClusterInfo();
        Member m = new Member("192.168.5.1",5050, true);
        c.addMember(m);
        c.setLeader(m);

        c.addMember(new Member("192.168.5.2",5051, false));
        c.addMember(new Member("192.168.5.3",5052, false));

        byte[] cBytes = c.toBytes() ;

        ClusterInfo cCopy = ClusterInfo.fromBytes(cBytes);

        System.out.println("done") ;
    }
}