package com.mj.distributed.message;

import com.mj.distributed.model.ClusterInfo;
import com.mj.distributed.model.Member;
import org.junit.jupiter.api.Test;


import java.nio.ByteBuffer;



public class ClusterInfoMessageTest {

    @Test
    public void serialize() throws Exception {

        ClusterInfo c = new ClusterInfo();
        Member m = new Member("192.168.5.1",5050, true);
        c.addMember(m);
        c.setLeader(m);

        c.addMember(new Member("192.168.5.2",5051, false));
        c.addMember(new Member("192.168.5.3",5052, false));

        ClusterInfoMessage cm = new ClusterInfoMessage(c);
        ByteBuffer b = cm.serialize();

        ClusterInfoMessage cmCopy = ClusterInfoMessage.deserialize(b) ;

        System.out.println("done");
    }
}