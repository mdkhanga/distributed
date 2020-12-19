package com.mj.distributed.peertopeer.server;

import com.mj.distributed.model.ClusterInfo;
import com.mj.distributed.model.RaftState;
import com.mj.raft.client.RaftClient;
import com.mj.raft.test.client.TestClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class LeaderElectionTest {

    static PeerServer leader;
    static PeerServer server1;
    static PeerServer server2;


    @BeforeAll
    public static void init() throws Exception {
        System.out.println("running init") ;

        leader = new PeerServer(1, 6001, RaftState.leader);
        leader.start(null) ;

        String[] seeds = new String[1];
        seeds[0] = "localhost:6001";

        server1 = new PeerServer(2, 6002,RaftState.follower);
        server1.start(seeds);

        server2 = new PeerServer(3, 6003, RaftState.follower);
        server2.start(seeds);


        Thread.sleep(10000);

        RaftClient raftClient = new RaftClient("localhost", 6001);
        raftClient.connect();

        raftClient.send(23);


        Thread.sleep(5000);

    }


    @AfterAll
    public static void destroy() throws Exception {

        server1.stop();
        server1 = null ;
        server2.stop();
        server2= null ;
    }

    @Test
    public void leaderElectionOnFailure() throws Exception {


        TestClient ts = new TestClient("localhost",6002);
        ts.connect();
        ClusterInfo cs1 = ts.getClusterInfo() ;
        assertEquals(cs1.getLeader().getPort(),6001);
        assertEquals(cs1.getLeader().getHostString(),"localhost");



        TestClient ts2 = new TestClient("localhost",6003);
        ts2.connect();
        cs1 = ts2.getClusterInfo() ;
        assertEquals(cs1.getLeader().getPort(),6001);
        assertEquals(cs1.getLeader().getHostString(),"localhost");


        leader.stop();
        Thread.sleep(30000);

        // check for new leader
        cs1 = ts2.getClusterInfo() ;
        assertEquals(cs1.getLeader().getPort(),6002);
        assertEquals(cs1.getLeader().getHostString(),"localhost");
        ts.close();
        ts2.close();



    }

    private List<Integer> convertToIntList(List<byte[]> bytes) {

        List<Integer> ret = new ArrayList<>();

        bytes.forEach(e->{
            ret.add(ByteBuffer.wrap(e).getInt());
        });

        return ret ;


    }
}