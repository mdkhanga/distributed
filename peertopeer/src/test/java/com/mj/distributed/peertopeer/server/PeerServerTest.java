package com.mj.distributed.peertopeer.server;

import com.mj.distributed.model.RaftState;
import com.mj.raft.client.RaftClient;
import com.mj.raft.test.client.TestClient;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class PeerServerTest {

    @Test
    public void logReplication() throws Exception {


        PeerServer leader = new PeerServer(1, RaftState.leader);
        leader.start(null) ;

        String[] seeds = new String[1];
        seeds[0] = "localhost:5001";

        PeerServer server1 = new PeerServer(2, RaftState.follower);
        server1.start(seeds);

        PeerServer server2 = new PeerServer(3, RaftState.follower);
        server2.start(seeds);

        Thread.sleep(3000);

        List<Integer> inputs = Arrays.asList(23,33,44,91,66);

        RaftClient raftClient = new RaftClient("localhost", 5001);
        raftClient.connect();

        for (int i = 0 ; i <=4 ; i++) {
            raftClient.send(inputs.get(i));
        }



        Thread.sleep(5000);

        TestClient ts = new TestClient("localhost",5002);
        ts.connect();
        List<byte[]> server2Values = ts.get(0,5);
        List<Integer> server2Ints = convertToIntList(server2Values) ;

        TestClient ts2 = new TestClient("localhost",5003);
        ts2.connect();
        List<byte[]> server3Values = ts2.get(0,5);
        List<Integer> server3Ints = convertToIntList(server3Values) ;

        TestClient ts0 = new TestClient("localhost",5001);
        ts0.connect();
        List<byte[]> server0Values = ts0.get(0,5);
        List<Integer> server0Ints = convertToIntList(server0Values) ;

        for (int i = 0 ; i <= 4; i++ ) {
            assertEquals(server3Ints.get(i), server2Ints.get(i) );
            assertEquals(server0Ints.get(i), server2Ints.get(i) );
            System.out.println(server2Ints.get(i));
        }

    }

    private List<Integer> convertToIntList(List<byte[]> bytes) {

        List<Integer> ret = new ArrayList<>();

        bytes.forEach(e->{
            ret.add(ByteBuffer.wrap(e).getInt());
        });

        return ret ;


    }
}