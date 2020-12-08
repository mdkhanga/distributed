package com.mj.raft.test.client;

import com.mj.distributed.message.*;
import com.mj.distributed.peertopeer.server.PeerClient;
import com.mj.distributed.tcp.nio.NioCaller;
import com.mj.distributed.tcp.nio.NioCallerConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TestClient implements NioCallerConsumer {

    private final String hostString;
    private final int port;
    private NioCaller nioCaller;
    private final AtomicInteger seq = new AtomicInteger(1);
    // private Map<Integer, Message> responseSet = new ConcurrentHashMap<>();
    private Integer messageWaitingResponse ;
    private Message response;
    Logger LOG = LoggerFactory.getLogger(TestClient.class);

    public TestClient(String host, int port) {

        this.hostString = host;
        this.port = port;
    }

    public void connect() throws Exception {

        nioCaller = new NioCaller(hostString, port, this);
        nioCaller.start();
        TestClientHello hello = new TestClientHello();
        nioCaller.queueSendMessage(hello.serialize());

    }

    /*
    public void send(int value) throws Exception {

        byte[] val = ByteBuffer.allocate(4).putInt(value).array() ;
        RaftClientAppendEntry entry = new RaftClientAppendEntry(val);
        nioCaller.queueSendMessage(entry.serialize());

    } */

    public List<byte[]> get(int start, int count) throws Exception {

        Integer id = seq.getAndIncrement();
        GetServerLog gsLog = new GetServerLog(id, 0, count, (byte)0);
        nioCaller.queueSendMessage(gsLog.serialize());
        messageWaitingResponse = id;
        synchronized (messageWaitingResponse) {

            while(response == null ) {

                messageWaitingResponse.wait();

            }

            GetServerLogResponse r = (GetServerLogResponse) response ;
            return r.getEntries();
        }

    }

    public void addedConnection(SocketChannel s) {

    }

    public void droppedConnection(SocketChannel s) {

    }

    public void consumeMessage(SocketChannel s, int numBytes, ByteBuffer b)  {

        try {

            LOG.info("Test client Received a GetServerLog response message");

            synchronized (messageWaitingResponse) {

                // FIXME: could be a partial message or multiple messages
                response = GetServerLogResponse.deserialize(b);
                messageWaitingResponse.notifyAll();

            }
        } catch(Exception e) {
            LOG.error("Error deserializing message",e) ;
        }
    }

    public static void main(String[] args) throws Exception {

        TestClient client = new TestClient("localhost",5001);
        client.connect();
        // client.send(23);

        Scanner scanner = new Scanner(System.in);

        while(true) {

            System.out.print("Enter a number:") ;
            String s = scanner.nextLine();
            // client.send(Integer.valueOf(s));

        }



    }

}
