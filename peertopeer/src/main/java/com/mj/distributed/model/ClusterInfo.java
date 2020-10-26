package com.mj.distributed.model;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ClusterInfo {

    private Member leader ;
    private List<Member> members = Collections.synchronizedList(new ArrayList<Member>());

    public ClusterInfo() {}

    public ClusterInfo(Member leader, List<Member> ms) {
        this.leader = leader;
        this.members = ms ;
    }

    public void addMember(Member m) {
        members.add(m);
    }

    public Member getLeader() {
        return leader;
    }

    public void setLeader(Member m) {
        leader = m;
    }

    public List<Member> getMembers() {
        return members;
    }

    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(out);

        d.writeInt(members.size());

        for (int i = 0 ; i < members.size() ; i++) {
            byte[] mbytes = members.get(i).toBytes() ;
            d.writeInt(mbytes.length);
            d.write(mbytes);
        }

        byte[] ret = out.toByteArray();
        out.close();
        d.close();
        return ret;
    }

    public static ClusterInfo fromBytes(byte[] bytes) throws IOException {
        ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
        DataInputStream din = new DataInputStream(bin);
        ClusterInfo cinfo = new ClusterInfo();

        int numMembers = din.readInt() ;

        for(int i = 0 ; i < numMembers ; i++) {

            int numbytes = din.readInt() ;
            byte[] mbytes = new byte[numbytes];
            din.read(mbytes, 0, numbytes);
            Member m = Member.fromBytes(mbytes);
            cinfo.addMember(m);
            if (m.isLeader()) {
                cinfo.setLeader(m);
            }


        }


        return cinfo;
    }

    public void removeMember(String host, int port) {

        Iterator<Member> itm = members.iterator();

        while (itm.hasNext()) {
            Member m = itm.next();
            if (m.getHostString().equals(host) && m.getPort() == port ) {
                itm.remove();
            }
        }
    }

    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[");
        members.forEach((m)->{
           b.append(m.toString());
        });
        b.append("]");

        return b.toString();
    }
}
