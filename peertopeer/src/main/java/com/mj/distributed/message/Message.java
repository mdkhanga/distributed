package com.mj.distributed.message;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface Message {

    long getMessageId() ;
    void serialize(DataOutputStream out) throws IOException;
    Message deserialize(DataInputStream in) throws IOException  ;

}
