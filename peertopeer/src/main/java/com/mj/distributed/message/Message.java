package com.mj.distributed.message;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface Message {
    long getMessageId() ;
    long getMessageTypeId() ;
    void serialize(DataOutputStream out) throws IOException;
    void deserialize(DataInputStream in) throws IOException  ;

    String print() ;

}
