package com.mj.distributed.message;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by manoj on 7/15/17.
 */
public abstract class AbstractMessage implements Message {

    long messageId ;

    private static AtomicLong nextMessageId = new AtomicLong(0) ;

    public long getMessageId() {
        return messageId ;
    }


}
