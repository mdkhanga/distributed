package com.mj.distributed.message;

public class AppendEntriesMessage {

    private int messageType = 4;

    private int leaderId;

    private int term;
    private LogEntry[] entries;

    private int prevTerm;
    private long prevIndex;

    private long leaderCommitIndex;

}
