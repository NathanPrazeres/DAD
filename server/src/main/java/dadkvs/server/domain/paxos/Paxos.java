package dadkvs.server.domain.paxos;

import java.util.concurrent.atomic.AtomicInteger;

public class Paxos {
	public AtomicInteger timestamp = new AtomicInteger(-1);
    public AtomicInteger reqId = new AtomicInteger(-1);
    public AtomicInteger seqNum = new AtomicInteger();
    public AtomicInteger numResponses = new AtomicInteger();
}
