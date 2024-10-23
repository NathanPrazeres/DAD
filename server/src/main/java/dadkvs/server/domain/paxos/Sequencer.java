package dadkvs.server.domain.paxos;

import java.util.concurrent.atomic.AtomicInteger;

public class Sequencer {
	public AtomicInteger seqNumber = new AtomicInteger(0);

	public int readSeqNumber() {
		return seqNumber.get();
	}

	public int getSequenceNumber() {
		return seqNumber.getAndUpdate(seq -> (seq == Integer.MAX_VALUE) ? 0 : seq + 1);
	}
}
