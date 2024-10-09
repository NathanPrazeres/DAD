package dadkvs.server;

import java.util.concurrent.atomic.AtomicInteger;

public class Sequencer {
	private final AtomicInteger _seqNumber = new AtomicInteger(0);

	public int readSeqNumber() {
		return _seqNumber.get();
	}

	public int getSequenceNumber() {
		return _seqNumber.getAndUpdate(seq -> (seq == Integer.MAX_VALUE) ? 0 : seq + 1);
	}
}
