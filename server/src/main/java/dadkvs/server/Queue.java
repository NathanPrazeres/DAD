package dadkvs.server;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.atomic.AtomicInteger;

public class Queue {
	private final AtomicInteger _nextSeqNumber = new AtomicInteger(0);

	public void waitForQueueNumber(int queueNumber) {
		while (queueNumber != _nextSeqNumber.get()) {
		}
	}

	public void incrementQueueNumber() {
		_nextSeqNumber.updateAndGet(seq -> (seq == Integer.MAX_VALUE) ? 0 : seq + 1);
	}
}
	