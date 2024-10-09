package dadkvs.server;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.atomic.AtomicInteger;

public class Queue {
	private final AtomicInteger _nextSeqNumber = new AtomicInteger(0);
	private final Lock _waitQueueLock = new ReentrantLock();
	private final Condition _waitQueueCondition = _waitQueueLock.newCondition();

	public void waitForQueueNumber(int queueNumber) {
		_waitQueueLock.lock();
		try {
			while (queueNumber != _nextSeqNumber.get()) {
				try {
					_waitQueueCondition.await();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new RuntimeException(e);
				}
			}
		} finally {
			_waitQueueLock.unlock();
		}
	}

	public void incrementQueueNumber() {
		_nextSeqNumber.updateAndGet(seq -> (seq == Integer.MAX_VALUE) ? 0 : seq + 1);
		_waitQueueLock.lock();
		try {
			_waitQueueCondition.signalAll();
		} finally {
			_waitQueueLock.unlock();
		}
	}
}
