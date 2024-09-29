package dadkvs.server;

import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class Queue {
	private int _nextSeqNumber = 0;
	private final ReadWriteLock _queueLock = new ReentrantReadWriteLock();
	private final Lock _waitQueueLock = new ReentrantLock();
	private final Condition _waitQueueCondition = _waitQueueLock.newCondition();

	public void waitForQueueNumber(int queueNumber) {
		_queueLock.readLock().lock();
		try {
			while (queueNumber != _nextSeqNumber) {
				_queueLock.readLock().unlock();
				_waitQueueLock.lock();
				try {
					_waitQueueCondition.await();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				} finally {
					_waitQueueLock.unlock();
				}
				_queueLock.readLock().lock();
			}
		} finally {
			_queueLock.readLock().unlock();
		}
		System.out.println(String.format("Increment queue %d", queueNumber));
	}

	public void incrementQueueNumber() {
		_queueLock.writeLock().lock();
		try {
			_nextSeqNumber++;
			if (_nextSeqNumber == Integer.MAX_VALUE) {
				_nextSeqNumber = 0;
			}
		} finally {
			_queueLock.writeLock().unlock();
		}
		_waitQueueLock.lock();
		try {
			_waitQueueCondition.signalAll();
		} finally {
			_waitQueueLock.unlock();
		}
	}
}
