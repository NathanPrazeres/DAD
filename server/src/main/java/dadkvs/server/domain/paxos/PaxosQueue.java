package dadkvs.server.domain.paxos;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PaxosQueue {
	private final ConcurrentHashMap<Integer, Integer> _requestMap = new ConcurrentHashMap<>();
	private final Lock _waitQueueLock = new ReentrantLock();
	private final Condition _waitQueueCondition = _waitQueueLock.newCondition();
	private volatile boolean _cancellationRequested = false;

	public int getSequenceNumber(final int reqid) {
		Integer seqNumber = _requestMap.get(reqid);
	
		while (seqNumber == null) {
			_waitQueueLock.lock();
			try {
				seqNumber = _requestMap.get(reqid);
				while (seqNumber == null  && !_cancellationRequested) {
					try {
						_waitQueueCondition.await();
					} catch (final InterruptedException e) {
						Thread.currentThread().interrupt();
						throw new RuntimeException(e);
					}
					seqNumber = _requestMap.get(reqid);
				}
				if (_cancellationRequested) {
					seqNumber = -1;
					_cancellationRequested = false;
				}
			} finally {
				_waitQueueLock.unlock();
			}
		}
		return seqNumber;
	}

	public void requestCancellation() {
		_waitQueueLock.lock();
		try {
			_cancellationRequested = true;
			_waitQueueCondition.signalAll();
		} finally {
			_waitQueueLock.unlock();
		}
	}

	public void addRequest(final int reqId, final int seqNumber) {
		_requestMap.put(reqId, seqNumber);
		_waitQueueLock.lock();
		try {
			_waitQueueCondition.signalAll();
		} finally {
			_waitQueueLock.unlock();
		}
	}
}
