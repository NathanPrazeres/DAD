package dadkvs.server; 
import java.util.ArrayList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.ConcurrentHashMap;

public class PaxosQueue {
    private ConcurrentHashMap<Integer, Integer> _requestMap  = new ConcurrentHashMap<>();
	private final Lock _waitQueueLock = new ReentrantLock();
	private final Condition _waitQueueCondition = _waitQueueLock.newCondition();

    public int getSequenceNumber(int reqid) {
		Integer seqNumber = _requestMap.get(reqid);
		
		while (seqNumber == null) {
			_waitQueueLock.lock();
			try {
				while (_requestMap.get(reqid) == null) {
					try {
						_waitQueueCondition.await();
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						throw new RuntimeException(e);
					}
				}
				seqNumber = _requestMap.get(reqid);
			} finally {
				_waitQueueLock.unlock();
			}
		}
		return seqNumber;
    }

    public void addRequest(int reqid, int seqNumber) {
		_requestMap.put(reqid, seqNumber);
		_waitQueueLock.lock();
		try {
			_waitQueueCondition.signalAll();
		} finally {
			_waitQueueLock.unlock();
		}
    }
}
