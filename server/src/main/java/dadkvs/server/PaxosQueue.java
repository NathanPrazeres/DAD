package dadkvs.server; 
import java.util.ArrayList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.ConcurrentHashMap;

public class PaxosQueue {
	// (request ID, (Sequence number, counter))
    private ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Integer>> _requestMap  = new ConcurrentHashMap<>();
	private final Lock _waitQueueLock = new ReentrantLock();
	private final Condition _waitQueueCondition = _waitQueueLock.newCondition();

    public int getSequenceNumber(int reqid) {
		Integer seqNumber = getValidSeqNum(reqid, 2);
		while (seqNumber == null) {
			_waitQueueLock.lock();
			try {
				while (getValidSeqNum(reqid, 2) == null) {
					try {
						_waitQueueCondition.await();
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						throw new RuntimeException(e);
					}
				}
				seqNumber = getValidSeqNum(reqid, 2);
			} finally {
				_waitQueueLock.unlock();
			}
		}
		return seqNumber;
    }

	private Integer getValidSeqNum(int reqid, int counter) {
		ConcurrentHashMap<Integer, Integer> seqMap = _requestMap.get(reqid);
		if (seqMap != null) {
			for (Integer seqNum : seqMap.keySet()) {
				if (seqMap.get(seqNum) >= counter) {
					return seqNum;
				}
			}
		}
		return null;
	}

    public void addRequest(int reqid, int seqNumber) {
		if (_requestMap.get(reqid) != null) {
			int counter = _requestMap.get(reqid).get(seqNumber);
			_requestMap.get(reqid).put(seqNumber, counter++);
		} else {
			ConcurrentHashMap<Integer, Integer> seqMap = new ConcurrentHashMap();
			seqMap.put(seqNumber, 1);
			_requestMap.put(reqid, seqMap);
		}
		_waitQueueLock.lock();
		try {
			_waitQueueCondition.signalAll();
		} finally {
			_waitQueueLock.unlock();
		}
    }
}
