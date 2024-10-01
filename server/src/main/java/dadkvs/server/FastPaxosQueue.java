package dadkvs.server; 
import java.util.ArrayList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FastPaxosQueue {
    private final ArrayList<Integer> _waitingRequests = new ArrayList<>();
	private final ReadWriteLock _queueLock = new ReentrantReadWriteLock();
	private final Lock _waitQueueLock = new ReentrantLock();
	private final Condition _waitQueueCondition = _waitQueueLock.newCondition();

    public int getSeqFromLeader(int reqid) {
        _queueLock.readLock().lock();
		int queueNumber = 0; // FIXME: THIS IS VERY INCORRECT
		int _nextSeqNumber = 0; // FIXME: THIS IS VERY INCORRECT
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

		return queueNumber;
    }
}
