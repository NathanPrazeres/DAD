package dadkvs.server; 
import java.util.ArrayList;

public class FastPaxosQueue {
    private ArrayList<int> _waitingRequests = new new ArrayList<int>();
	private final ReadWriteLock _queueLock = new ReentrantReadWriteLock();
	private final Lock _waitQueueLock = new ReentrantLock();
	private final Condition _waitQueueCondition = _waitQueueLock.newCondition();

    public int GetSeqFromLeader(int reqid) {
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
    }
}
