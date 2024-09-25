import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class Sequencer {
    private int _seqNumber = 0;
    private final ReadWriteLock  _seqLock = new ReentrantReadWriteLock();
    private final Lock _waitSeqLock = new ReentrantLock();
    private final Condition _waitSeqCondition = _waitSeqLock.newCondition();

    public void waitForSeqNumber(int seqNumber) {
        _seqLock.readLock().lock();
        try {
          while (seqNumber != _seqNumber) {
            _seqLock.readLock().unlock();
            _waitSeqLock.lock();
            try {
                _waitSeqCondition.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                _waitSeqLock.unlock();
            }
            _seqLock.readLock().lock();
          }
        } finally {
            _seqLock.readLock().unlock();
        }
      }

    private void incrementSequenceNumber() {
        _seqLock.writeLock().lock();
        try {
            _seqNumber++;
            if (_seqNumber == Integer.MAX_VALUE) {
                _seqNumber = 0;
            }
        } finally {
            _seqLock.writeLock().unlock();
        }
        _waitSeqLock.lock();
        try {
            _waitSeqCondition.signalAll();
        } finally {
            _waitSeqLock.unlock();
        }
    }
}
