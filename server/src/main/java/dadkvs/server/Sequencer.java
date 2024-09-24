public class Sequencer {
    private int _seqNumber = 0;
    private final ReadWrite _seqLock = new ReentrantReadWriteLock();
    private final Lock _waitSeqLock = new ReentrantLock();
    private final Condition _waitSeqCondition = new  _waitSeqLock.newCondition();

    private void incrementSequenceNumber() {
        seqLock.writeLock().lock();
        try {
            _seqNumber++;
            if (_seqNumber == Integer.MAX_VALUE) {
                _seqNumber = 0;
            }
        } finally {
            _seqLock.lock();
        }
        _waitSeqLock.lock();
        try {
            _waitSeqCondition.signalAll();
        } finally {
            _waitSeqLock.unlock();
        }
    }
}
