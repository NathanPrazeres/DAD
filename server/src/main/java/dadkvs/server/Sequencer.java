package dadkvs.server;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReadWriteLock;

public class Sequencer {
	private int _seqNumber = 0;
	private final ReadWriteLock _seqLock = new ReentrantReadWriteLock();

	public int readSeqNumber() {
		int seqNumber;
		_seqLock.readLock().lock();
		try {
			seqNumber = _seqNumber;
		} finally {
			_seqLock.readLock().unlock();
		}
		return seqNumber;
	}

	public int getSeqNumber() {
		int seqNumber;
		_seqLock.readLock().lock();
		try {
			seqNumber = _seqNumber;
		} finally {
			_seqLock.readLock().unlock();
		}
		incrementSeqNumber();
		return seqNumber;
	}

	private void incrementSeqNumber() {
		_seqLock.writeLock().lock();
		try {
			_seqNumber++;
			if (_seqNumber == Integer.MAX_VALUE) {
				_seqNumber = 0;
			}
		} finally {
			_seqLock.writeLock().unlock();
		}
	}
}
