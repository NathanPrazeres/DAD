package dadkvs.server.domain.paxos;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import dadkvs.DadkvsPaxos;
import dadkvs.server.domain.ServerState;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;

public class Proposer extends Acceptor {
	public ConcurrentHashMap<Integer, Paxos> paxosHashMap = new ConcurrentHashMap<>();
	private final Sequencer _sequencer;
	private final ConcurrentLinkedQueue<Integer> _requestQueue;
	private final Lock _paxosLock = new ReentrantLock();
    private final Condition _paxosCondition = _paxosLock.newCondition();

	public Proposer() {
		_requestQueue = new ConcurrentLinkedQueue<>();
		_sequencer = new Sequencer();
	}

	public void setServerState(final ServerState serverState) {
		_serverState = serverState;
		initPaxosComms();
		_serverState.requestCancellation();
	}

	public void handleCommittx(final int reqId) {
		_serverState.logSystem.writeLog("Handling commit request with Request ID: " + reqId);
		_requestQueue.add(reqId);
		final int seqNumber = _sequencer.getSequenceNumber();
		paxosHashMap.put(seqNumber, new Paxos(seqNumber, reqId, _serverState, this));
	}

	public void promote() {
		// proposer can't be promoted
	}

	public void demote() {
		terminateComms();
		_serverState.changePaxosState(new Acceptor());
	}
	
	@Override
	public void reconfigure(int newConfig) {
		int[] config = ServerState.CONFIGS[newConfig];
		boolean found = false;
	
		for (int id : config) {
			if (id == _serverState.myId) {
				found = true;
				break;
			}
		}
	
		if (!found) {
			_serverState.logSystem.writeLog("Reconfiguring");
			super.demote();
		}
	}

	public void blockPaxos(int seqNumber) {
		waitForPaxosElement(seqNumber);
		paxosHashMap.get(seqNumber).lockPaxos();
	}

	public void unblockPaxos(int seqNumber) {
		waitForPaxosElement(seqNumber);
		paxosHashMap.get(seqNumber).unlockPaxos();
	}

	private void waitForPaxosElement(int seqNumber) {
        _paxosLock.lock();
        try {
            while (!paxosHashMap.containsKey(seqNumber)) {
                try {
                    _paxosCondition.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        } finally {
            _paxosLock.unlock();
        }
    }
}
