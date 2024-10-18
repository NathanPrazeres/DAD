package dadkvs.server.domain.paxos;

import java.util.ArrayList;
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
import io.grpc.Server;

public class Paxos {
    private final Lock _waitPaxosLock = new ReentrantLock();
	private final Condition _waitPaxosCondition = _waitPaxosLock.newCondition();
	private boolean _blocked = false;
    private int _priority = -1;
    private int _reqId;
    private int _currentReqId;
    private ServerState _serverState;
    private Acceptor _acceptor;

    public Paxos(int seqNumber, int reqId, ServerState serverState, Acceptor acceptor) {
        _reqId = reqId;
        _serverState = serverState;
        _acceptor = acceptor;
		runPaxos(seqNumber);
    }

    public void lockPaxos() {
		_waitPaxosLock.lock();
		try {
			_blocked = true;
		} finally {
			_waitPaxosLock.unlock();
		}
	}

	public void unlockPaxos() {
		_waitPaxosLock.lock();
		try {
			_blocked = false;
			_waitPaxosCondition.signalAll();
		} finally {
			_waitPaxosLock.unlock();
		}
	}

	private void waitBlockPaxos() {
		_waitPaxosLock.lock();
		try {
			while (_blocked) {
				try {
					_waitPaxosCondition.await();
				} catch (final InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new RuntimeException(e);
				}
			}
		} finally {
			_waitPaxosLock.unlock();
		}
	}

    public boolean runPaxos(final int seqNum) {
		_serverState.logSystem.writeLog("Starting [PAXOS(" + seqNum + ")]...");
		try {
			while (true) {
				waitBlockPaxos();
				if (!runPhaseOne(seqNum)) {
					continue;
				}
				waitBlockPaxos();
				if (!runPhaseTwo(seqNum)) {
					continue;
				}

				_serverState.logSystem.writeLog("Ending [PAXOS(" + seqNum + ")]...");
				break;
			}
		} catch (final RuntimeException e) {
			_serverState.logSystem.writeLog("Exception occurred while running Paxos: " + e.getMessage());
			return false;
		}
		return true;
	}

    private void setNewTimestamp() {
		_priority += _serverState.getNumberOfServers();
	}

    private boolean runPhaseOne(final int seqNum) {
		_serverState.logSystem
				.writeLog("[PAXOS (" + seqNum + ")]\t\tSTARTING PHASE ONE.");

		_serverState.logSystem
				.writeLog("[PAXOS (" + seqNum + ")]\t\tSENDING PREPARES.");

		final DadkvsPaxos.PhaseOneRequest.Builder prepare = DadkvsPaxos.PhaseOneRequest.newBuilder()
				.setPhase1Index(seqNum)
				.setPhase1Config(_serverState.getConfiguration())
				.setPhase1Timestamp(_priority);
		final ArrayList<DadkvsPaxos.PhaseOneReply> promiseResponses = new ArrayList<>();
		final GenericResponseCollector<DadkvsPaxos.PhaseOneReply> collector = new GenericResponseCollector<>(
				promiseResponses,
				_serverState.acceptors.length);

		final int nAcceptors = _serverState.acceptors.length;
		final CountDownLatch latch = new CountDownLatch(nAcceptors);
		final ExecutorService executor = Executors.newFixedThreadPool(nAcceptors);

		for (final int acceptor : _serverState.acceptors) {
			executor.submit(() -> {
				try {
					final CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> observer = new CollectorStreamObserver<>(collector);
					_acceptor.asyncStubs[acceptor].phaseone(prepare.build(), observer);
				} catch (final RuntimeException e) {
					_serverState.logSystem.writeLog(
							"Exception occurred while sending Prepare request to Acceptor " + acceptor + ": " + e.getMessage());
				} finally {
					latch.countDown();
				}
			});
		}

		try {
			_serverState.logSystem.writeLog("[PAXOS (" + seqNum + ")]\t\tWaiting for all threads to finish.");
			latch.await();
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			e.printStackTrace();
		} finally {
			_serverState.logSystem.writeLog("[PAXOS (" + seqNum + ")]\t\tAll threads done");
			executor.shutdown();
		}

		final int responsesNeeded = _serverState.getQuorum(_serverState.acceptors.length);
		try {
			collector.waitForTarget(responsesNeeded);
		} catch (final RuntimeException e) {
			_serverState.logSystem.writeLog("Exception occurred during Phase 1: " + e.getCause().getMessage());
		}

		if (promiseResponses.size() >= responsesNeeded) {
			final boolean hasNaks = promiseResponses.stream().anyMatch(response -> !response.getPhase1Accepted());
			if (!hasNaks) {
				_currentReqId = extractHighestProposedValue(promiseResponses, seqNum);
				if (_currentReqId == -1) {
					_currentReqId = _reqId;
					_serverState.logSystem.writeLog("[PAXOS (" + seqNum + ")]\t\tREMOVED " + _currentReqId + " FROM THE QUEUE");
				}
				_serverState.logSystem
						.writeLog("[PAXOS (" + seqNum + ")]\t\tFINISHED PHASE ONE WITH REQUEST ID " + _currentReqId);
				return true;
			}
			setNewTimestamp();
		} else {
			_serverState.logSystem.writeLog("Error: didn't get enough responses from the quorum in Phase 1.");
			System.out.println("Error: didn't get enough responses from the quorum in Phase 1.");
		}
		return false;
	}

	private boolean runPhaseTwo(final int seqNum) {
		_serverState.logSystem
				.writeLog("[PAXOS (" + seqNum + ")]\t\tSTARTING PHASE TWO.");

		_serverState.logSystem
				.writeLog("[PAXOS (" + seqNum + ")]\t\tSENDING ACCEPT - " + "Value: " + _currentReqId + " Priority: " + _priority);

		final DadkvsPaxos.PhaseTwoRequest.Builder accept = DadkvsPaxos.PhaseTwoRequest.newBuilder()
				.setPhase2Config(_serverState.getConfiguration())
				.setPhase2Index(seqNum)
				.setPhase2Value(_currentReqId)
				.setPhase2Timestamp(_priority);

		final ArrayList<DadkvsPaxos.PhaseTwoReply> acceptedResponses = new ArrayList<>();
		final GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> collector = new GenericResponseCollector<>(
				acceptedResponses,
				_serverState.acceptors.length);

		final int nAcceptors = _serverState.acceptors.length;
		final CountDownLatch latch = new CountDownLatch(nAcceptors);
		final ExecutorService executor = Executors.newFixedThreadPool(nAcceptors);

		for (final int acceptor : _serverState.acceptors) {
			executor.submit(() -> {
				try {
					final CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply> observer = new CollectorStreamObserver<>(collector);
					_acceptor.asyncStubs[acceptor].phasetwo(accept.build(), observer);
				} catch (final RuntimeException e) {
					_serverState.logSystem.writeLog(
							"Exception occurred while sending Phase 2 request to acceptor " + acceptor + ": " + e.getMessage());
				} finally {
					latch.countDown();
				}
			});
		}

		try {
			_serverState.logSystem.writeLog("[PAXOS (" + seqNum + ")]\t\tWaiting for all threads to finish.");
			latch.await();
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			e.printStackTrace();
		} finally {
			_serverState.logSystem.writeLog("[PAXOS (" + seqNum + ")]\t\tAll threads done");
			executor.shutdown();
		}

		final int responsesNeeded = _serverState.getQuorum(_serverState.acceptors.length);
		try {
			collector.waitForTarget(responsesNeeded);
		} catch (final RuntimeException e) {
			_serverState.logSystem.writeLog("Exception occurred during Phase 2: " + e.getCause().getMessage());
		}

		if (acceptedResponses.size() >= responsesNeeded) {
			final boolean hasNaks = acceptedResponses.stream().anyMatch(response -> !response.getPhase2Accepted());
			if (!hasNaks) {
				_serverState.logSystem
						.writeLog("[PAXOS (" + seqNum + ")]\t\tFINISHED PHASE TWO.");
				return true;
			}
			setNewTimestamp();
		} else {
			_serverState.logSystem.writeLog("Error: didn't get enough responses from the quorum in Phase 2.");
			System.out.println("Error: didn't get enough responses from the quorum in Phase 2.");
		}
		return false;
	}

	public int extractHighestProposedValue(final ArrayList<DadkvsPaxos.PhaseOneReply> phase1Responses, final int seqNum) {
		int value = -1;
		int i = 0;
		int highestTimestamp = -1;
		for (final DadkvsPaxos.PhaseOneReply response : phase1Responses) {
			if (response.getPhase1Value() != -1) {
				if (response.getPhase1Timestamp() > highestTimestamp) {
					highestTimestamp = response.getPhase1Timestamp();
					value = response.getPhase1Value();
				}
			}
			_serverState.logSystem.writeLog("[PAXOS (" + seqNum + ")]\t\tResponse: " + i + " TimeStamp: "
					+ response.getPhase1Timestamp() + " Value: " + response.getPhase1Value());
			i++;
		}

		_serverState.logSystem.writeLog(
				"[PAXOS (" + seqNum + ")]\t\tHighest Proposed Value: " + value + " Highest Time Stamp: " + highestTimestamp);
		return value;
	}
}
