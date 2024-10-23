package dadkvs.server.domain.paxos;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
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
    private final Lock _waitPaxosLock = new ReentrantLock();
	private final Condition _waitPaxosCondition = _waitPaxosLock.newCondition();
	private final Sequencer _sequencer;
	private final ConcurrentLinkedQueue<Integer> _requestQueue;
	private boolean _blocked = false;

	public Proposer() {
		_requestQueue = new ConcurrentLinkedQueue<>();
		_sequencer = new Sequencer();
	}

	public void setServerState(final ServerState serverState) {
		this.serverState = serverState;
		initPaxosComms();
		serverState.requestCancellation();
		int highestPaxosInstance = getHighestPaxosInstance();
		serverState.logSystem.writeLog("Highest sequence number: " + highestPaxosInstance);
		if (highestPaxosInstance != -1) {
			_sequencer.seqNumber.set(highestPaxosInstance + 1);
		}
	}

	public void handleCommittx(final int reqId) {
		serverState.logSystem.writeLog("Handling commit request with Request ID: " + reqId);
		waitBlockPaxos();
		_requestQueue.add(reqId);
		final int seqNumber = _sequencer.getSequenceNumber();

		Paxos paxos = getPaxos(seqNumber);
		paxos.seqNum.set(seqNumber);
		paxosInstancesHashMap.put(seqNumber, paxos);

		runPaxos(seqNumber);
	}

	public void promote() {
		// proposer can't be promoted
	}

	public void demote() {
		terminateComms();
		serverState.changePaxosState(new Acceptor());
	}
	
	@Override
	public void reconfigure(int newConfig) {
		int[] config = ServerState.CONFIGS[newConfig];
		boolean found = false;
	
		for (int id : config) {
			if (id == serverState.myId) {
				found = true;
				break;
			}
		}
	
		if (!found) {
			serverState.logSystem.writeLog("Reconfiguring");
			super.demote();
		}
	}

	public void blockPaxos() {
		_waitPaxosLock.lock();
		try {
			_blocked = true;
		} finally {
			_waitPaxosLock.unlock();
		}
	}

	public void unblockPaxos() {
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
		serverState.logSystem.writeLog("Starting [PAXOS(" + seqNum + ")]...");
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

				serverState.logSystem.writeLog("Ending [PAXOS(" + seqNum + ")]...");
				break;
			}
		} catch (final RuntimeException e) {
			serverState.logSystem.writeLog("Exception occurred while running Paxos: " + e.getMessage());
			return false;
		}
		return true;
	}

    private void setNewTimestamp(int seqNum) {
		Paxos paxos = paxosInstancesHashMap.get(seqNum);
		paxos.timestamp.set(paxos.timestamp.get() + serverState.getNumberOfServers());
	}

    private boolean runPhaseOne(final int seqNum) {
		serverState.logSystem
				.writeLog("[PAXOS (" + seqNum + ")]\t\tSTARTING PHASE ONE.");

		serverState.logSystem
				.writeLog("[PAXOS (" + seqNum + ")]\t\tSENDING PREPARES.");

		int timestamp;
		if (paxosInstancesHashMap.get(seqNum).timestamp.get() == -1) {
			timestamp = serverState.myId();
		} else {
			timestamp = paxosInstancesHashMap.get(seqNum).timestamp.get();
		}

		final DadkvsPaxos.PhaseOneRequest.Builder prepare = DadkvsPaxos.PhaseOneRequest.newBuilder()
				.setPhase1Index(seqNum)
				.setPhase1Config(serverState.getConfiguration())
				.setPhase1Timestamp(timestamp);
		final ArrayList<DadkvsPaxos.PhaseOneReply> promiseResponses = new ArrayList<>();
		final GenericResponseCollector<DadkvsPaxos.PhaseOneReply> collector = new GenericResponseCollector<>(
				promiseResponses,
				serverState.acceptors.length);

		final int nAcceptors = serverState.acceptors.length;
		final CountDownLatch latch = new CountDownLatch(nAcceptors);
		final ExecutorService executor = Executors.newFixedThreadPool(nAcceptors);

		for (final int acceptor : serverState.acceptors) {
			executor.submit(() -> {
				try {
					final CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> observer = new CollectorStreamObserver<>(collector);
					asyncStubs[acceptor].phaseone(prepare.build(), observer);
				} catch (final RuntimeException e) {
					serverState.logSystem.writeLog(
							"Exception occurred while sending Prepare request to Acceptor " + acceptor + ": " + e.getMessage());
				} finally {
					latch.countDown();
				}
			});
		}

		try {
			serverState.logSystem.writeLog("[PAXOS (" + seqNum + ")]\t\tWaiting for all threads to finish.");
			latch.await();
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			e.printStackTrace();
		} finally {
			serverState.logSystem.writeLog("[PAXOS (" + seqNum + ")]\t\tAll threads done");
			executor.shutdown();
		}

		final int responsesNeeded = serverState.getQuorum(serverState.acceptors.length);
		try {
			collector.waitForTarget(responsesNeeded);
		} catch (final RuntimeException e) {
			serverState.logSystem.writeLog("Exception occurred during Phase 1: " + e.getCause().getMessage());
		}

		if (promiseResponses.size() >= responsesNeeded) {
			final boolean hasNaks = promiseResponses.stream().anyMatch(response -> !response.getPhase1Accepted());
			if (!hasNaks) {
				paxosInstancesHashMap.get(seqNum).reqId.set(extractHighestProposedValue(promiseResponses, seqNum));
				if (paxosInstancesHashMap.get(seqNum).reqId.get() == -1) {
					paxosInstancesHashMap.get(seqNum).reqId.set(_requestQueue.poll());
					serverState.logSystem.writeLog("[PAXOS (" + seqNum + ")]\t\tREMOVED " + paxosInstancesHashMap.get(seqNum).reqId.get() + " FROM THE QUEUE");
				}
				serverState.logSystem
						.writeLog("[PAXOS (" + seqNum + ")]\t\tFINISHED PHASE ONE WITH REQUEST ID " + paxosInstancesHashMap.get(seqNum).reqId.get());
				return true;
			}
			serverState.logSystem.writeLog("[PAXOS (" + seqNum + ")]\t\tSetting new timestamp");
			setNewTimestamp(seqNum);
		} else {
			serverState.logSystem.writeLog("Error: didn't get enough responses from the quorum in Phase 1.");
			System.out.println("Error: didn't get enough responses from the quorum in Phase 1.");
		}
		return false;
	}

	private boolean runPhaseTwo(final int seqNum) {
		serverState.logSystem
				.writeLog("[PAXOS (" + seqNum + ")]\t\tSTARTING PHASE TWO.");

		serverState.logSystem
				.writeLog("[PAXOS (" + seqNum + ")]\t\tSENDING ACCEPT - " + "Value: " + paxosInstancesHashMap.get(seqNum).reqId.get() + " Priority: " + paxosInstancesHashMap.get(seqNum).timestamp.get());

		final DadkvsPaxos.PhaseTwoRequest.Builder accept = DadkvsPaxos.PhaseTwoRequest.newBuilder()
				.setPhase2Config(serverState.getConfiguration())
				.setPhase2Index(seqNum)
				.setPhase2Value(paxosInstancesHashMap.get(seqNum).reqId.get())
				.setPhase2Timestamp(paxosInstancesHashMap.get(seqNum).timestamp.get());

		final ArrayList<DadkvsPaxos.PhaseTwoReply> acceptedResponses = new ArrayList<>();
		final GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> collector = new GenericResponseCollector<>(
				acceptedResponses,
				serverState.acceptors.length);

		final int nAcceptors = serverState.acceptors.length;
		final CountDownLatch latch = new CountDownLatch(nAcceptors);
		final ExecutorService executor = Executors.newFixedThreadPool(nAcceptors);

		for (final int acceptor : serverState.acceptors) {
			executor.submit(() -> {
				try {
					final CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply> observer = new CollectorStreamObserver<>(collector);
					asyncStubs[acceptor].phasetwo(accept.build(), observer);
				} catch (final RuntimeException e) {
					serverState.logSystem.writeLog(
							"Exception occurred while sending Phase 2 request to acceptor " + acceptor + ": " + e.getMessage());
				} finally {
					latch.countDown();
				}
			});
		}

		try {
			serverState.logSystem.writeLog("[PAXOS (" + seqNum + ")]\t\tWaiting for all threads to finish.");
			latch.await();
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			e.printStackTrace();
		} finally {
			serverState.logSystem.writeLog("[PAXOS (" + seqNum + ")]\t\tAll threads done");
			executor.shutdown();
		}

		final int responsesNeeded = serverState.getQuorum(serverState.acceptors.length);
		try {
			collector.waitForTarget(responsesNeeded);
		} catch (final RuntimeException e) {
			serverState.logSystem.writeLog("Exception occurred during Phase 2: " + e.getCause().getMessage());
		}

		if (acceptedResponses.size() >= responsesNeeded) {
			final boolean hasNaks = acceptedResponses.stream().anyMatch(response -> !response.getPhase2Accepted());
			if (!hasNaks) {
				serverState.logSystem
						.writeLog("[PAXOS (" + seqNum + ")]\t\tFINISHED PHASE TWO.");
				return true;
			}
			setNewTimestamp(seqNum);
		} else {
			serverState.logSystem.writeLog("Error: didn't get enough responses from the quorum in Phase 2.");
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
			serverState.logSystem.writeLog("[PAXOS (" + seqNum + ")]\t\tResponse: " + i + " TimeStamp: "
					+ response.getPhase1Timestamp() + " Value: " + response.getPhase1Value());
			i++;
		}

		serverState.logSystem.writeLog(
				"[PAXOS (" + seqNum + ")]\t\tHighest Proposed Value: " + value + " Highest Time Stamp: " + highestTimestamp);
		return value;
	}
}
