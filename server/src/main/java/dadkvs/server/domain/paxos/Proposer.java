package dadkvs.server.domain.paxos;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import dadkvs.DadkvsPaxos;
import dadkvs.server.domain.ServerState;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;

public class Proposer extends Acceptor {
	private final Sequencer _sequencer;
	private final ConcurrentLinkedQueue<Integer> _requestQueue;
	private int _priority;
	private int _reqId;

	public Proposer() {
		_requestQueue = new ConcurrentLinkedQueue<>();
		_reqId = -1;
		_sequencer = new Sequencer();
	}

	public void setServerState(final ServerState serverState) {
		_serverState = serverState;
		_priority = _serverState.myId(); // So that server 0 has the lowest and 4 has the highest base priority
		initPaxosComms();
		_serverState.requestCancellation();
	}

	// public void handlePromiseRequest();
	public void handleCommittx(final int reqId) {
		_serverState.logSystem.writeLog("Handling commit request with Request ID: " + reqId);
		_requestQueue.add(reqId);
		final int seqNumber = _sequencer.getSequenceNumber();
		runPaxos(seqNumber);
	}

	public boolean runPaxos(final int seqNum) {
		_serverState.logSystem.writeLog("Starting [PAXOS(" + seqNum + ")]...");
		try {
			while (true) {
				if (!runPhaseOne(seqNum)) {
					continue;
				}
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

	public void promote() {
		// proposer can't be promoted
	}

	public void demote() {
		terminateComms();
		_serverState.changePaxosState(new Acceptor());
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
					asyncStubs[acceptor].phaseone(prepare.build(), observer);
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
				_reqId = extractHighestProposedValue(promiseResponses, seqNum);
				if (_reqId == -1) {
					_reqId = _requestQueue.poll();
					_serverState.logSystem.writeLog("[PAXOS (" + seqNum + ")]\t\tREMOVED " + _reqId + " FROM THE QUEUE");
				}
				_serverState.logSystem
						.writeLog("[PAXOS (" + seqNum + ")]\t\tFINISHED PHASE ONE WITH REQUEST ID " + _reqId);
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
				.writeLog("[PAXOS (" + seqNum + ")]\t\tSENDING ACCEPT - " + "Value: " + _reqId + " Priority: " + _priority);

		final DadkvsPaxos.PhaseTwoRequest.Builder accept = DadkvsPaxos.PhaseTwoRequest.newBuilder()
				.setPhase2Config(_serverState.getConfiguration())
				.setPhase2Index(seqNum)
				.setPhase2Value(_reqId)
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
					asyncStubs[acceptor].phasetwo(accept.build(), observer);
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
}
