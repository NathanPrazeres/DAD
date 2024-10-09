package dadkvs.server.domain;

import dadkvs.DadkvsPaxos;
import dadkvs.server.DadkvsServerState;
import dadkvs.server.Sequencer;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.ArrayList;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import io.grpc.ManagedChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Proposer extends Acceptor {
	private Sequencer _sequencer;
	private ConcurrentLinkedQueue<Integer> _requestQueue;
	private int _priority;
	private int _reqId;
	ManagedChannel[] channels;

	public Proposer() {
		_requestQueue = new ConcurrentLinkedQueue<>();
		_reqId = -1;
		_sequencer = new Sequencer();
	}

	public void setServerState(DadkvsServerState serverState) {
		this.serverState = serverState;
		_priority = this.serverState.myId(); // So that server 0 has the lowest and 4 has the highest base priority
		initPaxosComms();
	}

	// public void handlePromiseRequest();
	public void handleCommittx(int reqId) {
		this.serverState.logSystem.writeLog("Handling commit request with Request ID: " + reqId);
		_requestQueue.add(reqId);
		int seqNumber = _sequencer.getSequenceNumber();
		runPaxos(seqNumber);
	}

	private void setNewTimestamp() {
		_priority += this.serverState.getNumberOfServers();
	}

	public boolean runPaxos(int seqNum) {
		this.serverState.logSystem.writeLog("Starting [PAXOS(" + seqNum + ")]...");
		try {
			while (true) {
				if (!runPhaseOne(seqNum)) {
					continue;
				}
				if (!runPhaseTwo(seqNum)) {
					continue;
				}

				this.serverState.logSystem.writeLog("Ending [PAXOS(" + seqNum + ")]...");
				break;
			}
		} catch (RuntimeException e) {
			this.serverState.logSystem.writeLog("Exception occurred while running Paxos: " + e.getMessage());
			return false;
		}
		return true;
	}

	public int extractHighestProposedValue(ArrayList<DadkvsPaxos.PhaseOneReply> phase1Responses, int seqNum) {
		int value = -1;
		int i = 0;
		int highestTimestamp = -1;
		for (DadkvsPaxos.PhaseOneReply response : phase1Responses) {
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

	private boolean runPhaseOne(int seqNum) {
		this.serverState.logSystem
				.writeLog("[PAXOS (" + seqNum + ")]\t\tSTARTING PHASE ONE.");
		int[] acceptors = new int[] { 0, 1, 2 };

		this.serverState.logSystem
				.writeLog("[PAXOS (" + seqNum + ")]\t\tSENDING PREPARES.");

		DadkvsPaxos.PhaseOneRequest.Builder prepare = DadkvsPaxos.PhaseOneRequest.newBuilder()
				.setPhase1Index(seqNum)
				.setPhase1Config(this.serverState.getConfiguration())
				.setPhase1Timestamp(_priority);
		ArrayList<DadkvsPaxos.PhaseOneReply> promiseResponses = new ArrayList<>();
		GenericResponseCollector<DadkvsPaxos.PhaseOneReply> collector = new GenericResponseCollector<>(promiseResponses,
				acceptors.length);

		int nAcceptors = acceptors.length;
		CountDownLatch latch = new CountDownLatch(nAcceptors);
		ExecutorService executor = Executors.newFixedThreadPool(nAcceptors);

		for (int acceptor : acceptors) {
			executor.submit(() -> {
				try {
					CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> observer = new CollectorStreamObserver<>(collector);
					asyncStubs[acceptor].phaseone(prepare.build(), observer);
				} catch (RuntimeException e) {
					this.serverState.logSystem.writeLog(
							"Exception occurred while sending Prepare request to Acceptor " + acceptor + ": " + e.getMessage());
				} finally {
					latch.countDown();
				}
			});
		}

		try {
			latch.await();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			e.printStackTrace();
		} finally {
			executor.shutdown();
		}

		int responsesNeeded = this.serverState.getQuorum(acceptors.length);
		try {
			collector.waitForTarget(responsesNeeded);
		} catch (RuntimeException e) {
			this.serverState.logSystem.writeLog("Exception occurred during Phase 1: " + e.getCause().getMessage());
		}

		if (promiseResponses.size() >= responsesNeeded) {
			boolean hasNaks = promiseResponses.stream().anyMatch(response -> !response.getPhase1Accepted());
			if (!hasNaks) {
				_reqId = extractHighestProposedValue(promiseResponses, seqNum);
				if (_reqId == -1) {
					_reqId = _requestQueue.poll();
					this.serverState.logSystem.writeLog("[PAXOS (" + seqNum + ")]\t\tREMOVED " + _reqId + " FROM THE QUEUE");
				}
				this.serverState.logSystem
						.writeLog("[PAXOS (" + seqNum + ")]\t\tFINISHED PHASE ONE WITH REQUEST ID " + _reqId);
				return true;
			}
			setNewTimestamp();
		} else {
			this.serverState.logSystem.writeLog("Error: didn't get enough responses from the quorum in Phase 1.");
			System.out.println("Error: didn't get enough responses from the quorum in Phase 1.");
		}
		return false;
	}

	private boolean runPhaseTwo(int seqNum) {
		this.serverState.logSystem
				.writeLog("[PAXOS (" + seqNum + ")]\t\tSTARTING PHASE TWO.");
		int[] acceptors = new int[] { 0, 1, 2 };

		this.serverState.logSystem
				.writeLog("[PAXOS (" + seqNum + ")]\t\tSENDING ACCEPT - " + "Value: " + _reqId + " Priority: " + _priority);

		DadkvsPaxos.PhaseTwoRequest.Builder accept = DadkvsPaxos.PhaseTwoRequest.newBuilder()
				.setPhase2Config(this.serverState.getConfiguration())
				.setPhase2Index(seqNum)
				.setPhase2Value(_reqId)
				.setPhase2Timestamp(_priority);

		ArrayList<DadkvsPaxos.PhaseTwoReply> acceptedResponses = new ArrayList<>();
		GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> collector = new GenericResponseCollector<>(acceptedResponses,
				acceptors.length);

		for (int acceptor : acceptors) {
			try {
				CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply> observer = new CollectorStreamObserver<>(collector);
				asyncStubs[acceptor].phasetwo(accept.build(), observer);
			} catch (RuntimeException e) {
				this.serverState.logSystem.writeLog(
						"Exception occurred while sending Phase 2 request to acceptor " + acceptor + ": " + e.getMessage());
			}
		}

		int responsesNeeded = this.serverState.getQuorum(acceptors.length);
		try {
			collector.waitForTarget(responsesNeeded);
		} catch (RuntimeException e) {
			this.serverState.logSystem.writeLog("Exception occurred during Phase 2: " + e.getCause().getMessage());
		}

		if (acceptedResponses.size() >= responsesNeeded) {
			boolean hasNaks = acceptedResponses.stream().anyMatch(response -> !response.getPhase2Accepted());
			if (!hasNaks) {
				this.serverState.logSystem
						.writeLog("[PAXOS (" + seqNum + ")]\t\tFINISHED PHASE TWO.");
				return true;
			}
			setNewTimestamp();
		} else {
			this.serverState.logSystem.writeLog("Error: didn't get enough responses from the quorum in Phase 2.");
			System.out.println("Error: didn't get enough responses from the quorum in Phase 2.");
		}
		return false;
	}

	public void promote() {
		// proposer can't be promoted
	}

	public void demote() {
		terminateComms();
		this.serverState.changePaxosState(new Acceptor());
	}
}
