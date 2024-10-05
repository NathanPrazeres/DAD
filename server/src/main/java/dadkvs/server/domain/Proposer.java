package dadkvs.server.domain;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsPaxos;
import dadkvs.server.DadkvsServerState;
import dadkvs.server.TransactionRecord;
import dadkvs.server.Sequencer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.ArrayList;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Proposer extends PaxosState {
	private Sequencer _sequencer;
	private ConcurrentLinkedQueue<Integer> _requestQueue;
	private int _priority;
	private int _reqId;
	private DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] async_stubs;
	private int n_servers = 5;
	private int acceptedValue = -1;
	ManagedChannel[] channels;

	public Proposer() {
		_requestQueue = new ConcurrentLinkedQueue<>();
		_reqId = -1;
		_sequencer = new Sequencer();
	}

	public void setServerState(DadkvsServerState serverState) {
		_serverState = serverState;
		_priority = _serverState.myId();
		initPaxosComms();
	}

	// public void handleLearnRequest();

	public DadkvsPaxos.PhaseOneReply handlePrepareRequest(DadkvsPaxos.PhaseOneRequest request) {
		int proposalConfig = request.getPhase1Config();
		int proposalIndex = request.getPhase1Index();
		int proposalTimestamp = request.getPhase1Timestamp();

		_serverState.logSystem.writeLog("[PAXOS (" + proposalIndex + ")]\t\tRECEIVED PREPARE.");

		if (proposalTimestamp > highestTimestamp) {
			_serverState.logSystem
					.writeLog("[PAXOS (" + proposalIndex + ")] Proposer's timestamp was higher than ours:\tAccepting.");
			// accept value
			DadkvsPaxos.PhaseOneReply response = DadkvsPaxos.PhaseOneReply.newBuilder()
					.setPhase1Config(proposalConfig)
					.setPhase1Index(proposalIndex)
					.setPhase1Accepted(true)
					.setPhase1Value(acceptedValue)
					.setPhase1Timestamp(highestTimestamp)
					.build();

			highestTimestamp = proposalTimestamp;

			return response;
		} else {
			_serverState.logSystem
					.writeLog("[PAXOS (" + proposalIndex + ")] Proposer's timestamp was lower than ours:\tRejecting.");
			// reject value
			DadkvsPaxos.PhaseOneReply response = DadkvsPaxos.PhaseOneReply.newBuilder()
					.setPhase1Config(proposalConfig)
					.setPhase1Index(proposalIndex)
					.setPhase1Accepted(false)
					.build();

			return response;
		}
	}

	public DadkvsPaxos.PhaseTwoReply handleAcceptRequest(DadkvsPaxos.PhaseTwoRequest request) {
		int proposalConfig = request.getPhase2Config();
		int proposalIndex = request.getPhase2Index();
		int proposalValue = request.getPhase2Value();
		int proposalTimestamp = request.getPhase2Timestamp();

		_serverState.logSystem.writeLog("[PAXOS (" + proposalIndex + ")]\t\tRECEIVED ACCEPT.");

		// If the proposal's timestamp is higher than the highest seen, accept it
		if (proposalTimestamp >= highestTimestamp) {
			_serverState.logSystem
					.writeLog("[PAXOS (" + proposalIndex + ")] Proposer's timestamp was higher than ours:\tAccepting.");

			highestTimestamp = proposalTimestamp;
			acceptedValue = proposalValue;

			// Respond that the proposal was accepted
			DadkvsPaxos.PhaseTwoReply response = DadkvsPaxos.PhaseTwoReply.newBuilder()
					.setPhase2Config(proposalConfig)
					.setPhase2Index(proposalIndex)
					.setPhase2Accepted(true)
					.build();

			// acceptors should trigger the learn request once they accept a value
			sendLearnRequest(proposalIndex);

			return response;
		} else {
			_serverState.logSystem
					.writeLog("[PAXOS (" + proposalIndex + ")] Proposer's timestamp was lower than ours:\tRejecting.");
			// Reject if a higher proposal has already been seen
			DadkvsPaxos.PhaseTwoReply response = DadkvsPaxos.PhaseTwoReply.newBuilder()
					.setPhase2Config(proposalConfig)
					.setPhase2Index(proposalIndex)
					.setPhase2Accepted(false)
					.build();

			return response;
		}
	}

	// public void handlePromiseRequest();
	public void handleCommittx(int reqId) {
		_requestQueue.add(reqId);
		int seqNumber = _sequencer.getSequenceNumber();
		runPaxos(seqNumber);
	}

	private void setNewTimestamp() {
		_priority = _serverState.getNumberOfServers();
	}

	public boolean runPaxos(int seqNum) {
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
		} catch (RuntimeException e) {
			_serverState.logSystem.writeLog("Exception occurred while running Paxos: " + e.getMessage());
			return false;
		}
		return true;
	}

	private int extractHighestSeqNum(ArrayList<DadkvsPaxos.PhaseOneReply> responses) {
		return responses.stream()
				.mapToInt(DadkvsPaxos.PhaseOneReply::getPhase1Index)
				.max()
				.orElse(-1);
	}

	private boolean runPhaseOne(int seqNum) {
		_serverState.logSystem
				.writeLog("[PAXOS (" + seqNum + ")]\t\tSTARTING PHASE ONE.");
		int[] acceptors = new int[] { 0, 1, 2 };

		DadkvsPaxos.PhaseOneRequest.Builder prepare = DadkvsPaxos.PhaseOneRequest.newBuilder()
				.setPhase1Index(seqNum)
				.setPhase1Config(_serverState.getConfiguration())
				.setPhase1Timestamp(_priority);
		ArrayList<DadkvsPaxos.PhaseOneReply> promise_responses = new ArrayList<>();
		GenericResponseCollector<DadkvsPaxos.PhaseOneReply> collector = new GenericResponseCollector<>(promise_responses,
				acceptors.length);

		for (int acceptor : acceptors) {
			try {
				CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> observer = new CollectorStreamObserver<>(collector);
				async_stubs[acceptor].phaseone(prepare.build(), observer);
			} catch (RuntimeException e) {
				_serverState.logSystem.writeLog(
						"Exception occurred while sending Prepare request to Acceptor " + acceptor + ": " + e.getMessage());
			}
		}

		int responsesNeeded = _serverState.getQuorum(acceptors.length);
		try {
			collector.waitForTarget(responsesNeeded);
		} catch (RuntimeException e) {
			_serverState.logSystem.writeLog("Exception occurred during Phase 1: " + e.getCause().getMessage());
		}

		if (promise_responses.size() >= responsesNeeded) {
			boolean hasNaks = promise_responses.stream().anyMatch(response -> !response.getPhase1Accepted());
			if (!hasNaks) {
				_reqId = extractHighestSeqNum(promise_responses);
				if (_reqId == -1) {
					_reqId = _requestQueue.poll();
				}
				_serverState.logSystem
						.writeLog("[PAXOS (" + seqNum + ")]\t\tFINISHED PHASE ONE.");
				return true;
			}
			setNewTimestamp();
		} else {
			_serverState.logSystem.writeLog("Error: didn't get enough responses from the quorum in Phase 1.");
			System.out.println("Error: didn't get enough responses from the quorum in Phase 1.");
		}
		return false;
	}

	private boolean runPhaseTwo(int seqNum) {
		_serverState.logSystem
				.writeLog("[PAXOS (" + seqNum + ")]\t\tSTARTING PHASE TWO.");
		int[] acceptors = new int[] { 0, 1, 2 };

		DadkvsPaxos.PhaseTwoRequest.Builder accept = DadkvsPaxos.PhaseTwoRequest.newBuilder()
				.setPhase2Config(_serverState.getConfiguration())
				.setPhase2Config(seqNum)
				.setPhase2Index(_reqId)
				.setPhase2Timestamp(_priority);

		ArrayList<DadkvsPaxos.PhaseTwoReply> accepted_responses = new ArrayList<>();
		GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> collector = new GenericResponseCollector<>(accepted_responses,
				acceptors.length);

		for (int acceptor : acceptors) {
			try {
				CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply> observer = new CollectorStreamObserver<>(collector);
				async_stubs[acceptor].phasetwo(accept.build(), observer);
			} catch (RuntimeException e) {
				_serverState.logSystem.writeLog(
						"Exception occurred while sending Phase 2 request to acceptor " + acceptor + ": " + e.getMessage());
			}
		}

		int responsesNeeded = _serverState.getQuorum(acceptors.length);
		try {
			collector.waitForTarget(responsesNeeded);
		} catch (RuntimeException e) {
			_serverState.logSystem.writeLog("Exception occurred during Phase 2: " + e.getCause().getMessage());
		}

		if (accepted_responses.size() >= responsesNeeded) {
			boolean hasNaks = accepted_responses.stream().anyMatch(response -> !response.getPhase2Accepted());
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

	public void sendLearnRequest(int paxosIndex) {
		// DadkvsPaxos.LearnRequest request = DadkvsPaxos.LearnRequest.newBuilder();

		_serverState.logSystem
				.writeLog("[PAXOS (" + paxosIndex + ")]\t\tSTARTING LEARN PHASE.");

		_serverState.logSystem
				.writeLog("[PAXOS (" + paxosIndex + ")] Sending Learn request to all Learners.");

		_serverState.logSystem
				.writeLog("[PAXOS (" + paxosIndex + ")] Sending learn requests is still unimplemented.");

		_serverState.logSystem
				.writeLog("[PAXOS (" + paxosIndex + ")]\t\tENDING LEARN PHASE.");
	}

	public void promote() {
		// proposer can't be promoted
	}

	public void demote() {
		terminateComms();
		_serverState.changePaxosState(new Acceptor());
	}

	private void initPaxosComms() {
		// set servers
		String[] targets = new String[n_servers];

		for (int i = 0; i < n_servers; i++) {
			int target_port = _serverState.base_port + i;
			targets[i] = new String();
			targets[i] = "localhost:" + target_port;
			System.out.printf("targets[%d] = %s%n", i, targets[i]);
		}

		channels = new ManagedChannel[n_servers];

		for (int i = 0; i < n_servers; i++) {
			channels[i] = ManagedChannelBuilder.forTarget(targets[i]).usePlaintext().build();
		}

		async_stubs = new DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[n_servers];

		for (int i = 0; i < n_servers; i++) {
			async_stubs[i] = DadkvsPaxosServiceGrpc.newStub(channels[i]);
		}
		_serverState.logSystem.writeLog("Opened Stubs for PAXOS communication.");
	}

	private void terminateComms() {
		for (int i = 0; i < n_servers; i++) {
			channels[i].shutdownNow();
		}
		_serverState.logSystem.writeLog("Closed Stubs for PAXOS communication.");
	}
}
