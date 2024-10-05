package dadkvs.server.domain;

import dadkvs.DadkvsMain;
import dadkvs.server.DadkvsServerState;
import dadkvs.server.TransactionRecord;
import dadkvs.DadkvsPaxos;
import java.util.ArrayList;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Acceptor extends PaxosState {
	private int acceptedValue = -1;
	private int n_servers = 5;
	ManagedChannel[] channels;
	private DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] async_stubs;

	public void setServerState(DadkvsServerState serverState) {
		_serverState = serverState;
		initPaxosComms();
	}

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
					.writeLog("[PAXOS (" + proposalIndex + ")] Proposer's timestamp was higher/equal to ours:\tAccepting.");

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

	public void handleCommittx(int reqid) {
		// Acceptor does nothing
	}

	public void promote() {
		terminateComms();
		_serverState.changePaxosState(new Proposer());

	}

	public void demote() {
		terminateComms();
		_serverState.changePaxosState(new Learner());
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
