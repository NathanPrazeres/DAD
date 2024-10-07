package dadkvs.server.domain;

import dadkvs.server.DadkvsServerState;
import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.ConcurrentHashMap;

public class Acceptor extends PaxosState {
						    // (Index, Value)
	private ConcurrentHashMap<Integer, Integer> _paxosInstance  = new ConcurrentHashMap<>();
	private int n_servers = 5;
	private int _priority;
	ManagedChannel[] channels;
	public DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] async_stubs;

	public void setServerState(DadkvsServerState serverState) {
		this.serverState = serverState;
		_priority = this.serverState.myId(); // So that server 0 has the lowest and 4 has the highest base priority
		initPaxosComms();
	}

	public DadkvsPaxos.PhaseOneReply handlePrepareRequest(DadkvsPaxos.PhaseOneRequest request) {
		int proposalConfig = request.getPhase1Config();
		int proposalIndex = request.getPhase1Index();
		int proposalTimestamp = request.getPhase1Timestamp();

		this.serverState.logSystem.writeLog("[PAXOS (" + proposalIndex + ")]\t\tRECEIVED PREPARE.");

		if (proposalTimestamp > highestTimestamp) {
			this.serverState.logSystem
					.writeLog("[PAXOS (" + proposalIndex + ")]\t\tProposer's timestamp was higher than ours: " + proposalTimestamp + "\tAccepting.");
			// accept value
			highestTimestamp = proposalTimestamp;

			if (_paxosInstance.get(proposalIndex) == null) {
				_paxosInstance.put(proposalIndex, -1);
			}

			this.serverState.logSystem
					.writeLog("[PAXOS (" + proposalIndex + ")] Replying with value: " + _paxosInstance.get(proposalIndex));

			DadkvsPaxos.PhaseOneReply response = DadkvsPaxos.PhaseOneReply.newBuilder()
					.setPhase1Config(proposalConfig)
					.setPhase1Index(proposalIndex)
					.setPhase1Accepted(true)
					.setPhase1Value(_paxosInstance.get(proposalIndex))
					.setPhase1Timestamp(highestTimestamp)
					.build();


			return response;
		} else {
			this.serverState.logSystem
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

		this.serverState.logSystem.writeLog("[PAXOS (" + proposalIndex + ")]\t\tRECEIVED ACCEPT.");

		// If the proposal's timestamp is higher than the highest seen, accept it
		if (proposalTimestamp >= _priority) {

			this.serverState.logSystem
					.writeLog("[PAXOS (" + proposalIndex + ")] Proposer's timestamp was higher/equal to ours:\tAccepting.");

			_priority = proposalTimestamp;
			_paxosInstance.put(proposalIndex, proposalValue);

			// Respond that the proposal was accepted
			DadkvsPaxos.PhaseTwoReply response = DadkvsPaxos.PhaseTwoReply.newBuilder()
					.setPhase2Config(proposalConfig)
					.setPhase2Index(proposalIndex)
					.setPhase2Accepted(true)
					.build();

			// acceptors should trigger the learn request once they accept a value
			sendLearnRequest(proposalIndex, _priority, _paxosInstance.get(proposalIndex), async_stubs);

			return response;
		} else {
			this.serverState.logSystem
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

	public void handleCommittx(int reqid) {
		// Acceptor does nothing
	}

	public void promote() {
		terminateComms();
		this.serverState.changePaxosState(new Proposer());

	}

	public void demote() {
		terminateComms();
		this.serverState.changePaxosState(new Learner());
	}

	public void initPaxosComms() {
		// set servers
		String[] targets = new String[n_servers];

		for (int i = 0; i < n_servers; i++) {
			int target_port = this.serverState.base_port + i;
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
		this.serverState.logSystem.writeLog("Opened Stubs for PAXOS communication.");
	}

	public void terminateComms() {
		for (int i = 0; i < n_servers; i++) {
			channels[i].shutdownNow();
		}
		this.serverState.logSystem.writeLog("Closed Stubs for PAXOS communication.");
	}
}
