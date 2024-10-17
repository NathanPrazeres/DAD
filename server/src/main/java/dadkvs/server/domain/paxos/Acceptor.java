package dadkvs.server.domain.paxos;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.server.domain.ServerState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Acceptor extends PaxosState {
	// (Index, Value)
	private final ConcurrentHashMap<Integer, Integer> _paxosInstance = new ConcurrentHashMap<>();
	private final int nServers = 5;
	private int _priority;
	ManagedChannel[] channels;
	public DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] asyncStubs;

	public void setServerState(final ServerState serverState) {
		_serverState = serverState;
		_priority = _serverState.myId(); // So that server 0 has the lowest and 4 has the highest base priority
		initPaxosComms();
	}

	public DadkvsPaxos.PhaseOneReply handlePrepareRequest(final DadkvsPaxos.PhaseOneRequest request) {
		final int proposalConfig = request.getPhase1Config();
		final int proposalIndex = request.getPhase1Index();
		final int proposalTimestamp = request.getPhase1Timestamp();

		_serverState.logSystem.writeLog("[PAXOS (" + proposalIndex + ")]\t\tRECEIVED PREPARE.");

		if (proposalTimestamp > highestTimestamp) {
			_serverState.logSystem
					.writeLog("[PAXOS (" + proposalIndex + ")]\t\tProposer's timestamp was higher than ours: " + proposalTimestamp
							+ "\tAccepting.");
			// accept value
			highestTimestamp = proposalTimestamp;

			if (_paxosInstance.get(proposalIndex) == null) {
				_paxosInstance.put(proposalIndex, -1);
			}

			_serverState.logSystem
					.writeLog("[PAXOS (" + proposalIndex + ")] Replying with value: " + _paxosInstance.get(proposalIndex));

			final DadkvsPaxos.PhaseOneReply response = DadkvsPaxos.PhaseOneReply.newBuilder()
					.setPhase1Config(proposalConfig)
					.setPhase1Index(proposalIndex)
					.setPhase1Accepted(true)
					.setPhase1Value(_paxosInstance.get(proposalIndex))
					.setPhase1Timestamp(highestTimestamp)
					.build();

			return response;
		} else {
			_serverState.logSystem
					.writeLog("[PAXOS (" + proposalIndex + ")] Proposer's timestamp was lower than ours:\tRejecting.");
			// reject value
			final DadkvsPaxos.PhaseOneReply response = DadkvsPaxos.PhaseOneReply.newBuilder()
					.setPhase1Config(proposalConfig)
					.setPhase1Index(proposalIndex)
					.setPhase1Accepted(false)
					.build();

			return response;
		}
	}

	public DadkvsPaxos.PhaseTwoReply handleAcceptRequest(final DadkvsPaxos.PhaseTwoRequest request) {
		final int proposalConfig = request.getPhase2Config();
		final int proposalIndex = request.getPhase2Index();
		final int proposalValue = request.getPhase2Value();
		final int proposalTimestamp = request.getPhase2Timestamp();

		_serverState.logSystem.writeLog("[PAXOS (" + proposalIndex + ")]\t\tRECEIVED ACCEPT.");

		// If the proposal's timestamp is higher than the highest seen, accept it
		if (proposalTimestamp >= _priority) {

			_serverState.logSystem
					.writeLog("[PAXOS (" + proposalIndex + ")] Proposer's timestamp was higher/equal to ours:\tAccepting.");

			_priority = proposalTimestamp;
			_paxosInstance.put(proposalIndex, proposalValue);

			// Respond that the proposal was accepted
			final DadkvsPaxos.PhaseTwoReply response = DadkvsPaxos.PhaseTwoReply.newBuilder()
					.setPhase2Config(proposalConfig)
					.setPhase2Index(proposalIndex)
					.setPhase2Accepted(true)
					.build();

			// acceptors should trigger the learn request once they accept a value
			sendLearnRequest(proposalIndex, _priority, _paxosInstance.get(proposalIndex), asyncStubs);

			return response;
		} else {
			_serverState.logSystem
					.writeLog("[PAXOS (" + proposalIndex + ")] Proposer's timestamp was lower than ours:\tRejecting.");
			// Reject if a higher proposal has already been seen
			final DadkvsPaxos.PhaseTwoReply response = DadkvsPaxos.PhaseTwoReply.newBuilder()
					.setPhase2Config(proposalConfig)
					.setPhase2Index(proposalIndex)
					.setPhase2Accepted(false)
					.build();

			return response;
		}
	}

	public void handleCommittx(final int reqId) {
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

	public void initPaxosComms() {
		// set servers
		final String[] targets = new String[nServers];

		for (int i = 0; i < nServers; i++) {
			final int targetPort = _serverState.basePort + i;
			targets[i] = new String();
			targets[i] = "localhost:" + targetPort;
			System.out.printf("targets[%d] = %s%n", i, targets[i]);
		}

		channels = new ManagedChannel[nServers];

		for (int i = 0; i < nServers; i++) {
			channels[i] = ManagedChannelBuilder.forTarget(targets[i]).usePlaintext().build();
		}

		asyncStubs = new DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[nServers];

		for (int i = 0; i < nServers; i++) {
			asyncStubs[i] = DadkvsPaxosServiceGrpc.newStub(channels[i]);
		}
		_serverState.logSystem.writeLog("Opened Stubs for PAXOS communication.");
	}

	public void terminateComms() {
		_serverState.logSystem.writeLog("Initiating graceful shutdown of gRPC channels...");

		for (int i = 0; i < nServers; i++) {
			channels[i].shutdown();
		}
		for (int i = 0; i < nServers; i++) {
			try {
				if (!channels[i].awaitTermination(30, TimeUnit.SECONDS)) {
					_serverState.logSystem.writeLog("Forcing shutdown of gRPC channel: " + i);
					channels[i].shutdownNow();
				}
			} catch (InterruptedException e) {
				_serverState.logSystem.writeLog("Interrupted while waiting for channel shutdown: " + i);
				channels[i].shutdownNow();
				Thread.currentThread().interrupt();
			}
		}
		_serverState.logSystem.writeLog("Closed Stubs for PAXOS communication.");
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
			demote();
		}
	}
}
