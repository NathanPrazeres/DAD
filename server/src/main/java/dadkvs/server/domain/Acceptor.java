package dadkvs.server.domain;

import dadkvs.DadkvsMain;
import dadkvs.server.DadkvsServerState;
import dadkvs.server.TransactionRecord;
import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;

public class Acceptor extends PaxosState {
	private int acceptedValue = -1;

	public void setServerState(DadkvsServerState serverState) {
		_serverState = serverState;
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

			_serverState.logSystem
					.writeLog("[PAXOS (" + proposalIndex + ")] Sending Learn request to all Learners.");
			// acceptors should trigger the learn request once they accept a value
			// boolean learned = sendLearnRequests(proposalIndex);

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

	public void handleCommittx(int reqid) {
		// Acceptor does nothing
	}

	public void promote() {
		_serverState.changePaxosState(new Proposer());

	}

	public void demote() {
		_serverState.changePaxosState(new Learner());
	}
}
