package dadkvs.server.domain;

import java.util.concurrent.atomic.AtomicInteger;

import dadkvs.DadkvsMain;
import dadkvs.server.DadkvsServerState;
import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;

public abstract class PaxosState {

	int highestTimestamp = -1;
	AtomicInteger num_responses;
	DadkvsServerState _serverState;

	public abstract DadkvsPaxos.PhaseTwoReply handleAcceptRequest(DadkvsPaxos.PhaseTwoRequest request);

	public abstract DadkvsPaxos.PhaseOneReply handlePrepareRequest(DadkvsPaxos.PhaseOneRequest request);

	public DadkvsPaxos.LearnReply handleLearnRequest(DadkvsPaxos.LearnRequest request) {

		int learnConfig = request.getLearnconfig();
		int learnIndex = request.getLearnindex();
		int learnReqid = request.getLearnvalue();
		int learnTimestamp = request.getLearntimestamp();

		_serverState.logSystem.writeLog("[PAXOS (" + learnIndex + ")]\t\tRECEIVED ACCEPT.");

		if (highestTimestamp == -1 || learnTimestamp > highestTimestamp) {
			_serverState.logSystem
					.writeLog("[PAXOS (" + learnIndex + ")] Received request with higher timestamp than ours:\tResetting;");

			num_responses = new AtomicInteger(0);
		} else if (highestTimestamp > learnTimestamp) {
			_serverState.logSystem
					.writeLog("[PAXOS (" + learnIndex + ")] Received request with lower timestamp than ours:\tRejecting");
		} else {
			_serverState.logSystem
					.writeLog("[PAXOS (" + learnIndex + ")] Received request with timestamp equal to ours:\tAccepting.");
		}

		return DadkvsPaxos.LearnReply.newBuilder()
				.setLearnconfig(learnConfig)
				.setLearnindex(learnIndex)
				.setLearnaccepted(true)
				.build();
	}

	public abstract void handleCommittx(int reqid);

	public abstract void setServerState(DadkvsServerState serverState);

	public abstract void promote();

	public abstract void demote();

	public void resetResponseCounter() {
		num_responses = new AtomicInteger(0);
	}

	public void addResponseToCounter() {
		num_responses.incrementAndGet();
	}
}
