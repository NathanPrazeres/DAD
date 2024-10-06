package dadkvs.server.domain;

import dadkvs.DadkvsMain;
import dadkvs.server.DadkvsServerState;
import dadkvs.server.TransactionRecord;
import dadkvs.DadkvsPaxos;

public class Learner extends PaxosState {
	private int highestTimestamp = -1;

	public void setServerState(DadkvsServerState serverState) {
		this.serverState = serverState;
	}

	public DadkvsPaxos.PhaseOneReply handlePrepareRequest(DadkvsPaxos.PhaseOneRequest request) {
		return null;
	}

	public DadkvsPaxos.PhaseTwoReply handleAcceptRequest(DadkvsPaxos.PhaseTwoRequest request) {
		return null;
	}

	public void handleCommittx(int reqid) {
		// Learner does nothing
	}

	public void promote() {
		this.serverState.changePaxosState(new Acceptor());
	}

	public void demote() {
	}
}
