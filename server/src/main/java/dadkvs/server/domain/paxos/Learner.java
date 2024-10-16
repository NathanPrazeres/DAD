package dadkvs.server.domain.paxos;

import dadkvs.DadkvsPaxos;
import dadkvs.server.domain.ServerState;

public class Learner extends PaxosState {
	public void setServerState(final ServerState serverState) {
		this.serverState = serverState;
	}

	public DadkvsPaxos.PhaseOneReply handlePrepareRequest(final DadkvsPaxos.PhaseOneRequest request) {
		return null;
	}

	public DadkvsPaxos.PhaseTwoReply handleAcceptRequest(final DadkvsPaxos.PhaseTwoRequest request) {
		return null;
	}

	public void handleCommittx(final int reqId) {
		// Learner does nothing
	}

	public void promote() {
		this.serverState.changePaxosState(new Acceptor());
	}

	@Override
	public String toString() {
		return "Learner []";
	}

	public void demote() {
	}
}
