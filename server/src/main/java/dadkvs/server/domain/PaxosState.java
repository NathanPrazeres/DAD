package dadkvs.server.domain;

import dadkvs.DadkvsMain;
import dadkvs.server.DadkvsServerState;
import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;

public abstract class PaxosState {

	int highestTimestamp = -1;
	DadkvsServerState _serverState;

	public abstract DadkvsPaxos.PhaseTwoReply handleAcceptRequest(DadkvsPaxos.PhaseTwoRequest request);

	public abstract DadkvsPaxos.PhaseOneReply handlePrepareRequest(DadkvsPaxos.PhaseOneRequest request);

	public void handleLearnRequest(DadkvsPaxos.LearnRequest request) {

	}

	public abstract void handleCommittx(int reqid);

	public abstract void setServerState(DadkvsServerState serverState);

	public abstract void promote();

	public abstract void demote();
}
