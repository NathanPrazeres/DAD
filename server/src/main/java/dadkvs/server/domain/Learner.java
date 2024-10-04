package dadkvs.server.domain;

import dadkvs.DadkvsMain;
import dadkvs.server.DadkvsServerState;
import dadkvs.server.TransactionRecord;
import dadkvs.DadkvsPaxos;

public class Learner implements PaxosState {
    private DadkvsServerState _serverState;
    private int highestTimestamp = -1;

    public void setServerState(DadkvsServerState serverState) {
        _serverState = serverState;
    }

    // public void handleLearnRequest();
    // public void handlePrepareRequest();
    public DadkvsPaxos.PhaseOneReply handlePrepareRequest(DadkvsPaxos.PhaseOneRequest request) { return null; }
    public DadkvsPaxos.PhaseTwoReply handleAcceptRequest(DadkvsPaxos.PhaseTwoRequest request) { return null; }
    public void handleCommittx(int reqid) {
        // Learner does nothing
    }

    public void promote() {
        _serverState.changePaxosState(new Acceptor());
    }

    public void demote() {}
}
