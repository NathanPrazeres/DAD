package dadkvs.server.domain;

import dadkvs.DadkvsMain;
import dadkvs.server.DadkvsServerState;
import dadkvs.server.TransactionRecord;

public class Acceptor implements PaxosState {
    private DadkvsServerState _serverState;

    public void setServerState(DadkvsServerState serverState) {
        _serverState = serverState;
    }

    // public void handleLearnRequest();
    // public void handlePrepareRequest();
    // public void handlePromiseRequest();
    public void handleCommittx(int reqid) {
        // Learner does nothings
    }

    public void promote() {
        _serverState.changePaxosState(new Leader());
    }

    public void demote() {
        _serverState.changePaxosState(new Learner());
    }
}
