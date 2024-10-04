package dadkvs.server.domain;

import dadkvs.DadkvsMain;
import dadkvs.server.DadkvsServerState;

public interface PaxosState {
    // public  handleLearnRequest(request);
    // public  handlePrepareRequest(request);
    public void handleCommittx(int reqid);
    public void setServerState(DadkvsServerState serverState);
}