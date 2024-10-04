package dadkvs.server.domain;

import dadkvs.DadkvsMain;
import dadkvs.server.DadkvsServerState;
import dadkvs.server.TransactionRecord;
import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;

public class Acceptor implements PaxosState {
    private DadkvsServerState _serverState;
    private int highestTimestamp = -1;
    private int acceptedValue = -1;

    public void setServerState(DadkvsServerState serverState) {
        _serverState = serverState;
    }

    // public void handleLearnRequest();
    public DadkvsPaxos.PhaseOneReply handlePrepareRequest(DadkvsPaxos.PhaseOneRequest request) {
        int proposalConfig = request.getPhase1Config();
		int proposalIndex = request.getPhase1Index();
		int proposalTimestamp = request.getPhase1Timestamp();

        if (proposalTimestamp > highestTimestamp) {
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

		// if acceptor has already accepted any value, reject
		if (acceptedValue == -1) {

			DadkvsPaxos.PhaseTwoReply response = DadkvsPaxos.PhaseTwoReply.newBuilder()
					.setPhase2Config(proposalConfig)
					.setPhase2Index(proposalIndex)
					.setPhase2Accepted(false)
					.build();
            
			return response;
		}

		// If the proposal's timestamp is higher than the highest seen, accept it
		if (proposalTimestamp >= highestTimestamp) {
            highestTimestamp = proposalTimestamp;
            acceptedValue = proposalValue;

			// Respond that the proposal was accepted
			DadkvsPaxos.PhaseTwoReply response = DadkvsPaxos.PhaseTwoReply.newBuilder()
					.setPhase2Config(proposalConfig)
					.setPhase2Index(proposalIndex)
					.setPhase2Accepted(true)
					.build();

			// acceptors should trigger the learn request once they accept a value
			// boolean learned = sendLearnRequests(proposalIndex);

            return response;
		} else {
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
        // Learner does nothings
    }

    public void promote() {
        _serverState.changePaxosState(new Leader());
    }

    public void demote() {
        _serverState.changePaxosState(new Learner());
    }
}
