package dadkvs.server;

/* these imported classes are generated by the contract */
import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;

import io.grpc.stub.StreamObserver;

public class DadkvsPaxosServiceImpl extends DadkvsPaxosServiceGrpc.DadkvsPaxosServiceImplBase {

	DadkvsServerState server_state;

	public DadkvsPaxosServiceImpl(DadkvsServerState state) {
		this.server_state = state;

	}

	@Override
	public void phaseone(DadkvsPaxos.PhaseOneRequest request,
			StreamObserver<DadkvsPaxos.PhaseOneReply> responseObserver) {
		// for debug purposes
		responseObserver.onNext(server_state.paxosState.handlePrepareRequest(request));
		responseObserver.onCompleted();
	}

	@Override
	public void phasetwo(DadkvsPaxos.PhaseTwoRequest request,
			StreamObserver<DadkvsPaxos.PhaseTwoReply> responseObserver) {
		// for debug purposes
		responseObserver.onNext(server_state.paxosState.handleAcceptRequest(request));
		responseObserver.onCompleted();
	}

	@Override
	public void learn(DadkvsPaxos.LearnRequest request, StreamObserver<DadkvsPaxos.LearnReply> responseObserver) {

		try {
			server_state.logSystem.writeLog("Learn request received at server " + server_state.my_id);
			responseObserver.onNext(server_state.paxosState.handleLearnRequest(request));
		} catch (Exception e) {
			server_state.logSystem.writeLog("Error: " + e.getMessage());
			System.exit(0);
		}
		responseObserver.onCompleted();
	}
}
