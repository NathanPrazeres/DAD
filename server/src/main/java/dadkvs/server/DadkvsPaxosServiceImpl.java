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
		System.out.println("Receive phase1 request");
		server_state.logSystem.writeLog("Receive phase1 request: " + request.getPhase1Index());

		responseObserver.onNext(server_state.paxosState.handlePrepareRequest(request));
		responseObserver.onCompleted();
	}

	@Override
	public void phasetwo(DadkvsPaxos.PhaseTwoRequest request,
			StreamObserver<DadkvsPaxos.PhaseTwoReply> responseObserver) {
		// for debug purposes
		server_state.logSystem.writeLog("Received phase two request");
		System.out.println("Received phase two request: " + request);
		responseObserver.onNext(server_state.paxosState.handleAcceptRequest(request));
		responseObserver.onCompleted();
	}

	@Override
	public void learn(DadkvsPaxos.LearnRequest request, StreamObserver<DadkvsPaxos.LearnReply> responseObserver) {
		// for debug purposes
		server_state.logSystem.writeLog("Received learn request");
		System.out.println("Received learn request: " + request);

		server_state.logSystem.writeLog("Learn reply sent.");
		// responseObserver.onNext(response);
		responseObserver.onCompleted();
	}
}