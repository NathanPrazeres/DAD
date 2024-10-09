package dadkvs.server;

/* these imported classes are generated by the contract */
import dadkvs.DadkvsConsole;
import dadkvs.DadkvsConsoleServiceGrpc;

import io.grpc.stub.StreamObserver;

public class DadkvsConsoleServiceImpl extends DadkvsConsoleServiceGrpc.DadkvsConsoleServiceImplBase {

	DadkvsServerState serverState;

	public DadkvsConsoleServiceImpl(DadkvsServerState state) {
		this.serverState = state;
	}

	@Override
	public void setleader(DadkvsConsole.SetLeaderRequest request,
			StreamObserver<DadkvsConsole.SetLeaderReply> responseObserver) {
		// for debug purposes
		System.out.println(request);

		boolean responseValue = true;
		if (request.getIsleader()) {
			serverState.paxosState.promote();
		} else {
			serverState.paxosState.demote();
		}
		this.serverState.iAmLeader = request.getIsleader();

		// for debug purposes
		System.out.println("I am the leader = " + this.serverState.iAmLeader);
		serverState.logSystem.writeLog("I am the leader = " + this.serverState.iAmLeader);

		this.serverState.mainLoop.wakeup();

		DadkvsConsole.SetLeaderReply response = DadkvsConsole.SetLeaderReply.newBuilder()
				.setIsleaderack(responseValue).build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
		serverState.logSystem.writeLog("Set leader request completed");
	}

	@Override
	public void setdebug(DadkvsConsole.SetDebugRequest request,
			StreamObserver<DadkvsConsole.SetDebugReply> responseObserver) {
		// for debug purposes
		System.out.println(request);

		boolean responseValue = true;

		this.serverState.debugMode = request.getMode();
		this.serverState.mainLoop.wakeup();

		// for debug purposes
		System.out.println("Setting debug mode to = " + this.serverState.debugMode);
		serverState.logSystem.writeLog("Setting debug mode to = " + this.serverState.debugMode);

		DadkvsConsole.SetDebugReply response = DadkvsConsole.SetDebugReply.newBuilder()
				.setAck(responseValue).build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
		serverState.logSystem.writeLog("Set debug request completed");
	}
}
