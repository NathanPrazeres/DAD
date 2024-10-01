package dadkvs.server;

/* these imported classes are generated by the contract */
import dadkvs.DadkvsFastPaxos;
import dadkvs.DadkvsFastPaxosServiceGrpc;

import io.grpc.stub.StreamObserver;

public class DadkvsFastPaxosServiceImpl extends DadkvsFastPaxosServiceGrpc.DadkvsFastPaxosServiceImplBase {

	DadkvsServerState server_state;

	public DadkvsFastPaxosServiceImpl(DadkvsServerState state) {
		this.server_state = state;
	}

	@Override
	public void fastPaxos(DadkvsFastPaxos.FastPaxosRequest request,
			StreamObserver<DadkvsFastPaxos.FastPaxosReply> responseObserver) {
		// for debug purposes
		System.out.println(request);
		System.exit(0);

		server_state.addSeqFromLeader(request.getReqId(), request.getSeqNum());

		DadkvsFastPaxos.FastPaxosReply response = DadkvsFastPaxos.FastPaxosReply.newBuilder().build();

		responseObserver.onNext(response);
	}

}
