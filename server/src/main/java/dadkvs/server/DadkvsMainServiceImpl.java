package dadkvs.server;

/* these imported classes are generated by the contract */
import dadkvs.DadkvsMain;
import dadkvs.DadkvsMainServiceGrpc;

import io.grpc.stub.StreamObserver;

public class DadkvsMainServiceImpl extends DadkvsMainServiceGrpc.DadkvsMainServiceImplBase {
	DadkvsServerState server_state;
	int timestamp;

	static final int SERVER_DELAY = 5000; // 5 seconds

	public DadkvsMainServiceImpl(DadkvsServerState state) {
		this.server_state = state;
		this.timestamp = 0;
	}

	public void tryWait() {
		try {
			Thread.sleep((int) (SERVER_DELAY * (Math.random() + 0.5))); // Median is SERVER_DELAY
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void read(DadkvsMain.ReadRequest request, StreamObserver<DadkvsMain.ReadReply> responseObserver) {
		// for debug purposes
		System.out.println("Receiving read request:" + request);

		int reqid = request.getReqid();
		int key = request.getKey();

		if (reqid % 100 != 0) { // NOTE: its not a console request
			if (this.server_state.frozen) {
				System.out.println("Server is frozen. Blocking all client requests.");
				return;
			} else if (this.server_state.slow_mode) {
				tryWait();
			}
		}

		VersionedValue vv = this.server_state.store.read(key);
		server_state.nextInLine();
		DadkvsMain.ReadReply response = DadkvsMain.ReadReply.newBuilder()
				.setReqid(reqid).setValue(vv.getValue()).setTimestamp(vv.getVersion()).build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	@Override
	public void committx(DadkvsMain.CommitRequest request, StreamObserver<DadkvsMain.CommitReply> responseObserver) {
		// for debug purposes
		System.out.println("Receiving commit request:" + request);

		int reqid = request.getReqid();
		int key1 = request.getKey1();
		int version1 = request.getVersion1();
		int key2 = request.getKey2();
		int version2 = request.getVersion2();
		int writekey = request.getWritekey();
		int writeval = request.getWriteval();

		if (reqid % 100 != 0) { // NOTE: its not a console request
			if (this.server_state.frozen) {
				System.out.println("Server is frozen. Blocking all client requests.");
				return;
			} else if (this.server_state.slow_mode) {
				tryWait();
			}
		}

		// for debug purposes
		System.out.println("reqid " + reqid + " key1 " + key1 + " v1 " + version1 + " k2 " + key2 + " v2 " + version2
				+ " wk " + writekey + " writeval " + writeval);

		this.timestamp++;
		TransactionRecord txrecord = new TransactionRecord(key1, version1, key2, version2, writekey, writeval,
				this.timestamp);

		if (server_state.i_am_leader) {
			// if leader, send fast paxos request with sequence number as epoch

			// for debug purposes
			System.out.println("Result is ready to be ordered by leader with reqid " + reqid);

			server_state.orderIdRequest(reqid, server_state.getSequencerNumber());
			server_state.nextInLine();

			boolean result = this.server_state.store.commit(txrecord);
		} else {
			// if slave server, add to queue? and wait until you receive a request from the
			// client that the leader has ordered
			if (/* received request matching order set by leader */ false) {
				boolean result = this.server_state.store.commit(txrecord);
			}
		}

		DadkvsMain.CommitReply response = DadkvsMain.CommitReply.newBuilder()
				.setReqid(reqid).setAck(result).build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}
}
