package dadkvs.server;

/* these imported classes are generated by the contract */
import java.util.ArrayList;

import dadkvs.DadkvsFastPaxos;
import dadkvs.DadkvsMain;
import dadkvs.DadkvsMainServiceGrpc;
import dadkvs.DadkvsFastPaxosServiceGrpc;
import dadkvs.util.GenericResponseCollector;
import dadkvs.util.CollectorStreamObserver;

import io.grpc.stub.StreamObserver;

public class DadkvsMainServiceImpl extends DadkvsMainServiceGrpc.DadkvsMainServiceImplBase {
	private DadkvsServerState server_state;
	private DadkvsFastPaxosServiceGrpc.DadkvsFastPaxosServiceStub[] _async_stubs;
	int timestamp;

	static final int SERVER_DELAY = 5000; // 5 seconds

	public DadkvsMainServiceImpl(DadkvsServerState state, DadkvsFastPaxosServiceGrpc.DadkvsFastPaxosServiceStub[] async_stubs) {
		this.server_state = state;
		this.timestamp = 0;
		_async_stubs = async_stubs;
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
		server_state.logSystem.writeLog("Receiving read request:" + request);

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
		server_state.logSystem.writeLog("Read request completed");
	}

	@Override
	public void committx(DadkvsMain.CommitRequest request, StreamObserver<DadkvsMain.CommitReply> responseObserver) {
		int reqid = request.getReqid();
		int key1 = request.getKey1();
		int version1 = request.getVersion1();
		int key2 = request.getKey2();
		int version2 = request.getVersion2();
		int writekey = request.getWritekey();
		int writeval = request.getWriteval();
		
		System.out.println("Receiving commit request:" + request);
		server_state.logSystem.writeLog("Receiving commit request:" + request);

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

		server_state.logSystem.writeLog("reqid " + reqid + " key1 " + key1 + " v1 " + version1 + " k2 " + key2 + " v2 " + version2
				+ " wk " + writekey + " writeval " + writeval);
		
		int seqNumber;

		if (server_state.i_am_leader) {
			seqNumber = server_state.getSequencerNumber();
		} else {
			server_state.logSystem.writeLog("Waiting for leader");
			seqNumber = server_state.getSeqFromLeader(reqid);
			server_state.logSystem.writeLog("Received");
		}

		server_state.waitInLine(seqNumber);
				
		this.timestamp++;
		TransactionRecord txrecord = new TransactionRecord(key1, version1, key2, version2, writekey, writeval,
			this.timestamp);

		boolean result = this.server_state.store.commit(txrecord);

		// for debug purposes
		System.out.println("Result is ready to be ordered by leader with reqid " + reqid);
		server_state.logSystem.writeLog("Result is ready to be ordered by leader with reqid " + reqid);

		DadkvsMain.CommitReply response = DadkvsMain.CommitReply.newBuilder()
				.setReqid(reqid).setAck(result).build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();

		server_state.nextInLine();

		// Send sequence number to other replicas
		if (server_state.i_am_leader) {
			server_state.logSystem.writeLog("Sending sequence number to replicas");
			doFastPaxos(reqid, seqNumber);
		}
	}

	public boolean doFastPaxos(int req_id, int seq_num) {
		DadkvsFastPaxos.FastPaxosRequest.Builder order_request = DadkvsFastPaxos.FastPaxosRequest.newBuilder();

		order_request.setReqId(req_id)
				.setSeqNum(seq_num);

		server_state.logSystem.writeLog("Request ID: " + req_id);
		server_state.logSystem.writeLog("Sequence Number: " + seq_num);
		System.out.println("Request ID: " + req_id);
		System.out.println("Sequence Number: " + seq_num);

		ArrayList<DadkvsFastPaxos.FastPaxosReply> order_responses = new ArrayList<DadkvsFastPaxos.FastPaxosReply>();
		GenericResponseCollector<DadkvsFastPaxos.FastPaxosReply> order_collector = new GenericResponseCollector<DadkvsFastPaxos.FastPaxosReply>(
				order_responses, server_state.n_servers - 1);
		for (int i = 0; i < server_state.n_servers - 1; i++) {
			CollectorStreamObserver<DadkvsFastPaxos.FastPaxosReply> order_observer = new CollectorStreamObserver<DadkvsFastPaxos.FastPaxosReply>(
					order_collector);
			_async_stubs[i].fastPaxos(order_request.build(), order_observer);

		}

		return true;
	}
}
