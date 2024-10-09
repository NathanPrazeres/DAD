package dadkvs.server;

import dadkvs.server.domain.PaxosState;
import dadkvs.server.domain.Learner;
import dadkvs.server.domain.Acceptor;
import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class DadkvsServerState {
	boolean iAmLeader;
	public int nServers;
	int debugMode;
	public int basePort;
	public int myId;
	int storeSize;
	public KeyValueStore store;
	MainLoop mainLoop;
	Thread mainLoopWorker;
	boolean slowMode;
	boolean frozen;
	Object freezeLock;

	private Queue _queue;
	private PaxosQueue _paxosQueue;

	public PaxosState paxosState;
	public LogSystem logSystem;
	public int configuration;

	public DadkvsServerState(int kv_size, int port, int myself) {
		basePort = port;
		nServers = 5;
		myId = myself;
		iAmLeader = false;
		debugMode = 0;
		storeSize = kv_size;
		store = new KeyValueStore(kv_size);
		mainLoop = new MainLoop(this);
		mainLoopWorker = new Thread(mainLoop);
		mainLoopWorker.start();
		slowMode = false;
		frozen = false;
		freezeLock = new Object();
		configuration = 0;

		_paxosQueue = new PaxosQueue();

		_queue = new Queue();

		if (myself > 2)
			paxosState = new Learner();
		else
			paxosState = new Acceptor();

		logSystem = new LogSystem(String.valueOf(port + myself), 1);
		logSystem.writeLog("Started session");
		logSystem.writeLog("I am " + paxosState.getClass().getSimpleName());

		paxosState.setServerState(this);
	}

	public int myId() {
		return myId;
	}

	public int getNumberOfServers() {
		return nServers;
	}

	public int getConfiguration() {
		return configuration;
	}

	public <T extends PaxosState> void changePaxosState(T newState) {
		logSystem.writeLog("Changed paxos state from " + paxosState.getClass().getSimpleName() + " to "
				+ newState.getClass().getSimpleName());
		paxosState = newState;
		paxosState.setServerState(this);
	}

	public int getSequenceNumber(int reqId) {
		return _paxosQueue.getSequenceNumber(reqId);
	}

	public void addRequest(int reqId, int seqNumber) {
		_paxosQueue.addRequest(reqId, seqNumber);
	}

	public void waitInLine(int queueNumber) {
		_queue.waitForQueueNumber(queueNumber);
	}

	public void nextInLine() {
		_queue.incrementQueueNumber();
	}

	public int getQuorum(int nAcceptors) {
		return (int) Math.floor(nAcceptors / 2) + 1;
	}
}
