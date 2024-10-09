package dadkvs.server;

import dadkvs.server.domain.Acceptor;
import dadkvs.server.domain.Learner;
import dadkvs.server.domain.PaxosState;

public class DadkvsServerState {
	boolean iAmLeader;
	public int nServers;
	int debugMode;
	public int basePort;
	public int myId;
	int storeSize;
	public KeyValueStore store;
	boolean slowMode;
	boolean frozen;
	Object freezeLock;

	private final Queue _queue;
	private final PaxosQueue _paxosQueue;

	public PaxosState paxosState;
	public LogSystem logSystem;
	public int configuration;

	public DadkvsServerState(final int kv_size, final int port, final int myself) {
		basePort = port;
		nServers = 5;
		myId = myself;
		iAmLeader = false;
		debugMode = 0;
		storeSize = kv_size;
		store = new KeyValueStore(kv_size);
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

	public void temp() {
		switch (debugMode) {
			case 0:
				// Normal mode
				System.out.println("Debug mode 0: Normal mode.");
				break;
			case 1:
				// Crash the _server
				System.out.println("Debug mode 1: Crash the server.");
				// just brute forcing for now
				// FIXME: maybe close stubs
				System.exit(0);
				break;
			case 2:
				// Freeze the _server
				System.out.println("Debug mode 2: Freeze the server.");
				frozen = true;
				break;
			case 3:
				// Un-freeze the _server
				System.out.println("Debug mode 3: Un-freeze the server.");
				synchronized (freezeLock) {
					frozen = false;
					freezeLock.notifyAll();
				}
				break;
			case 4:
				// Slow mode on (in_sert random delay between request processing)
				System.out.println("Debug mode 4: Slow mode on");
				slowMode = true;
				break;
			case 5:
				// Slow mode off (remove random delay)
				System.out.println("Debug mode 5: Slow mode off");
				slowMode = false;
				break;
			default:
				System.out.println("Unknown debug mode: " + debugMode);
				break;
		}
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

	public <T extends PaxosState> void changePaxosState(final T newState) {
		logSystem.writeLog("Changed paxos state from " + paxosState.getClass().getSimpleName() + " to "
				+ newState.getClass().getSimpleName());
		paxosState = newState;
		paxosState.setServerState(this);
	}

	public int getSequenceNumber(final int reqId) {
		return _paxosQueue.getSequenceNumber(reqId);
	}

	public void addRequest(final int reqId, final int seqNumber) {
		_paxosQueue.addRequest(reqId, seqNumber);
	}

	public void waitInLine(final int queueNumber) {
		_queue.waitForQueueNumber(queueNumber);
	}

	public void nextInLine() {
		_queue.incrementQueueNumber();
	}

	public int getQuorum(final int nAcceptors) {
		return (int) Math.floor(nAcceptors / 2) + 1;
	}
}
