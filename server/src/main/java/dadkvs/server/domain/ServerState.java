package dadkvs.server.domain;

import dadkvs.server.LogSystem;
import dadkvs.server.domain.paxos.Acceptor;
import dadkvs.server.domain.paxos.Learner;
import dadkvs.server.domain.paxos.PaxosQueue;
import dadkvs.server.domain.paxos.PaxosState;
import dadkvs.server.domain.paxos.Proposer;

public class ServerState {
	public int nServers;
	public int basePort;
	public int myId;
	public boolean slowMode;
	public boolean frozen;
	public Object freezeLock;
	public PaxosState paxosState;
	public LogSystem logSystem;
	
	private KeyValueStore _store;
	private final Queue _queue;
	private final PaxosQueue _paxosQueue;
	
	public int[] acceptors;
	public int configuration;
    public static final int[][] CONFIGS = {
        {0, 1, 2},
        {1, 2, 3},
        {2, 3, 4}
    };

	public ServerState(final int kv_size, final int port, final int myself) {
		basePort = port;
		nServers = 5;
		myId = myself;
		_store = new KeyValueStore(kv_size);
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
		
		acceptors = CONFIGS[configuration];
		logSystem = new LogSystem(String.valueOf(port + myself), 1);
		logSystem.writeLog("Started session");
		logSystem.writeLog("I am " + paxosState.getClass().getSimpleName());

		paxosState.setServerState(this);
	}

	public void setDebugMode(int debugMode, int arg) {
		switch (debugMode) {
			case 0:
				// Normal mode
				logSystem.writeLog("Debug mode 0: Normal mode.");
				break;
			case 1:
				// Crash the _server
				logSystem.writeLog("Debug mode 1: Crash the server.");
				// just brute forcing for now
				// FIXME: maybe close stubs
				System.exit(0);
				break;
			case 2:
				// Freeze the _server
				logSystem.writeLog("Debug mode 2: Freeze the server.");
				frozen = true;
				break;
			case 3:
				// Un-freeze the _server
				logSystem.writeLog("Debug mode 3: Un-freeze the server.");
				synchronized (freezeLock) {
					frozen = false;
					freezeLock.notifyAll();
				}
				break;
			case 4:
				// Slow mode on (in_sert random delay between request processing)
				logSystem.writeLog("Debug mode 4: Slow mode on");
				slowMode = true;
				break;
			case 5:
				// Slow mode off (remove random delay)
				logSystem.writeLog("Debug mode 5: Slow mode off");
				slowMode = false;
				break;
			case 6:
				logSystem.writeLog("Debug mode 6: Block Paxos mode on");
				if (paxosState instanceof Proposer) {
					((Proposer) paxosState).blockPaxos();
				}
				break;
			case 7:
				logSystem.writeLog("Debug mode 7: Block Paxos mode off");
				if (paxosState instanceof Proposer) {
					((Proposer) paxosState).unblockPaxos();
				}
				break;
			default:
				logSystem.writeLog("Unknown debug mode: " + debugMode);
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

	public boolean hasSequenceNumber(final int reqId) {
		return _paxosQueue.hasSequenceNumber(reqId);
	}

	public int getSequenceNumber(final int reqId) {
		return _paxosQueue.getSequenceNumber(reqId);
	}

	public int waitForSequenceNumber(final int reqId) {
		return _paxosQueue.waitForSequenceNumber(reqId);
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

	public void setLeader(boolean isLeader) {
		if (isLeader) {
			logSystem.writeLog("Promoted");
			paxosState.promote();
		} else {
			logSystem.writeLog("Demoted");
			paxosState.demote();
		}
	}

	public boolean commit(TransactionRecord txrecord) {
		if (txrecord.getPrepareKey() == 0) {
			configuration++;
			if (configuration > 2)
				configuration = 0;
			acceptors = CONFIGS[configuration];
			paxosState.reconfigure(configuration);
		}
		boolean result = _store.commit(txrecord);
		logSystem.writeLog(_store.toString());
		return result;
	}
	
	public VersionedValue read(int key) {
		return _store.read(key);
	}
	
	public void requestCancellation() {
		_paxosQueue.requestCancellation();
	}
}
