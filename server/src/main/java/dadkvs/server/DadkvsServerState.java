package dadkvs.server;

import dadkvs.server.domain.PaxosState;
import dadkvs.server.domain.Learner;
import dadkvs.server.domain.Acceptor;

public class DadkvsServerState {
	boolean i_am_leader;
	int n_servers;
	int debug_mode;
	int base_port;
	int my_id;
	int store_size;
	public KeyValueStore store;
	MainLoop main_loop;
	Thread main_loop_worker;
	boolean slow_mode;
	boolean frozen;

	private Queue _queue;
	private PaxosQueue _paxosQueue;

	public PaxosState paxosState;
	public LogSystem logSystem;

	public DadkvsServerState(int kv_size, int port, int myself) {
		base_port = port;
		n_servers = 5;
		my_id = myself;
		i_am_leader = false;
		debug_mode = 0;
		store_size = kv_size;
		store = new KeyValueStore(kv_size);
		main_loop = new MainLoop(this);
		main_loop_worker = new Thread(main_loop);
		main_loop_worker.start();
		slow_mode = false;
		frozen = false;

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

	public <T extends PaxosState> void changePaxosState(T newState) {
		logSystem.writeLog("Changed paxos state from " + paxosState.getClass().getSimpleName() + " to " + newState.getClass().getSimpleName());
        paxosState = newState;
        paxosState.setServerState(this);
	}

	public int getSequenceNumber(int reqid) {
		return _paxosQueue.getSequenceNumber(reqid);
	}

	public void waitInLine(int queueNumber) {
		_queue.waitForQueueNumber(queueNumber);
	}

	public void nextInLine() {
		_queue.incrementQueueNumber();
	}
}
