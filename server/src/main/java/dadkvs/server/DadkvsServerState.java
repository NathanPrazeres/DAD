package dadkvs.server;

public class DadkvsServerState {
	boolean i_am_leader;
	int n_servers;
	int debug_mode;
	int base_port;
	int my_id;
	int store_size;
	KeyValueStore store;
	MainLoop main_loop;
	Thread main_loop_worker;
	boolean slow_mode;
	boolean frozen;

	private Sequencer _sequencer;
	private Queue _queue;
	// TODO: possibly add hash table as a backlog to map ids that have not yet been
	// executed

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

		_sequencer = new Sequencer();
		_queue = new Queue();
	}

	public int getSequencerNumber() {
		return _sequencer.getSeqNumber();
	}

	public void waitInLine(int queueNumber) {
		_queue.waitForQueueNumber(queueNumber);
	}

	public void nextInLine() {
		_queue.incrementQueueNumber();
	}

	public void orderRequest(int reqId, int epoch) {
		// TODO: if when epoch == nextEpoch, execute, else, wait() until right epoch
		// nextEpoch should be a variable in server state
	}
}
