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

	private Sequencer _sequencer; // Leader
	private FastPaxosQueue _fastPaxosQueue; // Replica
	private Queue _queue;


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
		_fastPaxosQueue = new FastPaxosQueue();
	}

	public int getSequencerNumber() {
		return _sequencer.getSequenceNumber();
	}

	public int getSeqFromLeader(int reqid) {
		return _fastPaxosQueue.getSequenceNumber(reqid);
	}

	public void addSeqFromLeader(int reqid, int seqNumber) {
		_fastPaxosQueue.addRequest(reqid, seqNumber);
	}

	public void waitInLine(int queueNumber) {
		_queue.waitForQueueNumber(queueNumber);
	}

	public void nextInLine() {
		_queue.incrementQueueNumber();
	}
}
