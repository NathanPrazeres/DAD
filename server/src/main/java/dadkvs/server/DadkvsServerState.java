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

	private Sequencer _sequencer;
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

	public void orderId(int req_id, int epoch) {
		while (true) {
			if (epoch == _sequencer.readSeqNumber() + 1) {
				// TODO: execute transaction, increase sequence number, notify all
				notifyAll();
			} else {
				// TODO: wait until epoch is the next one
				try {
					wait();
				} catch (InterruptedException e) {
					// do nothing
				}

			}
		}
	}

	public void orderIdRequest(int req_id, int epoch) {
		// NOTE: currently, the function in DadkvsServer, but probably needs to be moved
	}

}
