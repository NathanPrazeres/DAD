package dadkvs.server.domain;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsPaxos;
import dadkvs.server.DadkvsServerState;
import dadkvs.server.TransactionRecord;
import dadkvs.server.Sequencer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.ArrayList;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import dadkvs.DadkvsPaxosServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Leader implements PaxosState {
    private DadkvsServerState _serverState;
    private Sequencer _sequencer;
    private ConcurrentLinkedQueue<Integer> _requestQueue;
    private int _priority;
    private int _reqId;
	private DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] async_stubs;
    private int n_servers = 5;
    ManagedChannel[] channels;

    public Leader() {
        _requestQueue = new ConcurrentLinkedQueue<>();
        _reqId = -1;
        _sequencer = new Sequencer();
    }
    
    public void setServerState(DadkvsServerState serverState) {
        _serverState = serverState;
        _priority = _serverState.myId();
        initPaxosComms();
    }

    // public void handleLearnRequest();
    // public void handlePrepareRequest();
    // public void handlePromiseRequest();
    public void handleCommittx(int reqId) {
        _requestQueue.add(reqId);
        int seqNumber = _sequencer.getSequenceNumber();
        runPaxos(seqNumber);
        
    }

    private void setNewTimestamp() {
        _priority = _serverState.getNumberOfServers();
    }

    public boolean runPaxos(int seqNum) {
        try {
            while (true) {
                if (!runPhaseOne(seqNum)) {
                    continue;
                }
                // if (!runPhaseTwo(seqNum)) {
                //     continue;
                // }
                break;
            }
        } catch (RuntimeException e) {
            _serverState.logSystem.writeLog("Exception occurred while running Paxos: " + e.getMessage());
            return false;
        }
        _serverState.logSystem.writeLog("Paxos is done");
        return true;
    }

    private int extractHighestSeqNum(ArrayList<DadkvsPaxos.PhaseOneReply> responses) {
        return responses.stream()
                .mapToInt(DadkvsPaxos.PhaseOneReply::getPhase1Index)
                .max()
                .orElse(-1);
    }

    private boolean runPhaseOne(int seqNum) {
        _serverState.logSystem.writeLog("Starting Phase 1 for sequence number: " + seqNum);
        System.out.println("Starting Phase 1 for sequence number: " + seqNum);
        int[] acceptors = new int[]{0, 1, 2};

        DadkvsPaxos.PhaseOneRequest.Builder prepare = DadkvsPaxos.PhaseOneRequest.newBuilder()
                .setPhase1Index(seqNum)
                .setPhase1Config(_serverState.getConfiguration())
                .setPhase1Timestamp(_priority);
        ArrayList<DadkvsPaxos.PhaseOneReply> promise_responses = new ArrayList<>();
        GenericResponseCollector<DadkvsPaxos.PhaseOneReply> collector = new GenericResponseCollector<>(promise_responses, acceptors.length);

        for (int acceptor : acceptors) {
            try {
                CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> observer = new CollectorStreamObserver<>(collector);
                async_stubs[acceptor].phaseone(prepare.build(), observer);
            } catch (RuntimeException e) {
                _serverState.logSystem.writeLog("Exception occurred while sending Prepare request to Acceptor " + acceptor + ": " + e.getMessage());
            }
        }

        int responsesNeeded = _serverState.getQuorum(acceptors.length);
        try {
            collector.waitForTarget(responsesNeeded);
        } catch (RuntimeException e) {
            _serverState.logSystem.writeLog("Exception occurred during Phase 1: " + e.getCause().getMessage());
        }

        if (promise_responses.size() >= responsesNeeded) {
            boolean hasNaks = promise_responses.stream().anyMatch(response -> !response.getPhase1Accepted());
            if (!hasNaks) {
                _reqId = extractHighestSeqNum(promise_responses);
                if (_reqId == -1) {
                    _reqId = _requestQueue.poll();
                }
                _serverState.logSystem.writeLog("Phase 1 completed");
                return true;
            }
            setNewTimestamp();
        } else {
            _serverState.logSystem.writeLog("Error: didn't get enough responses from the quorum in Phase 1.");
            System.out.println("Error: didn't get enough responses from the quorum in Phase 1.");
        }
        return false;
    }

    private boolean runPhaseTwo(int seqNum) {
        _serverState.logSystem.writeLog("Starting Phase 2 for sequence number: " + seqNum);
        System.out.println("Starting Phase 2 for sequence number: " + seqNum);
        int[] acceptors = new int[]{0, 1, 2};

        DadkvsPaxos.PhaseTwoRequest.Builder accept = DadkvsPaxos.PhaseTwoRequest.newBuilder()
                .setPhase2Config(_serverState.getConfiguration())
                .setPhase2Config(seqNum)
                .setPhase2Index(_reqId)
                .setPhase2Timestamp(_priority);

        ArrayList<DadkvsPaxos.PhaseTwoReply> accepted_responses = new ArrayList<>();
        GenericResponseCollector<DadkvsPaxos.PhaseTwoReply> collector = new GenericResponseCollector<>(accepted_responses, acceptors.length);

        for (int acceptor : acceptors) {
            try {
                CollectorStreamObserver<DadkvsPaxos.PhaseTwoReply> observer = new CollectorStreamObserver<>(collector);
                async_stubs[acceptor].phasetwo(accept.build(), observer);
            } catch (RuntimeException e) {
                _serverState.logSystem.writeLog("Exception occurred while sending Phase 2 request to acceptor " + acceptor + ": " + e.getMessage());
            }
        }

        int responsesNeeded = _serverState.getQuorum(acceptors.length);
        try {
            collector.waitForTarget(responsesNeeded);
        } catch (RuntimeException e) {
            _serverState.logSystem.writeLog("Exception occurred during Phase 2: " + e.getCause().getMessage());
        }

        if (accepted_responses.size() >= responsesNeeded) {
            boolean hasNaks = accepted_responses.stream().anyMatch(response -> !response.getPhase2Accepted());
            if (!hasNaks) {
                _serverState.logSystem.writeLog("Phase 2 completed");
                return true;
            }
            setNewTimestamp();
        } else {
            _serverState.logSystem.writeLog("Error: didn't get enough responses from the quorum in Phase 2.");
            System.out.println("Error: didn't get enough responses from the quorum in Phase 2.");
        }
        return false;
    }

    public void promote() {}

    public void demote() {
        terminateComms();
        _serverState.changePaxosState(new Acceptor());
    }

    private void initPaxosComms() {
		// set servers
		String[] targets = new String[n_servers];

		for (int i = 0; i < n_servers; i++) {
			int target_port = _serverState.base_port + i;
			targets[i] = new String();
			targets[i] = "localhost:" + target_port;
			System.out.printf("targets[%d] = %s%n", i, targets[i]);
		}

		channels = new ManagedChannel[n_servers];

		for (int i = 0; i < n_servers; i++) {
			channels[i] = ManagedChannelBuilder.forTarget(targets[i]).usePlaintext().build();
		}

		async_stubs = new DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[n_servers];

		for (int i = 0; i < n_servers; i++) {
			async_stubs[i] = DadkvsPaxosServiceGrpc.newStub(channels[i]);
		}
	}

    private void terminateComms() {
		for (int i = 0; i < n_servers; i++) {
			channels[i].shutdownNow();
		}
	}
}
