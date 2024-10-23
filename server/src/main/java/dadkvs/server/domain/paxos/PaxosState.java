package dadkvs.server.domain.paxos;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.server.domain.ServerState;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;

public abstract class PaxosState {
	protected ConcurrentHashMap<Integer, Paxos> paxosInstancesHashMap = new ConcurrentHashMap<>();
	protected ServerState serverState;

	protected Paxos getPaxos(int seqNum) {
		if (paxosInstancesHashMap.get(seqNum) == null) {
			serverState.logSystem.writeLog("[PAXOS (" + seqNum + ")]\t\tCreating new instance...");
			Paxos paxos = new Paxos();
			paxos.seqNum.set(seqNum);
			paxosInstancesHashMap.put(seqNum, paxos);
			return paxos;
		}
		return paxosInstancesHashMap.get(seqNum);
	}
	
	public DadkvsPaxos.LearnReply handleLearnRequest(final DadkvsPaxos.LearnRequest request) {
		final int learnConfig = request.getLearnconfig();
		final int learnIndex = request.getLearnindex();
		final int learnReqid = request.getLearnvalue();
		final int learnTimestamp = request.getLearntimestamp();
		boolean accepted = true;
		serverState.logSystem.writeLog("[PAXOS (" + learnIndex + ")]\t\tRECEIVED LEARN REQUEST.");

		Paxos paxos = getPaxos(learnIndex);
		
		if (learnTimestamp > paxos.timestamp.get()) {
			serverState.logSystem
			.writeLog("[PAXOS (" + learnIndex + ")] Received request with higher timestamp than ours:\tResetting;");
			paxos.numResponses = new AtomicInteger(1);
			paxos.timestamp.set(learnTimestamp);
		} else if (paxos.timestamp.get() > learnTimestamp) {
			serverState.logSystem
				.writeLog("[PAXOS (" + learnIndex + ")] Received request with lower timestamp than ours:\tRejecting");
			accepted = false;
		} else {
			serverState.logSystem
				.writeLog("[PAXOS (" + learnIndex + ")] Received request with timestamp equal to ours:\tAccepting.");
			
			if (paxos.numResponses.get() < 2)
			paxos.numResponses.incrementAndGet();
			
			if (paxos.numResponses.get() == 2) {
				serverState.logSystem.writeLog("[PAXOS (" + learnIndex + ")] Adding request: '" + learnReqid
				+ "' with sequencer number: '" + learnIndex + "'");
				serverState.addRequest(learnReqid, learnIndex);
			}
		}
		
		return DadkvsPaxos.LearnReply.newBuilder()
		.setLearnconfig(learnConfig)
		.setLearnindex(learnIndex)
		.setLearnaccepted(accepted)
		.build();
	}
	
	public void sendLearnRequest(final int paxosIndex, final int priority, final int acceptedValue,
	final DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] asyncStubs) {
		serverState.logSystem
		.writeLog("[PAXOS (" + paxosIndex + ")]\t\tSTARTING LEARN PHASE.");
		
		final DadkvsPaxos.LearnRequest.Builder request = DadkvsPaxos.LearnRequest.newBuilder();
		final ArrayList<DadkvsPaxos.LearnReply> learnResponses = new ArrayList<>();
		final GenericResponseCollector<DadkvsPaxos.LearnReply> learnCollector = new GenericResponseCollector<>(
			learnResponses,
			serverState.nServers);
			request.setLearnconfig(serverState.getConfiguration()).setLearnindex(paxosIndex).setLearnvalue(acceptedValue)
			.setLearntimestamp(priority);
			
			serverState.logSystem
			.writeLog("[PAXOS (" + paxosIndex + ")] Sending Learn request to all Learners.");
			serverState.logSystem
			.writeLog("[PAXOS (" + paxosIndex + ")] Learn request - Configuration: " + serverState.getConfiguration()
			+ " Value: " + acceptedValue + " Priority: " + priority);
			
			final int nServers = serverState.nServers;
			final CountDownLatch latch = new CountDownLatch(nServers);
			final ExecutorService executor = Executors.newFixedThreadPool(nServers);
			
			for (int i = 0; i < nServers; i++) {
				final int index = i;
				executor.submit(() -> {
					final CollectorStreamObserver<DadkvsPaxos.LearnReply> learnObserver = new CollectorStreamObserver<>(
						learnCollector);
						asyncStubs[index].learn(request.build(), learnObserver);
						latch.countDown();
					});
				}
				
				try {
					latch.await();
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			e.printStackTrace();
		} finally {
			executor.shutdown();
		}
		
		serverState.logSystem
		.writeLog("[PAXOS (" + paxosIndex + ")] Waiting for learn replys.");
		
		learnCollector.waitForTarget(1);
		if (learnResponses.size() >= 1) {
			serverState.logSystem
			.writeLog("[PAXOS (" + paxosIndex + ")]\t\tENDING LEARN PHASE - SUCCESS.");
		} else {
			serverState.logSystem
			.writeLog("[PAXOS (" + paxosIndex + ")]\t\tDID NOT RECEIVE LEARN REPLYS");
		}
	}

	public void setPaxosInstances(ConcurrentHashMap<Integer, Paxos> paxosInstances) {
		paxosInstancesHashMap = paxosInstances;
	}

	public ConcurrentHashMap<Integer, Paxos> getPaxosInstances() {
		return paxosInstancesHashMap;
	}

	protected int getHighestPaxosInstance() {
		return paxosInstancesHashMap.entrySet()
			.stream()
			.max(Map.Entry.comparingByKey())
			.map(Map.Entry::getKey)
			.orElse(-1);
	}

	public abstract void setServerState(ServerState serverState);
	public abstract void reconfigure(int newConfig);
	public abstract void handleCommittx(int reqId);
	public abstract void promote();
	public abstract void demote();
	public abstract DadkvsPaxos.PhaseTwoReply handleAcceptRequest(DadkvsPaxos.PhaseTwoRequest request);
	public abstract DadkvsPaxos.PhaseOneReply handlePrepareRequest(DadkvsPaxos.PhaseOneRequest request);
}
