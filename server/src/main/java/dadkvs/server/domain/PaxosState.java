package dadkvs.server.domain;

import java.util.concurrent.atomic.AtomicInteger;

import dadkvs.server.DadkvsServerState;
import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import dadkvs.util.GenericResponseCollector;
import dadkvs.util.CollectorStreamObserver;

public abstract class PaxosState {

	int highestTimestamp = -1;
	AtomicInteger numResponses = new AtomicInteger(0);
	public DadkvsServerState serverState;

	public abstract DadkvsPaxos.PhaseTwoReply handleAcceptRequest(DadkvsPaxos.PhaseTwoRequest request);

	public abstract DadkvsPaxos.PhaseOneReply handlePrepareRequest(DadkvsPaxos.PhaseOneRequest request);

	public DadkvsPaxos.LearnReply handleLearnRequest(DadkvsPaxos.LearnRequest request) {

		int learnConfig = request.getLearnconfig();
		int learnIndex = request.getLearnindex();
		int learnReqid = request.getLearnvalue();
		int learnTimestamp = request.getLearntimestamp();
		boolean accepted = true;
		serverState.logSystem.writeLog("[PAXOS (" + learnIndex + ")]\t\tRECEIVED LEARN REQUEST.");

		if (highestTimestamp == -1 || learnTimestamp > highestTimestamp) {
			serverState.logSystem
					.writeLog("[PAXOS (" + learnIndex + ")] Received request with higher timestamp than ours:\tResetting;");
			numResponses = new AtomicInteger(1);
			highestTimestamp = learnTimestamp;
		} else if (highestTimestamp > learnTimestamp) {
			serverState.logSystem
					.writeLog("[PAXOS (" + learnIndex + ")] Received request with lower timestamp than ours:\tRejecting");
			accepted = false;
		} else {
			serverState.logSystem
					.writeLog("[PAXOS (" + learnIndex + ")] Received request with timestamp equal to ours:\tAccepting.");

			if (numResponses.get() < 2)
				numResponses.incrementAndGet();

			if (numResponses.get() == 2) {
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

	public abstract void handleCommittx(int reqId);

	public abstract void setServerState(DadkvsServerState serverState);

	public abstract void promote();

	public abstract void demote();

	public void sendLearnRequest(int paxosIndex, int priority, int acceptedValue,
			DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] asyncStubs) {
		serverState.logSystem
				.writeLog("[PAXOS (" + paxosIndex + ")]\t\tSTARTING LEARN PHASE.");

		DadkvsPaxos.LearnRequest.Builder request = DadkvsPaxos.LearnRequest.newBuilder();
		ArrayList<DadkvsPaxos.LearnReply> learnResponses = new ArrayList<>();
		GenericResponseCollector<DadkvsPaxos.LearnReply> learnCollector = new GenericResponseCollector<>(learnResponses,
				serverState.nServers);
		request.setLearnconfig(serverState.configuration).setLearnindex(paxosIndex).setLearnvalue(acceptedValue)
				.setLearntimestamp(priority);

		serverState.logSystem
				.writeLog("[PAXOS (" + paxosIndex + ")] Sending Learn request to all Learners.");
		serverState.logSystem
				.writeLog("[PAXOS (" + paxosIndex + ")] Learn request - Configuration: " + serverState.getConfiguration()
						+ " Value: " + acceptedValue + " Priority: " + priority);

		int nServers = serverState.nServers;
		CountDownLatch latch = new CountDownLatch(nServers);
		ExecutorService executor = Executors.newFixedThreadPool(nServers);

		for (int i = 0; i < nServers; i++) {
			final int index = i;
			executor.submit(() -> {
				CollectorStreamObserver<DadkvsPaxos.LearnReply> learnObserver = new CollectorStreamObserver<>(learnCollector);
				asyncStubs[index].learn(request.build(), learnObserver);
				latch.countDown();
			});
		}

		try {
			latch.await();
		} catch (InterruptedException e) {
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
}
