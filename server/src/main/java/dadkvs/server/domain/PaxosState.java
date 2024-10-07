package dadkvs.server.domain;

import java.util.concurrent.atomic.AtomicInteger;

import dadkvs.server.DadkvsServerState;
import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import java.util.ArrayList;

import dadkvs.util.GenericResponseCollector;
import dadkvs.util.CollectorStreamObserver;

public abstract class PaxosState {

	int highestTimestamp = -1;
	AtomicInteger num_responses = new AtomicInteger(0);
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
			num_responses = new AtomicInteger(1);
			highestTimestamp = learnTimestamp;
		} else if (highestTimestamp > learnTimestamp) {
			serverState.logSystem
					.writeLog("[PAXOS (" + learnIndex + ")] Received request with lower timestamp than ours:\tRejecting");
			accepted = false;
		} else {
			serverState.logSystem
					.writeLog("[PAXOS (" + learnIndex + ")] Received request with timestamp equal to ours:\tAccepting.");
			
			if (num_responses.get() < 2)
				num_responses.incrementAndGet();
			
			if (num_responses.get() == 2) {
				serverState.logSystem.writeLog("[PAXOS (" + learnIndex + ")] Adding request: '" + learnReqid + "' with sequencer number: '" + learnIndex + "'");
				serverState.addRequest(learnReqid, learnIndex);
			}
		}

		return DadkvsPaxos.LearnReply.newBuilder()
				.setLearnconfig(learnConfig)
				.setLearnindex(learnIndex)
				.setLearnaccepted(accepted)
				.build();
	}

	public abstract void handleCommittx(int reqid);

	public abstract void setServerState(DadkvsServerState serverState);

	public abstract void promote();

	public abstract void demote();

	public void sendLearnRequest(int paxosIndex, int priority, int acceptedValue, DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] async_stubs) {
		serverState.logSystem
				.writeLog("[PAXOS (" + paxosIndex + ")]\t\tSTARTING LEARN PHASE.");

		DadkvsPaxos.LearnRequest.Builder request = DadkvsPaxos.LearnRequest.newBuilder();
		ArrayList<DadkvsPaxos.LearnReply> learn_responses = new ArrayList<>();
		GenericResponseCollector<DadkvsPaxos.LearnReply> learn_collector = new GenericResponseCollector<>(learn_responses, serverState.n_servers);
		request.setLearnconfig(serverState.configuration).setLearnindex(paxosIndex).setLearnvalue(acceptedValue).setLearntimestamp(priority);

		serverState.logSystem
				.writeLog("[PAXOS (" + paxosIndex + ")] Sending Learn request to all Learners.");
				serverState.logSystem
				.writeLog("[PAXOS (" + paxosIndex + ")] Learn request - Configuration: " + serverState.configuration + " Value: " + acceptedValue + " Priority: " + priority);
		for (int i = 0; i < serverState.n_servers; i++) {
			CollectorStreamObserver<DadkvsPaxos.LearnReply> learn_observer = new CollectorStreamObserver<DadkvsPaxos.LearnReply>(learn_collector);
			async_stubs[i].learn(request.build(), learn_observer);
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		serverState.logSystem
			.writeLog("[PAXOS (" + paxosIndex + ")] Waiting for learn replys.");

		learn_collector.waitForTarget(1);
		if (learn_responses.size() >= 1) {
			serverState.logSystem
					.writeLog("[PAXOS (" + paxosIndex + ")]\t\tENDING LEARN PHASE - SUCCESS.");
		} else {
			serverState.logSystem
				.writeLog("[PAXOS (" + paxosIndex + ")]\t\tDID NOT RECEIVE LEARN REPLYS");
		}
	}
}
