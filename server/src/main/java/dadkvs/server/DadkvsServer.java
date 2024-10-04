package dadkvs.server;

import java.util.ArrayList;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsFastPaxosServiceGrpc;
import dadkvs.DadkvsFastPaxos;

import dadkvs.util.GenericResponseCollector;
import dadkvs.util.CollectorStreamObserver;

public class DadkvsServer {

	static DadkvsServerState server_state;

	static DadkvsFastPaxosServiceGrpc.DadkvsFastPaxosServiceStub[] async_stubs;

	/** Server host port. */
	private static int port;

	public static void main(String[] args) throws Exception {
		final int kvsize = 1000;

		System.out.println(DadkvsServer.class.getSimpleName());

		// Print received arguments.
		System.out.printf("Received %d arguments%n", args.length);
		for (int i = 0; i < args.length; i++) {
			System.out.printf("arg[%d] = %s%n", i, args[i]);
		}

		// Check arguments.
		if (args.length < 2) {
			System.err.println("Argument(s) missing!");
			System.err.printf("Usage: java %s baseport replica-id%n", Server.class.getName());
			return;
		}

		int base_port = Integer.valueOf(args[0]);
		int my_id = Integer.valueOf(args[1]);

		server_state = new DadkvsServerState(kvsize, base_port, my_id);

		port = base_port + my_id;

		// create stubs to communicate between servers
		String[] targets = new String[server_state.n_servers - 1];
		int j = 0;
		for (int i = 0; i < server_state.n_servers; i++) {
			if (i == my_id)
				continue;
			int target_port = base_port + i;
			targets[j] = new String();
			targets[j] = "localhost:" + target_port;
			System.out.printf("targets[%d] = %s%n", j, targets[j]);
			j++;
		}

		ManagedChannel[] channels = new ManagedChannel[server_state.n_servers - 1];
		for (int i = 0; i < server_state.n_servers - 1; i++) {
			channels[i] = ManagedChannelBuilder.forTarget(targets[i]).usePlaintext().build();
		}

		async_stubs = new DadkvsFastPaxosServiceGrpc.DadkvsFastPaxosServiceStub[server_state.n_servers - 1];

		for (int i = 0; i < server_state.n_servers - 1; i++) {
			async_stubs[i] = DadkvsFastPaxosServiceGrpc.newStub(channels[i]);
		}

		final BindableService service_impl = new DadkvsMainServiceImpl(server_state, async_stubs);
		final BindableService console_impl = new DadkvsConsoleServiceImpl(server_state);
		final BindableService paxos_impl = new DadkvsPaxosServiceImpl(server_state);

		// Create a new server to listen on port.
		Server server = ServerBuilder.forPort(port).addService(service_impl).addService(console_impl).addService(paxos_impl)
				.build();
		// Start the server.
		server.start();
		// Server threads are running in the background.
		System.out.println("Server started");

		// Do not exit the main thread. Wait until server is terminated.
		server.awaitTermination();
	}
}
