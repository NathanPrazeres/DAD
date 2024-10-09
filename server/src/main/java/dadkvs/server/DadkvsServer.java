package dadkvs.server;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

public class DadkvsServer {

	static DadkvsServerState serverState;

	/** Server host port. */
	private static int port;

	public static void main(final String[] args) throws Exception {
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

		final int basePort = Integer.valueOf(args[0]);
		final int myId = Integer.valueOf(args[1]);

		serverState = new DadkvsServerState(kvsize, basePort, myId);

		port = basePort + myId;

		final BindableService serviceImpl = new DadkvsMainServiceImpl(serverState);
		final BindableService consoleImpl = new DadkvsConsoleServiceImpl(serverState);
		final BindableService paxosImpl = new DadkvsPaxosServiceImpl(serverState);

		// Create a new server to listen on port.
		final Server server = ServerBuilder.forPort(port).addService(serviceImpl).addService(consoleImpl)
				.addService(paxosImpl)
				.build();
		// Start the server.
		server.start();
		// Server threads are running in the background.
		System.out.println("Server started");

		// Do not exit the main thread. Wait until server is terminated.
		server.awaitTermination();
	}
}
