package dadkvs.server;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class LogSystem {
	private final String _logsPath = "./logs";
	private final String _currentSessionPath;

	private final Object lock = new Object();

	public LogSystem(final String serverIP, final int logRotation) {
		final LocalDateTime currentDateTime = LocalDateTime.now();
		final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd|MM|yyyy-HH:mm:ss");
		final String formattedDateTime = currentDateTime.format(formatter);

		_currentSessionPath = serverIP + "-" + formattedDateTime + ".txt";
		System.out.println("Current log session: " + _currentSessionPath);

		final File folder = new File(_logsPath);
		synchronized (lock) {
			if (!folder.exists()) {
				folder.mkdir();
			}
		}

		final List<File> myLogs = new ArrayList<>();
		final File[] fileList = folder.listFiles();
		if (fileList != null) {
			System.out.println("Existing logs:");
			for (final File file : fileList) {
				if (file.isFile() && file.getName().startsWith(serverIP)) {
					System.out.println(file.getName());
					myLogs.add(file);
				}
			}
		}

		if (myLogs.size() + 1 > logRotation) {
			for (int i = logRotation - 1; i < myLogs.size(); i++) {
				final File fileToDelete = myLogs.get(i);
				if (fileToDelete.delete()) {
					System.out.println("Deleted log file: " + fileToDelete.getName());
				} else {
					System.err.println("Failed to delete log file: " + fileToDelete.getName());
				}
			}
		}
	}

	public void writeLog(final String content) {
		synchronized (lock) {
			try (FileWriter fw = new FileWriter(_logsPath + "/" + _currentSessionPath, true);
					PrintWriter pw = new PrintWriter(fw)) {
				final LocalDateTime currentDateTime = LocalDateTime.now();
				final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
				final String formattedTime = currentDateTime.format(formatter);
				final Thread currentThread = Thread.currentThread();
				pw.println("[" + formattedTime + " - " + currentThread.getName() + "] " + content);
			} catch (final IOException e) {
				System.err.println("An error occurred while writing to the file: " + e.getMessage());
			}
		}
	}
}
