package dadkvs.server;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalTime;
import java.io.PrintWriter;
import java.io.IOException;
import java.io.File;

public class LogSystem {
    private final String _logsPath = "./logs";
    private final String _currentSessionPath;

    private final Object lock = new Object();

    public LogSystem(String serverIP) {
        LocalDateTime currentDateTime = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd|MM|yyyy-HH:mm:ss");
        String formattedDateTime = currentDateTime.format(formatter);

        _currentSessionPath = serverIP + "-" + formattedDateTime + ".txt";
        System.out.println("Current log session: " + _currentSessionPath);

        File folder = new File(_logsPath);
        synchronized (lock) {
            if (!folder.exists()) {
                folder.mkdir();
            }
        }
    }

    public void writeLog(String content) {
        synchronized (lock) {
            try (FileWriter fw = new FileWriter(_logsPath + "/" + _currentSessionPath, true);
                 PrintWriter pw = new PrintWriter(fw)) {
                pw.println(content);
            } catch (IOException e) {
                System.err.println("An error occurred while writing to the file: " + e.getMessage());
            }
        }
    }
}
