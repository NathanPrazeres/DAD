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
import java.util.ArrayList;
import java.util.List;

public class LogSystem {
    private final String _logsPath = "./logs";
    private final String _currentSessionPath;
    private int _logRotation;

    private final Object lock = new Object();

    public LogSystem(String serverIP, int logRotation) {
        LocalDateTime currentDateTime = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd|MM|yyyy-HH:mm:ss");
        String formattedDateTime = currentDateTime.format(formatter);

        _logRotation = logRotation;
        _currentSessionPath = serverIP + "-" + formattedDateTime + ".txt";
        System.out.println("Current log session: " + _currentSessionPath);

        File folder = new File(_logsPath);
        synchronized (lock) {
            if (!folder.exists()) {
                folder.mkdir();
            }
        }

        List<File> myLogs = new ArrayList<>();
        File[] fileList = folder.listFiles();
        if (fileList != null) {
            System.out.println("Existing logs:");
            for (File file: fileList) {
                if (file.isFile() && file.getName().startsWith(serverIP)) {
                    System.out.println(file.getName());
                    myLogs.add(file);
                }
            }
        }

        if (myLogs.size() + 1 > logRotation) {
            for (int i = logRotation - 1; i < myLogs.size(); i++) {
                File fileToDelete = myLogs.get(i);
                if (fileToDelete.delete()) {
                    System.out.println("Deleted log file: " + fileToDelete.getName());
                } else {
                    System.err.println("Failed to delete log file: " + fileToDelete.getName());
                }
            }
        }
    }

    public void writeLog(String content) {
        synchronized (lock) {
            try (FileWriter fw = new FileWriter(_logsPath + "/" + _currentSessionPath, true);
                PrintWriter pw = new PrintWriter(fw)) {
                LocalDateTime currentDateTime = LocalDateTime.now();
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
                String formattedTime = currentDateTime.format(formatter);
                pw.println("[" + formattedTime + "] " + content);
            } catch (IOException e) {
                System.err.println("An error occurred while writing to the file: " + e.getMessage());
            }
        }
    }
}