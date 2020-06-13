package cn.edu.thssdb.utils;

import javax.xml.bind.annotation.XmlElementDecl;
import java.io.*;

public class DBLogger {
    private String baseDir = "data";
    private String databaseName;
    private String logPath;

    public DBLogger(String name) {
        databaseName = name;
        logPath = baseDir + "/" + databaseName + "/" + databaseName + ".log";
        File logFile = new File(logPath);
        if (!logFile.exists()) {
            try {
              logFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void append(String str) {

        try {
            FileWriter writer = new FileWriter(logPath, true);
            writer.write(str);
            writer.write("\n");
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void clear(String str) {
        try {
            FileWriter writer = new FileWriter(logPath);
            writer.write("");
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
