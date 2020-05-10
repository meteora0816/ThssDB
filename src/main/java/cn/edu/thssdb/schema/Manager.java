package cn.edu.thssdb.schema;

import cn.edu.thssdb.server.ThssDB;

import javax.xml.crypto.Data;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private HashMap<String, Database> databases;
  private Database currentDB;
  private String baseDir = "data";
  private String metaFile = "DB.meta";
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() throws IOException {
    // TODO
    databases = new HashMap<>();

    File DBDir = new File(baseDir);
    File DBmeta = new File(baseDir+"/"+metaFile);
    if (!DBDir.exists()) {
      DBDir.mkdir();
      DBmeta.createNewFile();
    }
  }

  private Database createDatabaseIfNotExists(String name) throws IOException {
    Database newDB = new Database(baseDir, name);
    databases.put(name, newDB);
    return newDB;
  }

  private void deleteDatabase(String name) {
    if (databases.containsKey(name)) {
      Database delDB = databases.get(name);
      databases.remove(name);
    }
  }

  public void switchDatabase(String name) throws IOException {
    if (databases.containsKey(name)) {
      currentDB = databases.get(name);
    }
    else {
      currentDB = createDatabaseIfNotExists(name);
      databases.put(name, currentDB);
    }
  }

  public Database getCurrentDB() {
    return currentDB;
  }

  private static class ManagerHolder {
    private static Manager INSTANCE;

    static {
      try {
        INSTANCE = new Manager();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    private ManagerHolder() {

    }
  }
}
