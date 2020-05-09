package cn.edu.thssdb.schema;

import cn.edu.thssdb.server.ThssDB;

import javax.xml.crypto.Data;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private HashMap<String, Database> databases;
  private Database currentDB;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    // TODO
    databases = new HashMap<>();
  }

  private Database createDatabaseIfNotExists(String name) {
    Database newDB = new Database(name);
    databases.put(name, newDB);
    return newDB;
  }

  private void deleteDatabase(String name) {
    if (databases.containsKey(name)) {
      Database delDB = databases.get(name);
      databases.remove(name);
    }
  }

  public void switchDatabase(String name) {
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
    private static final Manager INSTANCE = new Manager();
    private ManagerHolder() {

    }
  }
}
