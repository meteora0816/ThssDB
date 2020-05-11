package cn.edu.thssdb.schema;

import cn.edu.thssdb.server.ThssDB;

import javax.xml.crypto.Data;
import java.io.*;
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
    databases = new HashMap<>();

    File DBDir = new File(baseDir);
    File DBmeta = new File(baseDir+"/"+metaFile);
    if (!DBDir.exists()) {
      DBDir.mkdir();
      DBmeta.createNewFile();
    }
    else {
      recover();
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
    // 切换数据库，如果没有就新建
    if (currentDB != null) {
      currentDB.quit();
    }
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

  public void quit() {
    // 存储元数据（有哪些数据库）
    /* DB.meta
       database num
       database1
       database2
       ... */
    try {
      OutputStream fop = new FileOutputStream(this.baseDir + "/" + metaFile);
      OutputStreamWriter writer = new OutputStreamWriter(fop, "UTF-8");
      writer.append(String.valueOf(databases.size()));
      writer.append("|");
      for (String key : databases.keySet()) {
        writer.append(key).append("|");
      }
      writer.close();
      fop.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void recover() {

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
