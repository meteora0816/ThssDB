package cn.edu.thssdb.schema;

import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import cn.edu.thssdb.schema.Table;

public class Database {

  private String baseDir;
  private String name;
  private String DBdir;
  private HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;

  public Database(String baseDir, String name) throws IOException {
    this.baseDir = baseDir;
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();

    this.DBdir = baseDir + '/' + name;
    File DBmeta = new File(this.DBdir + "/" + this.name + ".meta");
    File DB = new File(this.DBdir);
    if (!DB.exists()) {
      DB.mkdir();
      DBmeta.createNewFile();
    }
    recover();
  }

  private void persist() {
    // TODO
  }

  public void create(String name, Column[] columns) throws Exception {
    // 新建表
    Table newTable = new Table(this.DBdir, name, columns);
    tables.put(name, newTable);
  }

  public void drop(String name) {
    // 删除表
    tables.remove(name);
  }

  public String select(QueryTable[] queryTables) {
    // TODO
    QueryResult queryResult = new QueryResult(queryTables);
    return null;
  }

  private void recover() {
    // TODO
  }

  public void quit() {
    // TODO
  }
}
