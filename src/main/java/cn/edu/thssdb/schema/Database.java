package cn.edu.thssdb.schema;

import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import cn.edu.thssdb.schema.Table;

public class Database {

  private String name; // 数据库名称
  private String DBdir; // 数据库存储路径
  private HashMap<String, Table> tables; // 数据库中的所有表
  ReentrantReadWriteLock lock;

  public Database(String baseDir, String name) throws IOException {
    // baseDir 存储根目录, name 数据库名称
    this.name = name;
    this.DBdir = baseDir + '/' + name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();

    File DBmeta = new File(this.DBdir + "/" + this.name + ".meta");
    File DB = new File(this.DBdir);
    if (!DB.exists()) {
      DB.mkdir();
      DBmeta.createNewFile();
    }
    else {
      recover();
    }
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

  public Table getTable(String name) {
    return tables.get(name);
  }

  private void recover() {
    // 从磁盘恢复数据库

  }

  private void persist() {
    // 变更存储到磁盘
    System.out.println("database: persist");
    // 每一张表分别存储
    for (String key : tables.keySet()) {
      Table table = tables.get(key);
      table.persist();
    }
    // 存储元数据（有哪些表）
    try {
      OutputStream fop = new FileOutputStream(this.DBdir + "/" + this.name + ".meta");
      OutputStreamWriter writer = new OutputStreamWriter(fop, "UTF-8");
      writer.append(String.valueOf(tables.size()));
      writer.append("\r\n");
      for (String key : tables.keySet()) {
        writer.append(key).append("\r\n");
      }
      writer.close();
      fop.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public void quit() {
    // 退出数据库
    this.persist();
  }
}
