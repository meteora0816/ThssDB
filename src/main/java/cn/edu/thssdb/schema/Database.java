package cn.edu.thssdb.schema;

import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.utils.DBLogger;

public class Database {

  private String name; // 数据库名称
  private String DBdir; // 数据库存储路径
  private HashMap<String, Table> tables; // 数据库中的所有表
  private HashMap<String, Boolean> modified; // 相应的表有无修改

  public DBLogger dbLogger;
  ReentrantReadWriteLock lock;

  public Database(String baseDir, String name) throws IOException {
    // baseDir 存储根目录, name 数据库名称
    this.name = name;
    this.DBdir = baseDir + '/' + name;
    this.tables = new HashMap<>();
    this.modified = new HashMap<>();

    this.lock = new ReentrantReadWriteLock();
    this.dbLogger = new DBLogger(name);

    File DBmeta = new File(this.DBdir + "/" + this.name + ".meta");
    File DB = new File(this.DBdir);
    if (!DB.exists()) {
      DB.mkdir();
      DBmeta.createNewFile();
    }
    else {
      recover(DBmeta);
    }
  }

  public boolean containsTable(String tableName){
    return tables.containsKey(tableName);
  }

  public void create(String name, Column[] columns) throws Exception {
    // 新建表
    if (tables.containsKey(name)) {
      System.out.println("table " + name + " already exist.");
    }
    else {
      Table newTable = new Table(this.DBdir, name, columns);
      tables.put(name, newTable);
      modified.put(name, true);
    }
  }

  public void dropTable(String name) {
    // 删除表
    if (tables.containsKey(name)) {
      tables.get(name).drop();
      tables.remove(name);
    }
    else {
      System.out.println("table" + name + "doesn't exist.");
    }

  }

  public String select(QueryTable[] queryTables) {
    // TODO
    QueryResult queryResult = new QueryResult(queryTables);
    return null;
  }

  public Table getTable(String name) {
    modified.put(name, true);
    return tables.get(name);
  }

  private void recover(File DBmeta) {
    // 从磁盘恢复数据库
    System.out.println("Recover database " + this.name);
    try {
      FileReader reader = new FileReader(DBmeta);
      char[] buf = new char[1024];
      reader.read(buf);
      reader.close();
      String[] vals = String.valueOf(buf).split("\\|");
      System.out.println(vals[0] + " table(s)");
      int tableNum = Integer.parseInt(vals[0]);
      for (int i=0;i<tableNum;i++) {
        String tableName = vals[i+1];
        Table newTable = new Table(this.DBdir, tableName);
        tables.put(tableName, newTable);
        modified.put(tableName, false);
      }
    } catch(IOException e) {
      e.printStackTrace();
    }
  }

  private void persist() {
    // 变更存储到磁盘
    // System.out.println(this.name+ ": persist");
    // 每一张表分别存储
    for (String key : tables.keySet()) {
      if (modified.get(key) == true) {
        Table table = tables.get(key);
        table.persist();
        modified.put(key, false);
      }
      else {
        // System.out.println("skip");
      }
    }
    // 存储元数据（有哪些表）
    /* databaseName.meta
       Table Num|Table1|Table2|... */
    try {
      OutputStream fop = new FileOutputStream(this.DBdir + "/" + this.name + ".meta");
      OutputStreamWriter writer = new OutputStreamWriter(fop, "UTF-8");
      writer.append(String.valueOf(tables.size()));
      writer.append("|");
      for (String key : tables.keySet()) {
        writer.append(key).append("|");
      }
      writer.close();
      fop.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public void drop() {
    for (String key : tables.keySet()) {
      Table table = tables.get(key);
      table.drop();
    }
    this.tables.clear();
    File metaFile = new File(this.DBdir + "/" + this.name + ".meta");
    metaFile.delete();
    File logFile = new File(this.DBdir + "/" + this.name + ".log");
    logFile.delete();
    File dir = new File(this.DBdir);
    dir.delete();
  }

  public void quit() {
    // 退出数据库
    for (String key : this.tables.keySet()) {
      this.tables.get(key).persist();
    }
    this.persist();
  }

  public String show(){
    // 显示数据库信息
    Set<String> set = this.tables.keySet();
    StringBuilder ret = new StringBuilder();
    ret.append(this.name).append(":\n");
    ret.append(this.tables.size()).append(" table(s)\n");
    for(String tableName:set){
      ret.append(tableName).append("\n");
    }
    return ret.toString();
  }
}
