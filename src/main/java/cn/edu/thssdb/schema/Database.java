package cn.edu.thssdb.schema;

import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import cn.edu.thssdb.schema.Table;

public class Database {

  private String name;
  private HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    recover();
  }

  private void persist() {
    // TODO
  }

  public void create(String name, Column[] columns) throws Exception {
    // 新建表
    Table newTable = new Table(this.name, name, columns);
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
