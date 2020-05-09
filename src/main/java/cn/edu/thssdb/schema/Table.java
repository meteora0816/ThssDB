package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.utils.NDException;
import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  private String filename;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
    // 每张表由一颗B+树索引
    // primary key为key，row为value
  private int primaryIndex; // columns中的主键index

  public Table(String databaseName, String tableName, Column[] columns)
          throws Exception
            // 新建表
            // columns[] 每一列的元信息
  {
    this.databaseName = databaseName;
    this.tableName = tableName;

    this.primaryIndex = -1;
    Collections.addAll(this.columns, columns);

    this.filename = this.databaseName + "/" + this.tableName;
    File metaFile = new File(this.filename + ".meta"); // 元数据
    File dataFile = new File(this.filename + ".data");
    metaFile.createNewFile();
    dataFile.createNewFile();
  }

  private void recover() {
    // TODO
  }

  public void insert(Row row) throws NDException {
    int n = row.entries.size();

    // check null
    for (int i = 0; i < n; ++ i) {
      Entry val = row.entries.get(i);
      if (val == null && this.columns.get(i).notNull()) {
        // 该列不能为null
        throw new NDException(this.columns.get(i).name() + " can't be null");
      }
    }

    if (this.index != null) {
      this.index.put(row.entries.get(this.primaryIndex), row);
    }
  }

  public void delete(Entry entry) {
    this.index.remove(entry);
  }

  public void update(Entry entry) {
    // TODO
  }

  private void serialize() {
    // TODO
  }

  private ArrayList<Row> deserialize() {
    // TODO
    return null;
  }

  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = table.index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next().getValue();
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }
}
