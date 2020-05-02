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
  private int primaryIndex;

  public Table(String databaseName, String tableName, Column[] columns) throws Exception {
    this.databaseName = databaseName;
    this.tableName = tableName;

    this.primaryIndex = -1;
    Collections.addAll(this.columns, columns);

    this.filename = this.databaseName + "/" + this.tableName;
    File metaFile = new File(this.filename + ".meta");
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
      Object val = row.entries.get(i);
      String[] column = this.columns.get(i).toString().split(",");
      // val is null
      if (val == null) {
        // col can't be null
        if (column[3] == "0")
          throw new NDException(column[0] + " can't be null");
        i++;
      }
    }

    if (this.index != null) {
      this.index.put(row.entries.get(this.primaryIndex), row);
    }
  }

  public void delete(Row row) {
    if (this.primaryIndex!=-1) {
      this.index.remove(row.entries.get(this.primaryIndex));
    }
  }

  public void update() {
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
