package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.utils.NDException;
import javafx.util.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row>, Serializable {
  ReentrantReadWriteLock lock;
  private String databaseDir;
  public String tableName;
  private String tableDir;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
    // 每张表由一颗B+树索引
    // primary key为key，row为value
  private int primaryIndex; // columns中的主键index

  public Table(String databaseDir, String tableName, Column[] columns)
          throws Exception
            // 新建表
            // columns[] 每一列的元信息
  {
    this.databaseDir = databaseDir;
    this.tableName = tableName;
    this.index = new BPlusTree<>();

    this.primaryIndex = 0;
    this.columns = new ArrayList<>();
    Collections.addAll(this.columns, columns);

    this.tableDir = this.databaseDir + "/" + this.tableName;
    File tableDir = new File(this.tableDir);
    System.out.println(this.tableDir);
    if (!tableDir.exists()) {
      tableDir.mkdir();
    }
    File metaFile = new File(this.tableDir + "/" + this.tableName + ".meta"); // 元数据
    File dataFile = new File(this.tableDir + "/" + this.tableName + ".data");
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
    System.out.println(entry);
    this.index.remove(entry);
  }

  public void update(Entry entry) {
    // TODO
  }

  public Row getRow(Entry primarykey) {
    if (this.index.contains(primarykey)) {
      return this.index.get(primarykey);
    }
    else {
      return null;
    }
  }

  public void persist() {
    try {
      System.out.println(this.tableName + "persist");
      FileOutputStream fileOut = new FileOutputStream(this.tableDir + "/" + this.tableName + ".data");
      ObjectOutputStream out = new ObjectOutputStream(fileOut);
      out.writeObject(this);
      out.close();
      fileOut.close();
      System.out.println("Serialized data is saved in " + this.tableDir + "/" + this.tableName + ".data");
    } catch(IOException i) {
      i.printStackTrace();
    }

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
