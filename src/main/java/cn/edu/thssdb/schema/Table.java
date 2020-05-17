package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.utils.NDException;
import javafx.util.Pair;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
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

    for(int i=0;i<this.columns.size();i++){
        if(this.columns.get(i).isPrimary()){
          this.primaryIndex = i;
          break;
        }
    }
    System.out.println("primaryIndex: "+this.primaryIndex);
    this.tableDir = this.databaseDir + "/" + this.tableName;
    File tableDir = new File(this.tableDir);
    // System.out.println(this.tableDir);
    File metaFile = new File(this.tableDir + "/" + this.tableName + ".meta"); // 元数据
    File dataFile = new File(this.tableDir + "/" + this.tableName + ".data");
    if (!tableDir.exists()) {
      tableDir.mkdir();
      metaFile.createNewFile();
      dataFile.createNewFile();
    }
    else {
      System.out.println("Table " + this.tableName + " already exist.");
    }
  }

  public Table(String databaseDir, String tableName) {
    // 恢复存储在磁盘的表
    this.databaseDir = databaseDir;
    this.tableName = tableName;
    this.index = new BPlusTree<>();
    this.primaryIndex = 0;
    this.columns = new ArrayList<>();


    this.tableDir = this.databaseDir + "/" + this.tableName;
    File tableDir = new File(this.tableDir);
    if (!tableDir.exists()) {
      System.out.println("Table " + this.tableName + " doesn't exist.");
    }
    else {
      recover();
    }
  }

  private void recover() {
    // 从磁盘恢复元数据和数据
    System.out.println("Recover table " + this.tableName);
    try {
      FileInputStream fileIn = new FileInputStream(this.tableDir + "/" + this.tableName + ".meta");
      ObjectInputStream in = new ObjectInputStream(fileIn);
      this.columns = (ArrayList<Column>) in.readObject();
      in.close();
      fileIn.close();
      for(int i=0;i<this.columns.size();i++){
        if(this.columns.get(i).isPrimary()){
          this.primaryIndex = i;
          break;
        }
      }
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    }
    try {
      FileInputStream fileIn = new FileInputStream(this.tableDir + "/" + this.tableName + ".data");
      ObjectInputStream in = new ObjectInputStream(fileIn);
      this.index = (BPlusTree<Entry, Row>) in.readObject();
      in.close();
      fileIn.close();
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public void insert(Row row) throws NDException {
    int n = row.entries.size();
    System.out.println(Arrays.toString(row.entries.toArray()));
    // check null
    for (int i = 0; i < n; ++ i) {
      Entry val = row.entries.get(i);
      if (val == null && this.columns.get(i).notNull()) {
        // 该列不能为null
        throw new NDException(this.columns.get(i).name() + " can't be null");
      }
    }

    if (this.index != null) {
      try {
        this.index.put(row.entries.get(this.primaryIndex), row);
      } catch (DuplicateKeyException e) {
        e.printStackTrace();
      }

    }
  }

  public void delete(Entry entry) {
    System.out.println("delete key \"" + entry + "\" in " + this.tableName);
    this.index.remove(entry);
  }

  public void update(Entry entry, Row row) {
    if (this.index.contains(entry)) {
      this.index.update(entry, row);
      System.out.println("update row \""+row+"\" in "+this.tableName);
    }
    else {
      System.out.println("Row " + entry.value + " doesn't exist.");
    }
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
    // 持久化到磁盘
    System.out.println(this.tableName + ": persist");
    serialize();
    saveMetaData();
  }

  private void serialize() {
    // 序列化存储数据
    try {
      FileOutputStream fileOut = new FileOutputStream(this.tableDir + "/" + this.tableName + ".data");
      ObjectOutputStream out = new ObjectOutputStream(fileOut);
      out.writeObject(this.index);
      out.close();
      fileOut.close();
      System.out.println("Serialized data is saved in " + this.tableDir + "/" + this.tableName + ".data");
    } catch(IOException i) {
      i.printStackTrace();
    }
  }

  private void saveMetaData() {
    // 存储元数据（表的结构）
    try {
      FileOutputStream fileOut = new FileOutputStream(this.tableDir + "/" + this.tableName + ".meta");
      ObjectOutputStream out = new ObjectOutputStream(fileOut);
      out.writeObject(this.columns);
      out.close();
      fileOut.close();
      System.out.println("Serialized metadata is saved in " + this.tableDir + "/" + this.tableName + ".meta");
    } catch(IOException i) {
      i.printStackTrace();
    }
  }

  public void drop() {
    System.out.println("drop table: " + this.tableName);
    // 删除文件
    File file = new File(this.tableDir);
    File[] files = file.listFiles();
    for (int i=0;i<files.length;i++) {
      files[i].delete();
    }
    file.delete();
  }

  public int primaryIndex(){
    return this.primaryIndex;
  }

  public int columnNumber(){
    return this.columns.size();
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
