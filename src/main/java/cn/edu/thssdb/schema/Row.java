package cn.edu.thssdb.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringJoiner;

public class Row implements Serializable {
  // 某一行记录在表中所有列的数据
  private static final long serialVersionUID = -5809782578272943999L;
  protected ArrayList<Entry> entries;

  public Row() {
    this.entries = new ArrayList<>();
  }

  public Row(Entry[] entries) {
    this.entries = new ArrayList<>(Arrays.asList(entries));
  }

  public ArrayList<Entry> getEntries() {
    return entries;
  }

  public void appendEntries(ArrayList<Entry> entries) {
    this.entries.addAll(entries);
  }

  public String toString() {
    if (entries == null)
      return "EMPTY";
    StringJoiner sj = new StringJoiner(", ");
    for (Entry e : entries) {
      if(e!=null) {
        sj.add(e.toString());
      }else{
        sj.add("null");
      }
    }
    return sj.toString();
  }
}

