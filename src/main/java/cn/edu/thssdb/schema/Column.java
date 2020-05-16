package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnType;

import java.io.Serializable;

public class Column implements Comparable<Column>, Serializable {
  private String name;
  private ColumnType type;
  private int primary;
  private boolean notNull;
  private int maxLength;

  public Column(String name, ColumnType type, int primary, boolean notNull, int maxLength) {
    this.name = name;
    this.type = type;
    this.primary = primary;
    this.notNull = notNull;
    this.maxLength = maxLength;
  }

  @Override
  public int compareTo(Column e) {
    return name.compareTo(e.name);
  }

  public String toString() {
    return "columnName: "+name + ',' + "type: "+type + ',' + "primary: "+primary + ','
            + "notNull: "+notNull + ',' + "maxLength: "+maxLength+"\n";
  }

  public String name() {return name;}
  public boolean notNull() {return notNull;}
  public void setPrimary(int primary){
    this.primary = primary;
  }
  public boolean isPrimary(){
    return this.primary!=0;
  }
}
