package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnType;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class schemaTest {
    private Manager manager;
    private String databaseName;

    @Before
    public void setUp() throws Exception {
        this.manager = Manager.getInstance();
        this.databaseName = "database1";
        this.manager.switchDatabase(this.databaseName);

        Database DB = this.manager.getCurrentDB();
        Column[] columns = new Column[3];
        columns[0] = new Column("name", ColumnType.STRING, 0, false, 10);
        columns[1] = new Column("ID", ColumnType.INT, 0, false, 10);
        columns[2] = new Column("department", ColumnType.STRING, 0, false, 10);
        DB.create("table1", columns);

        Entry[] Entries = new Entry[3];
        Entries[0] = new Entry("meteora");
        Entries[1] = new Entry("2017000001");
        Entries[2] = new Entry("SS");
        Row row= new Row(Entries);
        DB.getTable("table1").insert(row);

        Entries[0] = new Entry("lxy");
        Entries[1] = new Entry("2017000002");
        Entries[2] = new Entry("CS");
        row= new Row(Entries);
        DB.getTable("table1").insert(row);

        columns[0] = new Column("name", ColumnType.STRING, 0, false, 10);
        columns[1] = new Column("Salary", ColumnType.INT, 0, false, 10);
        columns[2] = new Column("department", ColumnType.STRING, 0, false, 10);
        DB.create("table2", columns);

        Entries[0] = new Entry("lixingyao");
        Entries[1] = new Entry("2000000");
        Entries[2] = new Entry("CS");
        row= new Row(Entries);
        DB.getTable("table2").insert(row);
    }
    /*
    @Test
    public void testInsert() throws Exception {
        Database DB = this.manager.getCurrentDB();

        Row ans = DB.getTable("table1").index.get(new Entry("lxy"));
        System.out.println(ans.entries);
    }*/

    /*
    @Test
    public void testRemove() throws Exception {
        Database DB = this.manager.getCurrentDB();
        Entry entry = new Entry("lxy");
        Table table = DB.getTable("table1");

        table.delete(entry);

        Row ans = DB.getTable("table1").getRow(new Entry("lxy"));
        System.out.println(ans);
    }*/

    @Test
    public void testPersist() throws Exception {
        Database DB = this.manager.getCurrentDB();
        DB.quit();
        manager.quit();
    }


}
