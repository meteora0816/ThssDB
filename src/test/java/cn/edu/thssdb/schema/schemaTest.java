package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class schemaTest {
    private Manager manager;
    private String database1 = "database1";
    private String database2 = "database2";

    @Before
    public void setUp() throws Exception {
        this.manager = Manager.getInstance();
    }

    @Test
    public void testCreateTable() throws Exception {
        this.manager.switchDatabase(this.database1);
        Database DB = this.manager.getCurrentDB();

        Column[] columns = new Column[3];
        columns[0] = new Column("name", ColumnType.STRING, 0, false, 10);
        columns[1] = new Column("ID", ColumnType.INT, 0, false, 10);
        columns[2] = new Column("department", ColumnType.STRING, 0, false, 10);
        DB.create("table1", columns);

        columns[0] = new Column("name", ColumnType.STRING, 0, false, 10);
        columns[1] = new Column("Salary", ColumnType.INT, 0, false, 10);
        columns[2] = new Column("department", ColumnType.STRING, 0, false, 10);
        DB.create("table2", columns);
    }

    @Test
    public void testTableInsert() throws Exception {
        this.manager.switchDatabase(this.database1);
        Database DB = this.manager.getCurrentDB();

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

        Entries[0] = new Entry("lixingyao");
        Entries[1] = new Entry("2000000");
        Entries[2] = new Entry("CS");
        row= new Row(Entries);
        DB.getTable("table2").insert(row);
    }

    @Test
    public void testRemoveRow() throws Exception {
        this.manager.switchDatabase(this.database1);
        Database DB = this.manager.getCurrentDB();

        Entry entry = new Entry("lxy");
        DB.getTable("table1").delete(entry);
    }

    @Test
    public void testDropTable() throws Exception {
        this.manager.switchDatabase(this.database1);
        Database DB = this.manager.getCurrentDB();

        DB.dropTable("table2");
    }

    @Test
    public void testUpdate() throws IOException {
        this.manager.switchDatabase(this.database1);
        Database DB = this.manager.getCurrentDB();

        Entry primaryKey = new Entry("lxy");

        Entry[] Entries = new Entry[3];
        Entries[0] = primaryKey;
        Entries[1] = new Entry("2017000002");
        Entries[2] = new Entry("SS");
        Row row = new Row(Entries);

        DB.getTable("table1").update(primaryKey, row);
    }

    @Test
    public void testDeleteDatabase() {
        this.manager.deleteDatabase(this.database1);
    }

    @After
    public void quit() {
        manager.quit();
    }
}
