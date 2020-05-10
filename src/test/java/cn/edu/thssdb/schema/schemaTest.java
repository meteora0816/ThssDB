package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnType;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class schemaTest {
    private Manager manager;
    private String databaseName;

    @Before
    public void setUp() throws IOException {
        this.manager = Manager.getInstance();
        this.databaseName = "database1";
        this.manager.switchDatabase(this.databaseName);
    }

    @Test
    public void testInsert() throws Exception {
        Database DB = this.manager.getCurrentDB();
        Column[] columns = new Column[3];
        columns[0] = new Column("name", ColumnType.STRING, 0, false, 10);
        columns[1] = new Column("ID", ColumnType.INT, 0, false, 10);
        columns[2] = new Column("department", ColumnType.STRING, 0, false, 10);
        DB.create("table1", columns);
    }
}
