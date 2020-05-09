package cn.edu.thssdb.schema;

import org.junit.Before;
import org.junit.Test;

public class schemaTest {
    private Manager manager;
    private String databaseName;

    @Before
    public void setUp() {
        this.manager = Manager.getInstance();
        this.databaseName = "database1";
        this.manager.switchDatabase(this.databaseName);
    }

    @Test
    public void testInsert() {
        Database DB = this.manager.getCurrentDB();
        //Column[] columns = new Column[0];

        //DB.create("table1", columns);
    }
}
