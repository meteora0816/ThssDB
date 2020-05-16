package cn.edu.thssdb.parser;

import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.NDException;
import com.sun.org.apache.bcel.internal.generic.GotoInstruction;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SQLExecListener extends SQLBaseListener {
    private Manager manager;
    private ExecuteStatementResp resp = new ExecuteStatementResp();
    private Status status = new Status();
    private boolean success = true;

    @Override
    public void enterParse(SQLParser.ParseContext ctx){
        status.setMsg("\n");
        try {
            manager = new Manager();
            manager.switchDatabase("public");
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    @Override
    public void exitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {
        //resp = new ExecuteStatementResp();
        String dbName = ctx.database_name().getText();
        //System.out.println("dbname: "+dbName);
        try{
            if(manager.containDatabase(dbName)){
                //Status status = new Status(Global.FAILURE_CODE);
                success = false;
                status.setMsg(status.msg+"Duplicated database name.\n");
                //resp.setStatus(status);
            }
            else {
                manager.switchDatabase(dbName);
                //manager.quit();
                //Status status = new Status(Global.SUCCESS_CODE);
                //success = success;
                status.setMsg(status.msg+"Database created successfully.\n");
                //resp.setStatus(status);
            }
        }catch (IOException e){
            //Status status = new Status(Global.FAILURE_CODE);
            success = false;
            status.setMsg(status.msg+"Failed to create database file.\n");
            //System.out.println("Failed to create database");
            e.printStackTrace();
        }
    }

    @Override
    public void exitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) {
        String dbName = ctx.database_name().getText();
        if(manager.containDatabase(dbName)){
            manager.deleteDatabase(dbName);
            //success = success&&true;
            status.setMsg(status.msg+"Database deleted successfully.\n");
        }
        else{
            success = false;
            status.setMsg(status.msg+"Database failed to delete.\n");
        }
    }

    @Override
    public void exitUse_db_stmt(SQLParser.Use_db_stmtContext ctx) {
        String dbName = ctx.database_name().getText();
        try {
            manager.switchDatabase(dbName);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void exitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        String tableName = ctx.table_name().getText();
        List<SQLParser.Column_defContext> columnDefCtxs = ctx.column_def();
        int numOfColumns = columnDefCtxs.size();
        Column[] columns = new Column[numOfColumns];
        for(int i=0;i<numOfColumns;i++){
            SQLParser.Column_defContext column_defContext = columnDefCtxs.get(i);
            String columnName = column_defContext.column_name().getText();
            String typeRaw = column_defContext.type_name().getText();
            //预处理type
            typeRaw = typeRaw.toUpperCase();
            typeRaw.replaceAll(" ","");
            String typeLength = "32";
            if(typeRaw.charAt(0)=='S'){
                typeLength = "";
                for(int j=7;j<typeRaw.length()-1;j++){
                    typeLength+=typeRaw.charAt(j);
                }
                typeRaw = "STRING";
            }
            int maxLength = Integer.parseInt(typeLength);
            ColumnType columnType = ColumnType.valueOf(typeRaw);
            boolean notNull = false;
            List<SQLParser.Column_constraintContext> column_constraintContexts = column_defContext.column_constraint();
            if(!column_constraintContexts.isEmpty()) {
                String columnConstraint = column_constraintContexts.get(0).getText();//只实现not null
                //System.out.println("columnConstraint: "+columnConstraint);
                if (columnConstraint.toUpperCase().equals("NOTNULL")) {
                    notNull = true;
                }
            }
            columns[i] = new Column(columnName,columnType,0,notNull,maxLength);
        }
        List<SQLParser.Column_nameContext> table_constraintContexts = ctx.table_constraint().column_name();
        //System.out.println(table_constraintContexts.size());
        if(!table_constraintContexts.isEmpty()) {
            //String primaryName = ctx.table_constraint().column_name(0).getText();//only one primary key
            //System.out.println(primaryName);
            List<SQLParser.Column_nameContext> column_nameContexts = ctx.table_constraint().column_name();
            ArrayList<String> primaryNames = new ArrayList<>();
            int numOfPrimary = column_nameContexts.size();
            for(int k=0;k<numOfPrimary;k++){
                primaryNames.add(column_nameContexts.get(k).getText());
            }
            System.out.println(Arrays.toString(primaryNames.toArray()));
            for (int i = 0; i < numOfColumns; i++) {
                //System.out.println(columns[i].name());
                if (primaryNames.contains(columns[i].name())) {
                    columns[i].setPrimary(1);
                    //break;
                }
            }
        }
        try {
            Database db = manager.getCurrentDB();
            if(db.containsTable(tableName)){
                success = false;
                status.msg += "Duplicated tableName.\n";
            }
            else {
                db.create(tableName, columns);
                status.msg += "Create table successfully.\n";
            }
        }catch (Exception e){
            success = false;
            status.msg+="Failed to create table.";
        }
    }

    @Override
    public void exitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        String tableName = ctx.table_name().getText();
        manager.getCurrentDB().dropTable(tableName);
        status.msg+="Drop table successfully.";
    }

    //show table
    @Override
    public void exitShow_meta_stmt(SQLParser.Show_meta_stmtContext ctx) {
        String tableName = ctx.table_name().getText();
        status.msg+="Table: "+tableName+"\n";
        Table table = manager.getCurrentDB().getTable(tableName);
        List<Column> columns = table.columns;
        int len = columns.size();
        for(int i=0;i<len;i++){
            status.msg+=columns.get(i).toString();
        }
    }

    @Override
    public void
    exitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        String tableName = ctx.table_name().getText();
        List<SQLParser.Column_nameContext> column_nameContexts = ctx.column_name();
        int numOfColumn = column_nameContexts.size();
        String[] columnNames = new String[numOfColumn];
        for(int i=0;i<numOfColumn;i++){
            columnNames[i] = column_nameContexts.get(i).getText();
        }
        List<SQLParser.Value_entryContext> value_entryContexts = ctx.value_entry();
        int numOfEntries = value_entryContexts.size();
        Table currentTable = manager.getCurrentDB().getTable(tableName);

        Entry[] entries = new Entry[numOfEntries];
        for(int i=0;i<numOfEntries;i++){
            entries[i] = new Entry(value_entryContexts.get(i).getText());
        }
        Row insertRow;

        if(numOfColumn == 0){
            // 默认输入，entries不调整
            insertRow = new Row(entries);
        }
        else{
            int numOfRealColumns = currentTable.columns.size();
            Entry[] realEntries = new Entry[numOfRealColumns];
            for(int i=0;i<numOfRealColumns;i++){
                realEntries[i] = null;
            }
            for(int i=0;i<numOfColumn;i++){ //check every column
                int index;
                for(index=0;index<numOfRealColumns;index++){
                    if(currentTable.columns.get(index).name().equals(columnNames[i])){
                        break;
                    }
                }
                realEntries[index] = new Entry(entries[i]);
            }
            insertRow = new Row(realEntries);
        }

        try {
            currentTable.insert(insertRow);
        }catch(NDException e){
            success = false;
            status.msg+="Some of your insert values cannot be null.\n";
        }
    }

    @Override
    public void exitParse(SQLParser.ParseContext ctx) {
        manager.quit();
    }


    public ExecuteStatementResp getResult(){
        if(success){
            status.setCode(Global.SUCCESS_CODE);
        }
        else{
            status.setCode(Global.FAILURE_CODE);
        }
        resp.setStatus(status);
        return resp;
    }
}
