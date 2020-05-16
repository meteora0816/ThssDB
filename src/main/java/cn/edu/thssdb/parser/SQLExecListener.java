package cn.edu.thssdb.parser;

import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;
import com.sun.org.apache.bcel.internal.generic.GotoInstruction;


import java.io.IOException;
import java.util.List;

public class SQLExecListener extends SQLBaseListener {
    private Manager manager;
    private ExecuteStatementResp resp = new ExecuteStatementResp();
    private Status status = new Status();
    private boolean success = true;

    @Override
    public void enterParse(SQLParser.ParseContext ctx){
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
            if(typeRaw.charAt(0)=='S'){

            }
            ColumnType columnType = ColumnType.valueOf(column_defContext.type_name().getText());

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
