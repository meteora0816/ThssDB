package cn.edu.thssdb.parser;

import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.utils.Global;


import java.io.IOException;

public class SQLExecListener extends SQLBaseListener {
    private Manager manager;
    private ExecuteStatementResp resp;
    @Override
    public void exitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {
        resp = new ExecuteStatementResp();
        String dbName = ctx.database_name().toString();
        System.out.println("dbname: "+dbName);
        try{
            if(manager == null) {
                manager = new Manager();
            }
            if(manager.containDatabase(dbName)){
                Status status = new Status(Global.FAILURE_CODE);
                status.setMsg("Duplicated database name.");
                resp.setStatus(status);
            }
            else {
                manager.switchDatabase(dbName);
                Status status = new Status(Global.SUCCESS_CODE);
                status.setMsg("Database created successfully.");
                resp.setStatus(status);
            }
        }catch (IOException e){
            Status status = new Status(Global.FAILURE_CODE);
            status.setMsg("Failed to create database file.");
            //System.out.println("Failed to create database");
            e.printStackTrace();
        }
    }


    public ExecuteStatementResp getResult(){
        return resp;
    }
}
