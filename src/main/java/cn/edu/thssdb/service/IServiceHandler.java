package cn.edu.thssdb.service;

import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.parser.*;
import org.apache.thrift.TException;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;


import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Date;

public class IServiceHandler implements IService.Iface {

  static final String USER_INFO = "users.info";
  ArrayList<String> users = new ArrayList<>();
  ArrayList<String> pwds = new ArrayList<>();
  ArrayList<Long> sessionIds = new ArrayList<>();
  public long sessionCnt = 0;
  private boolean autoCommit = true;
  private Manager manager;

  private static final String toMD5(String pwd){
    String ret = "";
    try{
      MessageDigest messageDigest = MessageDigest.getInstance("MD5");
      messageDigest.update(pwd.getBytes());
      byte[] bytes = messageDigest.digest();
      ret = new BigInteger(1,bytes).toString(16);
    }catch (Exception e){
      e.printStackTrace();
    }
    return ret;
  }
  private void getUserInfo(){
    if(users.isEmpty()){
      File file = new File(USER_INFO);
      try{
        if(file.isFile()&&file.exists()){
          FileReader fileReader = new FileReader(USER_INFO);
          BufferedReader bufferedReader = new BufferedReader(fileReader);
          String str;
          while((str = bufferedReader.readLine())!=null){
            String[] userPwds = str.split(" ");
            users.add(userPwds[0]);
            pwds.add(userPwds[1]);
          }
          bufferedReader.close();
          fileReader.close();
        }
        else{
          file.createNewFile();
        }
      }catch (IOException e){
        e.printStackTrace();
      }
    }
  }

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws RPCException,TException {
    ConnectResp resp = new ConnectResp();
    // 需要实现用户信息管理，看用户名与密码是否匹配
    String usr = req.username;
    String pwd = toMD5(req.password);
    getUserInfo(); //load the file
    if(users.contains(usr)){
      int index = users.indexOf(usr);
      if(pwds.get(index).equals(pwd)){
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        sessionIds.add(sessionCnt);  //add session to pool
        resp.setSessionId(sessionCnt);
        sessionCnt++;
        try {
          manager = new Manager();
          manager.switchDatabase("public");
        }catch (IOException e){
          e.printStackTrace();
        }
      }
      else{
        resp.setStatus(new Status(Global.FAILURE_CODE));
        resp.setSessionId(-1);
        throw new RPCException("Invalid Password.");
      }
    }
    else{
      resp.setStatus(new Status(Global.FAILURE_CODE));
      resp.setSessionId(-1);
      throw new RPCException("Invalid Username.");
    }
    return resp;
  }

  @Override
  public DisconnectResp disconnect(DisconnectReq req) throws RPCException,TException {
    DisconnectResp resp = new DisconnectResp();
    // 大致要检查传入的sessionId, 然后把它从池中移除
    long sessionId = req.sessionId;
    if(sessionIds.contains(sessionId)){
      sessionIds.remove(sessionId);
      manager.quit();
      resp.setStatus(new Status(Global.SUCCESS_CODE));
    }
    else{
      resp.setStatus(new Status(Global.FAILURE_CODE));
      throw new RPCException("Disconnect failed.");
    }
    return resp;
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws RPCException, TException {
    // TODO
    /*
    需要根据具体数据库操作实现。已经实现：创建/切换数据库，删除数据库，创建表，删除表, 显示表信息，
    显示所有数据库，显示数据库信息，插入行，删除行，更新行，单表有/无条件全部行/部分行查询
     */
    ExecuteStatementResp resp = new ExecuteStatementResp();
    long sessionId = req.sessionId;
    if(sessionIds.contains(sessionId)){
      System.out.println("parse begins");
      String statement = req.statement;
      System.out.println(statement);

      CodePointCharStream charStream = CharStreams.fromString(statement);
      //System.out.println(charStream.toString());

      SQLLexer lexer = new SQLLexer(charStream);
      CommonTokenStream tokens = new CommonTokenStream(lexer);
      SQLParser parser = new SQLParser(tokens);
      ParseTree tree = parser.parse();
      // System.out.println(tree.toStringTree(parser));
      ParseTreeWalker walker = new ParseTreeWalker();
      SQLExecListener listener = new SQLExecListener(manager, autoCommit);

      walker.walk(listener,tree);
      resp = listener.getResult();
    }
    else{
      Status status = new Status(Global.FAILURE_CODE);
      status.setMsg("Please log in first.");
      resp.setStatus(status);
    }
    return resp;
  }

  @Override
  public startTransactionResp startTransaction(startTransactionReq req) {
    startTransactionResp resp = new startTransactionResp();
    autoCommit = false;
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public commitResp commit(commitReq req) {
    commitResp resp = new commitResp();
    manager.quit();
    autoCommit = true;
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public RegisterResp registNew(RegisterReq req) throws RPCException, TException{
    RegisterResp resp = new RegisterResp();
    String usr = req.username;
    String pwd = toMD5(req.password);
    getUserInfo();
    if(users.contains(usr)){
      throw new RPCException("Duplicated username.");
    }
    else{
      try{
        FileWriter fileWriter = new FileWriter(USER_INFO, true);
        fileWriter.write(usr+' '+pwd+"\r\n");
        fileWriter.flush();
        fileWriter.close();
        users.add(usr);
        pwds.add(pwd);
        resp.setStatus(new Status(Global.SUCCESS_CODE));
      }catch (IOException e){
        throw new RPCException("Failed to write into file.");
      }
    }
    return resp;
  }

  @Override
  public WithdrawResp withdraw(WithdrawReq req) throws RPCException, TException{
    // TODO
    WithdrawResp resp = new WithdrawResp();
    String usr = req.username;
    String pwd = toMD5(req.password);
    getUserInfo();
    if(users.contains(usr)){
      int index = users.indexOf(usr);
      if(pwds.get(index).equals(pwd)){
        users.remove(index);
        pwds.remove(index);
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        try{
          FileWriter withdrawWriter = new FileWriter(USER_INFO);
          int len = users.size();
          for(int i=0;i<len;++i){
            withdrawWriter.write(users.get(i)+' '+pwds.get(i)+"\r\n");
          }
          withdrawWriter.close();
        }catch (IOException e){
          e.printStackTrace();
          throw new RPCException("Failed to write into file.");
        }
      }
      else{
        resp.setStatus(new Status(Global.FAILURE_CODE));
        throw new RPCException("Invalid Password.");
      }
    }
    else{
      resp.setStatus(new Status(Global.FAILURE_CODE));
      throw new RPCException("Invalid Username.");
    }
    return resp;
  }
}
