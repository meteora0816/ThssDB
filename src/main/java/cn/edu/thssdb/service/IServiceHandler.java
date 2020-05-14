package cn.edu.thssdb.service;

import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;

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
  public DisconnetResp disconnect(DisconnetReq req) throws RPCException,TException {
    // TODO
    DisconnetResp resp = new DisconnetResp();
    // mock 大致要检查传入的sessionId, 然后把它从池中移除
    if(req.sessionId!=1919810){
      resp.setStatus(new Status(Global.FAILURE_CODE));
    }
    else{
      // 可能要从池中移除session
      resp.setStatus(new Status(Global.SUCCESS_CODE));
    }
    return resp;
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws RPCException, TException {
    // TODO
    //需要根据具体数据库操作实现。
    return null;
  }

  @Override
  public RegisterResp registNew(RegisterReq req) throws RPCException, TException{
    // TODO
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
  public WithdrawResp withdraw(WithdrawReq rep) throws RPCException, TException{
    // TODO
    return null;
  }
}
