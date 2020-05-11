package cn.edu.thssdb.service;

import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;

import java.util.Date;

public class IServiceHandler implements IService.Iface {

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    // TODO
    ConnectResp resp = new ConnectResp();
    // 需要实现用户信息管理，看用户名与密码是否匹配
    // mock
    if(req.username.equals("animal")&&req.password.equals("114514")){
      resp.setSessionId(1919810);
      resp.setStatus(new Status(Global.SUCCESS_CODE));
    }else{
      Status status = new Status(Global.FAILURE_CODE);
      status.setMsg("BOOM!");
      resp.setStatus(status);
    }
    return resp;
  }

  @Override
  public DisconnetResp disconnect(DisconnetReq req) throws TException {
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
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    // TODO
    //需要根据具体数据库操作实现。
    return null;
  }
}
