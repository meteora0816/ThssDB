namespace java cn.edu.thssdb.rpc.thrift

struct Status {
  1: required i32 code;
  2: optional string msg;
}

struct GetTimeReq {
}

struct RegisterReq{
  1: required string username
  2: required string password
}

struct RegisterResp{
  1: required Status status;
}

struct WithdrawReq{
  1: required string username
  2: required string password
}

struct WithdrawResp{
  1: required Status status
}

struct ConnectReq{
  1: required string username
  2: required string password
}

struct ConnectResp{
  1: required Status status
  2: required i64 sessionId
}

struct DisconnectReq{
  1: required i64 sessionId
}

struct DisconnectResp{
  1: required Status status
}

struct GetTimeResp {
  1: required string time
  2: required Status status
}

struct ExecuteStatementReq {
  1: required i64 sessionId
  2: required string statement
}

struct ExecuteStatementResp{
  1: required Status status
  2: required bool isAbort
  3: required bool hasResult
  // only for query
  4: optional list<string> columnsList
  5: optional list<list<string>> rowList
}

exception RPCException{
  1: required string msg;
}

service IService {
  GetTimeResp getTime(1: GetTimeReq req);
  RegisterResp registNew(1: RegisterReq req)throws(1:RPCException e);
  WithdrawResp withdraw(1: WithdrawReq req)throws(1:RPCException e);
  ConnectResp connect(1: ConnectReq req)throws(1:RPCException e);
  // DisconnetResp disconnect(1: DisconnetResp req);
  DisconnectResp disconnect(1:DisconnectReq req)throws(1:RPCException e);
  ExecuteStatementResp executeStatement(1: ExecuteStatementReq req)throws(1:RPCException e);
}
