package cn.edu.thssdb.client;

import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.utils.Global;
import javafx.util.Pair;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.XmlElementDecl;
import java.io.PrintStream;
import java.util.Scanner;

public class Client {

  private static final Logger logger = LoggerFactory.getLogger(Client.class);

  static final String HOST_ARGS = "h";
  static final String HOST_NAME = "host";

  static final String HELP_ARGS = "help";
  static final String HELP_NAME = "help";

  static final String PORT_ARGS = "p";
  static final String PORT_NAME = "port";

  private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);
  private static final Scanner SCANNER = new Scanner(System.in);

  private static TTransport transport;
  private static TProtocol protocol;
  private static IService.Client client;
  private static CommandLine commandLine;

  //session id
  //record the status of connection
  private static long sessionId = -1;


  public static void main(String[] args) {
    commandLine = parseCmd(args);
    if (commandLine.hasOption(HELP_ARGS)) {
      showHelp();
      return;
    }
    try {
      echoStarting();
      String host = commandLine.getOptionValue(HOST_ARGS, Global.DEFAULT_SERVER_HOST);
      int port = Integer.parseInt(commandLine.getOptionValue(PORT_ARGS, String.valueOf(Global.DEFAULT_SERVER_PORT)));
      transport = new TSocket(host, port);
      transport.open();
      protocol = new TBinaryProtocol(transport);
      client = new IService.Client(protocol);
      boolean open = true;
      while (true) {
        print(Global.CLI_PREFIX);
        String msg = SCANNER.nextLine();
        long startTime = System.currentTimeMillis();
        switch (msg.trim()) {
          case Global.SHOW_TIME:
            getTime();
            break;
          case Global.QUIT:
            open = false;
            break;
          case Global.RSTR:
            register();
            break;
          case Global.WDRW:
            withdraw();
            break;
          case Global.CONN:
            Pair<String, String> pair = getUsrPsw();
            connect(pair.getKey(), pair.getValue());
            break;
          case Global.DISC:
            disconnect(sessionId);
            break;
          case Global.EXEC:
            execute();
            break;
          case Global.TRANSACTION:
            transaction();
            break;
          case Global.COMMIT:
            commit();
            break;
          default:
            String command = msg.trim();
            if (isCommand(command)) {
              executeCommand(command);
            }
            else {
              println("Invalid statements!");
            }
            break;
        }
        long endTime = System.currentTimeMillis();
        println("It costs " + (endTime - startTime) + " ms.");
        if (!open) {
          break;
        }
      }
      transport.close();
    } catch (TTransportException e) {
      logger.error(e.getMessage());
    }
  }

  private static boolean isCommand(String command) {
    return command.startsWith(Global.CREATE_DATABASE)||command.startsWith(Global.DROP_DATABASE)||command.startsWith(Global.SHOW_DATABASES)||command.startsWith(Global.USE)||
            command.startsWith(Global.CREATE_TABLE)||command.startsWith(Global.DROP_TABLE)||command.startsWith(Global.SHOW_DATABASE)||command.startsWith(Global.SHOW_TABLE)||
            command.startsWith(Global.INSERT_INTO)||command.startsWith(Global.DELETE_FROM)||command.startsWith(Global.UPDATE)||command.startsWith(Global.SELECT);
  }

  private static void getTime() {
    GetTimeReq req = new GetTimeReq();
    try {
      println(client.getTime(req).getTime());
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private static void register(){
    println("Begin to register.");
    String usr;
    String pwd;
    println("Please enter your username:");
    usr = SCANNER.nextLine();
    println("Please enter your password:");
    pwd = SCANNER.nextLine();
    RegisterReq req = new RegisterReq(usr,pwd);
    try{
      RegisterResp resp = client.registNew(req);
      if(resp.status.code == Global.SUCCESS_CODE){
        println("Register success.");
      }
      else{
        println("Register failed.");
      }
    }catch (RPCException e){
      println(e.getMsg());
    }catch (TException e){
      logger.error(e.getMessage());
    }
  }

  // real connect
  private static Pair<String, String> getUsrPsw() {
    String usr, psw;
    println("Please enter your username:");
    // print(Global.CLI_PREFIX);
    usr = SCANNER.nextLine();
    println("Please enter your password:");
    // print(Global.CLI_PREFIX);
    psw = SCANNER.nextLine();
    Pair<String, String> pair = new Pair<>(usr, psw);
    return pair;
  }

  private static void connect(String username, String password){
    println("Connecting to database...");
    ConnectReq req = new ConnectReq(username, password);
    if(sessionId == -1) {
      try {
        ConnectResp resp = client.connect(req);
        if (resp.status.code == Global.SUCCESS_CODE) {
          println("Session id: " + resp.getSessionId() + "");
          sessionId = resp.getSessionId();
        } else {
          println("Connection Failed.");
        }
      } catch (RPCException e) {
        println(e.getMsg());
      } catch (TException e) {
        logger.error(e.getMessage());
      }
    }else{
      println("You have already logged in.");
    }
  }

  // real disconnect
  private static void disconnect(long SessionId){
    DisconnectReq req = new DisconnectReq(SessionId);
    if(sessionId == -1){
      println("You have not logged in yet.");
    }
    else{
      try{
        DisconnectResp resp = client.disconnect(req);
        if(resp.status.code == Global.SUCCESS_CODE){
          println("Disconnection Succeeded.");
          sessionId = -1;
        }
      }catch (RPCException e){
        println(e.getMsg());
      }catch (TException e){
        logger.error(e.getMessage());
      }
    }
  }

  public static void execute(){
    println("Please enter executable expression:");
    String expression = SCANNER.nextLine();
    ExecuteStatementReq req = new ExecuteStatementReq(sessionId, expression);
    try{
      ExecuteStatementResp resp = client.executeStatement(req);
      if(resp.status.code == Global.SUCCESS_CODE){
        println("Execution Succeeded.");
        println(resp.status.msg);
        if(resp.columnsList!=null){
          for(int i=0;i<resp.columnsList.size();i++){
            print(resp.columnsList.get(i)+" | ");
          }
          print("\n--------------------\n");
          for(int i=0;i<resp.rowList.size();i++){
            println(resp.rowList.get(i).get(0));
          }
        }
      }
      else{
        println("Execution Failed.");
        println(resp.status.msg);
      }
    }catch (RPCException e){
      println(e.getMsg());
    }catch (TException e){
      logger.error(e.getMessage());
    }
  }

  public static void transaction(){
    startTransactionReq req = new startTransactionReq();
    try {
      client.startTransaction(req);
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  public static void commit() {
    commitReq req = new commitReq();
    try {
      client.commit(req);
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  public static void executeCommand(String msg){
    ExecuteStatementReq req = new ExecuteStatementReq(sessionId, msg);
    try{
      ExecuteStatementResp resp = client.executeStatement(req);
      if(resp.status.code == Global.SUCCESS_CODE){
        println("Execution Succeeded.");
        println(resp.status.msg);
        if(resp.columnsList!=null){
          for(int i=0;i<resp.columnsList.size();i++){
            print(resp.columnsList.get(i)+" | ");
          }
          print("\n--------------------\n");
          for(int i=0;i<resp.rowList.size();i++){
            println(resp.rowList.get(i).get(0));
          }
        }
      }
      else{
        println("Execution Failed.");
        println(resp.status.msg);
      }
    }catch (RPCException e){
      println(e.getMsg());
    }catch (TException e){
      logger.error(e.getMessage());
    }
  }

  public static void withdraw(){
    String usr;
    String pwd;
    println("Begin to withdraw.");
    println("Please enter your username:");
    usr = SCANNER.nextLine();
    println("Please enter your password:");
    pwd = SCANNER.nextLine();
    WithdrawReq req = new WithdrawReq(usr,pwd);
    try{
      WithdrawResp resp = client.withdraw(req);
      if(resp.status.code == Global.SUCCESS_CODE){
        println("Withdraw succeeded.");
      }
      else{
        println("Withdraw failed.");
      }
    }catch (RPCException e){
      println(e.getMsg());
    }catch (TException e){
      logger.error(e.getMessage());
    }
  }

  static Options createOptions() {
    Options options = new Options();
    options.addOption(Option.builder(HELP_ARGS)
        .argName(HELP_NAME)
        .desc("Display help information(optional)")
        .hasArg(false)
        .required(false)
        .build()
    );
    options.addOption(Option.builder(HOST_ARGS)
        .argName(HOST_NAME)
        .desc("Host (optional, default 127.0.0.1)")
        .hasArg(false)
        .required(false)
        .build()
    );
    options.addOption(Option.builder(PORT_ARGS)
        .argName(PORT_NAME)
        .desc("Port (optional, default 6667)")
        .hasArg(false)
        .required(false)
        .build()
    );
    return options;
  }

  static CommandLine parseCmd(String[] args) {
    Options options = createOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      logger.error(e.getMessage());
      println("Invalid command line argument!");
      System.exit(-1);
    }
    return cmd;
  }

  static void showHelp() {

    println("Welcome to ThssDB written by swy, lxy & lz");
    println("To use the database system, you must register first, enter \" register;\" to register");
    println("To connect the database, enter \"connect;\"");
    println("To execute sql statements, enter \"execute\" first");
    println("Notice that if you want to use \" use \" statement, you must use it with the sql statement in one statement.");
    println("For example: use test;select * from tableTest;");
    println("To disconnect from the system, enter \"disconnect;\"");
    println("To withdraw your account, enter \"withdraw;\"");
    println("Good Luck!");
  }

  static void echoStarting() {
    println("----------------------");
    println("Starting ThssDB Client");
    println("----------------------");
  }

  static void print(String msg) {
    SCREEN_PRINTER.print(msg);
  }

  static void println() {
    SCREEN_PRINTER.println();
  }

  static void println(String msg) {
    SCREEN_PRINTER.println(msg);
  }
}
