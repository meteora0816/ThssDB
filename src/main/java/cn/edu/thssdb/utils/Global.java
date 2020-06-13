package cn.edu.thssdb.utils;

public class Global {
  public static int fanout = 129;

  public static int SUCCESS_CODE = 0;
  public static int FAILURE_CODE = -1;

  public static String DEFAULT_SERVER_HOST = "127.0.0.1";
  public static int DEFAULT_SERVER_PORT = 6667;

  public static String CLI_PREFIX = "ThssDB>";
  public static final String SHOW_TIME = "show time;";
  public static final String QUIT = "quit;";
  public static final String CONN = "connect;";
  public static final String DISC = "disconnect;";
  public static final String RSTR = "register;";
  public static final String EXEC = "execute;";
  public static final String WDRW = "withdraw;";
  public static final String TRANSACTION = "begin transaction;";
  public static final String COMMIT = "commit;";

  public static final String CREATE_DATABASE = "create database";
  public static final String DROP_DATABASE = "drop database";
  public static final String SHOW_DATABASES = "show databases";
  public static final String USE = "use";
  public static final String CREATE_TABLE = "create table";
  public static final String DROP_TABLE = "drop table";
  public static final String SHOW_DATABASE = "show database";
  public static final String SHOW_TABLE = "show table";
  public static final String INSERT_INTO = "insert into";
  public static final String DELETE_FROM = "delete from";
  public static final String UPDATE = "update";
  public static final String SELECT = "select";

  public static final String S_URL_INTERNAL = "jdbc:default:connection";
}
