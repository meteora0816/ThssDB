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

  public static final String CREATE_DATABASE = "[Cc][Re][Ee][Aa][Tt][Ee](\\s)+[Dd][Aa][Tt][Aa][Bb][Aa][Ss][Ee].*";
  public static final String DROP_DATABASE = "[Dd][Rr][Oo][Pp](\\s)+[Dd][Aa][Tt][Aa][Bb][Aa][Ss][Ee].*";
  public static final String SHOW_DATABASES = "[Ss][Hh][Oo][Ww](\\s)+[Dd][Aa][Tt][Aa][Bb][Aa][Ss][Ee][Ss].*";
  public static final String USE = "[Uu][Ss][Ee].*";
  public static final String CREATE_TABLE = "[Cc][Rr][Ee][Aa][Tt][Ee](\\s)+[Tt][Aa][Bb][Ll][Ee].*";
  public static final String DROP_TABLE = "[Dd][Rr][Oo][Pp](\\s)+[Tt][Aa][Bb][Ll][Ee].*";
  public static final String SHOW_DATABASE = "[Ss][Hh][Oo][Ww](\\s)+[Dd][Aa][Tt][Aa][Bb][Aa][Ss][Ee].*";
  public static final String SHOW_TABLE = "[Ss][Hh][Oo][Ww](\\s)+[Tt][Aa][Bb][Ll][Ee].*";
  public static final String INSERT_INTO = "[Ii][Nn][Ss][Ee][Rr][Tt](\\s)+[Ii][Nn][Tt][Oo].*";
  public static final String DELETE_FROM = "[Dd][Ee][Ll][Ee][Tt][Ee](\\s)+[Ff][Rr][Oo][Mm].*";
  public static final String UPDATE = "[Uu][Pp][Dd][Aa][Tt][Ee].*";
  public static final String SELECT = "[Ss][Ee][Ll][Ee][Cc][Tt].*";

  public static final String S_URL_INTERNAL = "jdbc:default:connection";
}
