package cn.edu.thssdb.schema;

import cn.edu.thssdb.server.ThssDB;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
    private HashMap<String, Database> databases;
    private Database currentDB;
    private String baseDir = "data";
    private String metaFile = "DB.meta";
    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public static Manager getInstance() {
        return Manager.ManagerHolder.INSTANCE;
    }

    public Manager() throws IOException {
        databases = new HashMap<>();

        File DBDir = new File(baseDir);
        File DBmeta = new File(baseDir+"/"+metaFile);
        if (!DBDir.exists()) {
            DBDir.mkdir();
            DBmeta.createNewFile();
        }
        else {
            recover();
        }
    }

    private Database createDatabaseIfNotExists(String name) throws IOException {
        Database newDB = new Database(baseDir, name);
        databases.put(name, newDB);
        return newDB;
    }

    public boolean containDatabase(String dbName){
        return databases.containsKey(dbName);
    }

    public void deleteDatabase(String name) {
        System.out.println("delete database: " + name);
        // 删除数据库
        if (databases.containsKey(name)) {
            Database delDB = databases.get(name);
            delDB.drop();
            databases.remove(name);
        }
        else {
            System.out.println("database " + name + " doesn't exist.");
        }
    }

    public void switchDatabase(String name) throws IOException {
        // 切换数据库，如果没有就新建
        if (currentDB != null) {
            currentDB.quit();
        }
        if (databases.containsKey(name)) {
            currentDB = databases.get(name);
        }
        else {
            currentDB = createDatabaseIfNotExists(name);
            databases.put(name, currentDB);
        }
    }

    public Database getCurrentDB() {
        return currentDB;
    }

    public void quit() {
        for (String key : this.databases.keySet()) {
            this.databases.get(key).quit();
        }
        // 存储元数据（有哪些数据库）
    /* DB.meta:
       Database Num|Database1|Database2|... */
        try {
            OutputStream fop = new FileOutputStream(this.baseDir + "/" + metaFile);
            OutputStreamWriter writer = new OutputStreamWriter(fop, "UTF-8");
            writer.append(String.valueOf(databases.size()));
            writer.append("|");
            for (String key : databases.keySet()) {
                writer.append(key).append("|");
            }
            writer.close();
            fop.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void recover() {
        System.out.println("Recover all databases...");
        try {
            File file = new File(this.baseDir + "/" + this.metaFile);
            FileReader reader = new FileReader(file);
            char[] buf = new char[1024];
            reader.read(buf);
            reader.close();
            String[] vals = String.valueOf(buf).split("\\|");
            System.out.println(vals[0] + " databases(s)");
            int databaseNum = Integer.parseInt(vals[0]);
            for (int i=0;i<databaseNum;i++) {
                String databaseName = vals[i+1];
                Database database = new Database(this.baseDir, databaseName);
                databases.put(databaseName, database);
            }
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    public String show(){
        StringBuilder ret = new StringBuilder();
        ret.append(this.databases.size()).append(" database(s):\n");
        for(String dbName:this.databases.keySet()){
            ret.append(dbName).append("\n");
        }
        return ret.toString();
    }

    private static class ManagerHolder {
        private static Manager INSTANCE;

        static {
            try {
                INSTANCE = new Manager();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private ManagerHolder() {

        }
    }
}
