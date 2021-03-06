package cn.edu.thssdb.parser;

import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.NDException;
import com.sun.org.apache.bcel.internal.generic.GotoInstruction;


import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLExecListener extends SQLBaseListener {
    private Manager manager;
    private ExecuteStatementResp resp = new ExecuteStatementResp();
    private Status status = new Status();
    private boolean success = true;
    private boolean autoCommit;
    private String command;
    private int tnum; // transaction

    public SQLExecListener(Manager mng, boolean ac, String cmd, int tn) {
        super();
        manager = mng;
        if(ac) manager.recover();
        autoCommit = ac;
        command = cmd;
        tnum = tn;
    }

    @Override
    public void enterParse(SQLParser.ParseContext ctx){
        status.setMsg("\n");
//        try {
//            manager = new Manager();
//            manager.switchDatabase("public");
//        }catch (IOException e){
//            e.printStackTrace();
//        }
    }

    @Override
    public void exitShow_db_stmt(SQLParser.Show_db_stmtContext ctx) {
        status.msg+=manager.show();
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
    public void exitShow_table_stmt(SQLParser.Show_table_stmtContext ctx) {
        String dbName = ctx.database_name().getText();
        try {
            manager.switchDatabase(dbName);
            status.msg+=manager.getCurrentDB().show();
        }catch(IOException e){
            success = false;
            status.msg+="Fail to load required database.";
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
            StringBuilder typeLength = new StringBuilder("32");
            if(typeRaw.charAt(0)=='S'){
                typeLength = new StringBuilder();
                for(int j=7;j<typeRaw.length()-1;j++){
                    typeLength.append(typeRaw.charAt(j));
                }
                typeRaw = "STRING";
            }
            int maxLength = Integer.parseInt(typeLength.toString());
            ColumnType columnType = ColumnType.valueOf(typeRaw);
            boolean notNull = false;
            List<SQLParser.Column_constraintContext> column_constraintContexts = column_defContext.column_constraint();
            if(!column_constraintContexts.isEmpty()) {
                String columnConstraint = column_constraintContexts.get(0).getText();//只实现not null
                //System.out.println("columnConstraint: "+columnConstraint);
                if (columnConstraint.toUpperCase().equals("NOTNULL")) {
                    notNull = true;
                }
            }
            columns[i] = new Column(columnName,columnType,0,notNull,maxLength);
        }
        List<SQLParser.Column_nameContext> table_constraintContexts = ctx.table_constraint().column_name();
        //System.out.println(table_constraintContexts.size());
        if(!table_constraintContexts.isEmpty()) {
            //String primaryName = ctx.table_constraint().column_name(0).getText();//only one primary key
            //System.out.println(primaryName);
            List<SQLParser.Column_nameContext> column_nameContexts = ctx.table_constraint().column_name();
            ArrayList<String> primaryNames = new ArrayList<>();
            int numOfPrimary = column_nameContexts.size();
            for (SQLParser.Column_nameContext column_nameContext : column_nameContexts) {
                primaryNames.add(column_nameContext.getText());
            }
            System.out.println(Arrays.toString(primaryNames.toArray()));
            for (int i = 0; i < numOfColumns; i++) {
                //System.out.println(columns[i].name());
                if (primaryNames.contains(columns[i].name())) {
                    columns[i].setPrimary(1);
                    break;
                }
            }
        }
        try {
            Database db = manager.getCurrentDB();
            if(db.containsTable(tableName)){
                success = false;
                status.msg += "Duplicated tableName.\n";
            }
            else {
                db.create(tableName, columns);
                status.msg += "Create table successfully.\n";
            }
        }catch (Exception e){
            success = false;
            status.msg+="Failed to create table.";
        }
    }

    @Override
    public void exitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        String tableName = ctx.table_name().getText();
        manager.getCurrentDB().dropTable(tableName);
        status.msg+="Drop table successfully.";
    }

    //show table
    @Override
    public void exitShow_meta_stmt(SQLParser.Show_meta_stmtContext ctx) {
        String tableName = ctx.table_name().getText();
        status.msg+="Table: "+tableName+"\n";
        Table table = manager.getCurrentDB().getTable(tableName);
        List<Column> columns = table.columns;
        int len = columns.size();
        for (Column column : columns) {
            status.msg += column.toString();
        }
        status.msg+="Number of Rows: "+table.index.size()+"\n";
    }

    @Override
    public void exitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        String tableName = ctx.table_name().getText();
        if(!manager.getCurrentDB().containsTable(tableName)){
            success = false;

            status.msg+="Table "+tableName+" does not exit";
            return;
        }
        List<SQLParser.Column_nameContext> column_nameContexts = ctx.column_name();
        int numOfColumn = column_nameContexts.size();
        String[] columnNames = new String[numOfColumn];
        for(int i=0;i<numOfColumn;i++){
            columnNames[i] = column_nameContexts.get(i).getText();
        }
        System.out.println("columnNum: "+numOfColumn);
        //List<SQLParser.Value_entryContext> value_entryContexts = ctx.value_entry();
        String rawEntryValue = ctx.value_entry(0).getText();
        // 去空格
        rawEntryValue = rawEntryValue.trim();
        // 去括号
        StringBuilder rawWithoutBrace = new StringBuilder();
        for(int i=1;i<rawEntryValue.length()-1;i++){
            rawWithoutBrace.append(rawEntryValue.charAt(i));
        }
        System.out.println(rawWithoutBrace);
        String[] entryValues = rawWithoutBrace.toString().split(",");
        int numOfEntries = entryValues.length;
        System.out.println("entryNum: "+numOfEntries);
        Table currentTable = manager.getCurrentDB().getTable(tableName);

        Entry[] entries = new Entry[numOfEntries];
        //System.out.println(Arrays.toString(entries));
        for(int i=0;i<numOfEntries;i++){
            entries[i] = new Entry(entryValues[i]);
        }
        System.out.println(Arrays.toString(entries));
        Row insertRow;

        if(numOfColumn == 0){
            // 默认输入，entries不调整
            insertRow = new Row(entries);
        }
        else{
            int numOfRealColumns = currentTable.columns.size();
            Entry[] realEntries = new Entry[numOfRealColumns];
            for(int i=0;i<numOfRealColumns;i++){
                realEntries[i] = new Entry(null);
            }
            for(int i=0;i<numOfColumn;i++){ //check every column
                int index;
                for(index=0;index<numOfRealColumns;index++){
                    if(currentTable.columns.get(index).name().equals(columnNames[i])){
                        break;
                    }
                }
                realEntries[index] = new Entry(entries[i]);
            }
            insertRow = new Row(realEntries);
        }

        try {
            currentTable.insert(insertRow);
        }catch (NullPointerException | NDException e){
            success = false;
            status.msg += "Some of your insert values cannot be null.";
        }catch (DuplicateKeyException e){
            success = false;
            status.msg += "Record already exists.";
        }
    }

    @Override
    public void exitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
        String tableName = ctx.table_name().getText();
        if(!manager.getCurrentDB().containsTable(tableName)){
            success = false;

            status.msg+="Table "+tableName+" does not exit";
            return;
        }
        Table currentTable = manager.getCurrentDB().getTable(tableName);
        String comparator = ctx.multiple_condition().condition().comparator().getText();
        System.out.println(comparator);
        String attrName = ctx.multiple_condition().condition().expression(0).getText();
        String attrValue = ctx.multiple_condition().condition().expression(1).getText();
        System.out.println(attrName);
        System.out.println(attrValue);
        //由于表的delete方法的参数是主键，所以首先找到所有被删除的行的主键
        ArrayList<Entry> deleteEntries = new ArrayList<>(); //被删除的行的主键
        //找到传入的语句中的attrName是第几列
        int attrNameIndex = 0;
        ArrayList<Column> currentColumns = currentTable.columns;
        for(int i=0;i<currentTable.columnNumber();i++){
            System.out.println(currentColumns.get(i).name());
            if(currentColumns.get(i).name().equals(attrName)){
                attrNameIndex = i;
                break;
            }
        }
        int primaryIndex = currentTable.primaryIndex();
        Entry attrValueEntry = new Entry(attrValue);
        Iterator<Row> iterator = currentTable.iterator();
        switch (comparator){
            case "=":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    //System.out.println(currentRow);
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)==0){
                        deleteEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            case "<":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)<0){
                        deleteEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            case ">":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)>0){
                        deleteEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            case "<=":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)<=0){
                        deleteEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            case ">=":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)>=0){
                        deleteEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            case "<>":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)!=0){
                        deleteEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            default:
                break;
        }
        for (Entry deleteEntry : deleteEntries) {
            currentTable.delete(deleteEntry);
        }
        status.msg+="Delete Successfully.\n";
    }

    @Override
    public void exitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
        // 更新哪个表
        String tableName = ctx.table_name().getText();
        if(!manager.getCurrentDB().containsTable(tableName)){
            success = false;

            status.msg+="Table "+tableName+" does not exit";
            return;
        }
        System.out.println(tableName);
        // 更新哪一列
        String attrToBeUpdated = ctx.column_name().getText();
        System.out.println(attrToBeUpdated);
        // 更新为何值
        String valueTobeUpdated = ctx.expression().getText();
        System.out.println(valueTobeUpdated);
        Table currentTable = manager.getCurrentDB().getTable(tableName);
        // 更新哪一行，找到主键
        String comparator = ctx.multiple_condition().condition().comparator().getText();
        String attrName = ctx.multiple_condition().condition().expression(0).getText();
        String attrValue = ctx.multiple_condition().condition().expression(1).getText();
        // 条件中的attrName是第几列
        int attrNameIndex = 0;
        ArrayList<Column> currentColumns = currentTable.columns;
        for(int i=0;i<currentTable.columnNumber();i++){
            if(currentColumns.get(i).name().equals(attrName)){
                attrNameIndex = i;
                break;
            }
        }
        // 更新中的attrToBeUpdated是第几列
        int attrToBeUpdatedIndex = 0;
        for(int i=0;i<currentTable.columnNumber();i++){
            if(currentColumns.get(i).name().equals(attrToBeUpdated)){
                attrToBeUpdatedIndex = i;
                break;
            }
        }
        int primaryIndex = currentTable.primaryIndex();
        Entry attrValueEntry = new Entry(attrValue);
        ArrayList<Entry> updateEntries = new ArrayList<>(); //被更新的行的主键
        Iterator<Row> iterator = currentTable.iterator();
        switch (comparator){
            case "=":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    System.out.println(currentRow);
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)==0){
                        updateEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            case "<":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    System.out.println(currentRow);
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)<0){
                        updateEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            case ">":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    System.out.println(currentRow);
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)>0){
                        updateEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            case "<=":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    System.out.println(currentRow);
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)<=0){
                        updateEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            case ">=":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    System.out.println(currentRow);
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)>=0){
                        updateEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            case "<>":
                while(iterator.hasNext()){
                    Row currentRow = iterator.next();
                    System.out.println(currentRow);
                    if(currentRow.getEntries().get(attrNameIndex).compareTo(attrValueEntry)!=0){
                        updateEntries.add(currentRow.getEntries().get(primaryIndex));
                    }
                }
                break;
            default:
                break;
        }
        for (Entry updateEntry : updateEntries) {
            System.out.println(updateEntry);
            Row updateRow = currentTable.getRow(updateEntry);
            System.out.println(updateRow);
            ArrayList<Entry> updateRowEntries = updateRow.getEntries();
            Entry[] newRowEntries = new Entry[updateRowEntries.size()];
            //ArrayList<Entry> newRowEntries = new ArrayList<>();
            for (int j = 0; j < updateRowEntries.size(); j++) {
                if (j == attrToBeUpdatedIndex) {
                    newRowEntries[j] = new Entry(valueTobeUpdated);
                } else {
                    newRowEntries[j] = updateRowEntries.get(j);
                }
            }
            //updateRowEntries.get(attrToBeUpdatedIndex) = new Entry(valueTobeUpdated);
            Row newRow = new Row(newRowEntries);
            currentTable.update(updateEntry, newRow);
        }
        status.msg+="Update Successfully.\n";
    }

    @Override
    public void exitSelect_stmt(SQLParser.Select_stmtContext ctx) {
        // manager.quit();
        // manager.recover();
        ArrayList<String> resultTables = new ArrayList<>();  // 点前面的
        ArrayList<String> resultColumns = new ArrayList<>(); // 点后面的
        List<SQLParser.Result_columnContext> result_columnContexts = ctx.result_column();
        //System.out.println(result_columnContexts.isEmpty());
        //System.out.println(result_columnContexts.get(0).getText());
        // 先解析选择哪些列
        boolean selectAll = false;
        if(result_columnContexts.get(0).getText().equals("*")){ //全选
            selectAll = true;
        }
        else {
            for (SQLParser.Result_columnContext result_columnContext : result_columnContexts) {
                if (result_columnContext.table_name() != null) {
                    resultTables.add(result_columnContext.table_name().getText());
                    //System.out.println("table name: "+result_columnContexts.get(i).table_name().getText());
                    resultColumns.add("*");
                } else {
                    //resultTables.add(result_columnContexts.get(i).column_full_name().table_name());
                    if (result_columnContext.column_full_name().table_name() == null) {// * . attrName
                        resultTables.add("*");
                        //System.out.println("result tables: *");
                    } else {
                        resultTables.add(result_columnContext.column_full_name().table_name().getText());
                        //System.out.println(result_columnContexts.get(i).column_full_name().table_name().getText());
                    }
                    resultColumns.add(result_columnContext.column_full_name().column_name().getText());
                    //System.out.println(result_columnContexts.get(i).column_full_name().column_name().getText());
                }
            }
        }
        // System.out.println(Arrays.toString(resultColumns.toArray()));
        // 再解析从哪些表中选择
        boolean isSingleTable = false; //单表查询
        String leftTableName = "";
        String rightTableName = "";
        String leftTableAttrName = "";
        String rightTableAttrName = "";
        if(ctx.table_query(0).table_name().size()==1){
            isSingleTable = true;
            leftTableName = ctx.table_query(0).table_name(0).getText();
        }
        else{
            leftTableName = ctx.table_query(0).table_name(0).getText();
            rightTableName = ctx.table_query(0).table_name(1).getText();
            // 解析on tableName1.attrName1 = tableName2.attrName2
            // 只做这一个功能了，乞丐版就乞丐版吧
            leftTableAttrName = ctx.table_query(0)
                    .multiple_condition()
                    .condition()
                    .expression(0)
                    .comparer()
                    .column_full_name()
                    .column_name()
                    .getText();
            rightTableAttrName = ctx.table_query(0)
                    .multiple_condition()
                    .condition()
                    .expression(1)
                    .comparer()
                    .column_full_name()
                    .column_name()
                    .getText();
        }
        //System.out.println("leftAttr: "+leftTableAttrName);
        //System.out.println("rightAttr: "+rightTableAttrName);
        // where attrName = attrValue 也是乞丐版
        String whereAttrName = null;
        String whereComparator = null;
        String whereAttrValue = null;
        if(ctx.multiple_condition()!=null) {
            whereAttrName = ctx.multiple_condition()
                    .condition()
                    .expression(0)
                    .comparer()
                    .column_full_name()
                    .column_name()
                    .getText();
            whereComparator = ctx.multiple_condition()
                    .condition()
                    .comparator()
                    .getText();
            whereAttrValue = ctx.multiple_condition()
                    .condition()
                    .expression(1)
                    .comparer()
                    .literal_value()
                    .getText();
        }
        //System.out.println("attrName: "+whereAttrName);
        //System.out.println("attrValue: "+whereAttrValue);
        //System.out.println("operator: "+whereComparator);
        if(isSingleTable){
            // 单表查询
            if(!manager.getCurrentDB().containsTable(leftTableName)){
                success = false;

                status.msg+="Table "+leftTableName+" does not exit";
                return;
            }
            Table currentTable = manager.getCurrentDB().getTable(leftTableName);
            ArrayList<Column> columns = currentTable.columns;
            if(selectAll){
                //全选
                resp.columnsList = new ArrayList<>();
                resp.rowList = new ArrayList<>();
                for (Column column : columns) {
                    resp.columnsList.add(column.name());
                }
                if(whereAttrName==null){
                    //没有选择条件
                    for (Row currentRow : currentTable) {
                        ArrayList<String> tmpRow = new ArrayList<>();
                        tmpRow.add(currentRow.toString());
                        resp.rowList.add(tmpRow);
                    }
                }
                else {
                    //whereAttrName ><= whereAttrValue
                    int attrIndex = 0;
                    for (int i = 0; i < columns.size(); i++) {
                        if (columns.get(i).name().equals(whereAttrName)) {
                            attrIndex = i;
                            break;
                        }
                    }
                    switch (whereComparator) {
                        case "=":
                            for (Row currentRow : currentTable) {
                                if (currentRow.getEntries().get(attrIndex).compareTo(new Entry(whereAttrValue)) == 0) {
                                    ArrayList<String> tmpRow = new ArrayList<>();
                                    tmpRow.add(currentRow.toString());
                                    resp.rowList.add(tmpRow);
                                }
                            }
                            break;
                        case "<":
                            for (Row currentRow : currentTable) {
                                if (currentRow.getEntries().get(attrIndex).compareTo(new Entry(whereAttrValue)) < 0) {
                                    ArrayList<String> tmpRow = new ArrayList<>();
                                    tmpRow.add(currentRow.toString());
                                    resp.rowList.add(tmpRow);
                                }
                            }
                            break;
                        case ">":
                            for (Row currentRow : currentTable) {
                                if (currentRow.getEntries().get(attrIndex).compareTo(new Entry(whereAttrValue)) > 0) {
                                    ArrayList<String> tmpRow = new ArrayList<>();
                                    tmpRow.add(currentRow.toString());
                                    resp.rowList.add(tmpRow);
                                }
                            }
                            break;
                        case "<=":
                            for (Row currentRow : currentTable) {
                                if (currentRow.getEntries().get(attrIndex).compareTo(new Entry(whereAttrValue)) <= 0) {
                                    ArrayList<String> tmpRow = new ArrayList<>();
                                    tmpRow.add(currentRow.toString());
                                    resp.rowList.add(tmpRow);
                                }
                            }
                            break;
                        case ">=":
                            for (Row currentRow : currentTable) {
                                if (currentRow.getEntries().get(attrIndex).compareTo(new Entry(whereAttrValue)) >= 0) {
                                    ArrayList<String> tmpRow = new ArrayList<>();
                                    tmpRow.add(currentRow.toString());
                                    resp.rowList.add(tmpRow);
                                }
                            }
                            break;
                        case "<>":
                            for (Row currentRow : currentTable) {
                                if (currentRow.getEntries().get(attrIndex).compareTo(new Entry(whereAttrValue)) != 0) {
                                    ArrayList<String> tmpRow = new ArrayList<>();
                                    tmpRow.add(currentRow.toString());
                                    resp.rowList.add(tmpRow);
                                }
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
            else{
                // 选择某几列
                //System.out.println(Arrays.toString(resultColumns.toArray()));
                resp.columnsList = new ArrayList<>();
                resp.rowList = new ArrayList<>();
                ArrayList<Integer> attrIndices = new ArrayList<>();
                //resp.columnsList.addAll(resultColumns);
                System.out.println(Arrays.toString(columns.toArray()));
                for(int i=0;i<columns.size();i++){
                    if(resultColumns.contains(columns.get(i).name())){
                        attrIndices.add(i);
                    }
                }
                // System.out.println(Arrays.toString(attrIndices.toArray()));
                //这一步不多余，否则输出的表可能表头与内容不匹配
                for(int i=0;i<attrIndices.size();i++){
                    resp.columnsList.add(columns.get(attrIndices.get(i)).name());
                }
                if(whereAttrName==null){
                    //没有选择条件
                    for (Row currentRow : currentTable) {
                        ArrayList<String> tmpRow = new ArrayList<>();
                        StringBuilder partRow = new StringBuilder();
                        for (int i = 0; i < attrIndices.size() - 1; i++) {
                            int index = attrIndices.get(i);
                            partRow.append(currentRow.getEntries().get(index).toString()).append(", ");
                        }
                        partRow.append(currentRow.getEntries().get(attrIndices.size() - 1).toString());
                        tmpRow.add(partRow.toString());
                        resp.rowList.add(tmpRow);
                    }
                }
                else{
                    //有选择条件
                    int attrIndex = 0;
                    for (int i = 0; i < columns.size(); i++) {
                        if (columns.get(i).name().equals(whereAttrName)) {
                            attrIndex = i;
                            break;
                        }
                    }
                    switch (whereComparator){
                        case "=":
                            for (Row currentRow : currentTable) {
                                if (currentRow.getEntries().get(attrIndex).compareTo(new Entry(whereAttrValue)) == 0) {
                                    ArrayList<String> tmpRow = new ArrayList<>();
                                    StringBuilder partRow = new StringBuilder();
                                    for (int i = 0; i < attrIndices.size() - 1; i++) {
                                        int index = attrIndices.get(i);
                                        System.out.println(index);
                                        System.out.println(Arrays.toString(currentRow.getEntries().toArray()));
                                        partRow.append(currentRow.getEntries().get(index).toString()).append(", ");
                                    }
                                    partRow.append(currentRow.getEntries().get(attrIndices.get(attrIndices.size()-1)).toString());
                                    tmpRow.add(partRow.toString());
                                    resp.rowList.add(tmpRow);
                                }
                            }
                            break;
                        case "<":
                            for (Row currentRow : currentTable) {
                                if (currentRow.getEntries().get(attrIndex).compareTo(new Entry(whereAttrValue)) < 0) {
                                    ArrayList<String> tmpRow = new ArrayList<>();
                                    StringBuilder partRow = new StringBuilder();
                                    for (int i = 0; i < attrIndices.size() - 1; i++) {
                                        int index = attrIndices.get(i);
                                        partRow.append(currentRow.getEntries().get(index).toString()).append(", ");
                                    }
                                    partRow.append(currentRow.getEntries().get(attrIndices.get(attrIndices.size()-1)).toString());
                                    tmpRow.add(partRow.toString());
                                    resp.rowList.add(tmpRow);
                                }
                            }
                            break;
                        case ">":
                            for (Row currentRow : currentTable) {
                                if (currentRow.getEntries().get(attrIndex).compareTo(new Entry(whereAttrValue)) > 0) {
                                    ArrayList<String> tmpRow = new ArrayList<>();
                                    StringBuilder partRow = new StringBuilder();
                                    for (int i = 0; i < attrIndices.size() - 1; i++) {
                                        int index = attrIndices.get(i);
                                        partRow.append(currentRow.getEntries().get(index).toString()).append(", ");
                                    }
                                    partRow.append(currentRow.getEntries().get(attrIndices.get(attrIndices.size()-1)).toString());
                                    tmpRow.add(partRow.toString());
                                    resp.rowList.add(tmpRow);
                                }
                            }
                            break;
                        case "<=":
                            for (Row currentRow : currentTable) {
                                if (currentRow.getEntries().get(attrIndex).compareTo(new Entry(whereAttrValue)) <= 0) {
                                    ArrayList<String> tmpRow = new ArrayList<>();
                                    StringBuilder partRow = new StringBuilder();
                                    for (int i = 0; i < attrIndices.size() - 1; i++) {
                                        int index = attrIndices.get(i);
                                        partRow.append(currentRow.getEntries().get(index).toString()).append(", ");
                                    }
                                    partRow.append(currentRow.getEntries().get(attrIndices.get(attrIndices.size()-1)).toString());
                                    tmpRow.add(partRow.toString());
                                    resp.rowList.add(tmpRow);
                                }
                            }
                            break;
                        case ">=":
                            for (Row currentRow : currentTable) {
                                if (currentRow.getEntries().get(attrIndex).compareTo(new Entry(whereAttrValue)) >= 0) {
                                    ArrayList<String> tmpRow = new ArrayList<>();
                                    StringBuilder partRow = new StringBuilder();
                                    for (int i = 0; i < attrIndices.size() - 1; i++) {
                                        int index = attrIndices.get(i);
                                        partRow.append(currentRow.getEntries().get(index).toString()).append(", ");
                                    }
                                    partRow.append(currentRow.getEntries().get(attrIndices.get(attrIndices.size()-1)).toString());
                                    tmpRow.add(partRow.toString());
                                    resp.rowList.add(tmpRow);
                                }
                            }
                            break;
                        case "<>":
                            for (Row currentRow : currentTable) {
                                if (currentRow.getEntries().get(attrIndex).compareTo(new Entry(whereAttrValue)) != 0) {
                                    ArrayList<String> tmpRow = new ArrayList<>();
                                    StringBuilder partRow = new StringBuilder();
                                    for (int i = 0; i < attrIndices.size() - 1; i++) {
                                        int index = attrIndices.get(i);
                                        partRow.append(currentRow.getEntries().get(index).toString()).append(", ");
                                    }
                                    partRow.append(currentRow.getEntries().get(attrIndices.get(attrIndices.size()-1)).toString());
                                    tmpRow.add(partRow.toString());
                                    resp.rowList.add(tmpRow);
                                }
                            }
                            break;
                        default:
                            break;
                    }

                }
            }

        }
        else{
            //多表查询
            //先执行join
            if(!manager.getCurrentDB().containsTable(leftTableName)){
                success = false;
                status.msg+="Table "+leftTableName+" does not exit";
                return;
            }
            if(!manager.getCurrentDB().containsTable(rightTableName)){
                success = false;
                status.msg+="Table "+rightTableName+" does not exit";
                return;
            }
            Table leftTable = manager.getCurrentDB().getTable(leftTableName);
            Table rightTable = manager.getCurrentDB().getTable(rightTableName);
            ArrayList<Column> leftColumns = leftTable.columns;
            ArrayList<Column> rightColumns = rightTable.columns;
            int leftOnIndex = 0;
            int rightOnIndex = 0;
            for(int i=0;i<leftColumns.size();i++){
                if(leftColumns.get(i).name().equals(leftTableAttrName)){
                    leftOnIndex = i;
                    break;
                }
            }
            for(int i=0;i<rightColumns.size();i++){
                if(rightColumns.get(i).name().equals(rightTableAttrName)){
                    rightOnIndex = i;
                    break;
                }
            }
            ArrayList<ArrayList<Row>> joinedTable = new ArrayList<>();
            for (Row currentLeftRow : leftTable) {
                Entry leftEntry = currentLeftRow.getEntries().get(leftOnIndex);
                for (Row currentRightRow : rightTable) {
                    Entry rightEntry = currentRightRow.getEntries().get(rightOnIndex);
                    if (leftEntry.compareTo(rightEntry) == 0) {
                        // 满足条件，两行join
                        ArrayList<Row> joinedRow = new ArrayList<>();
                        joinedRow.add(currentLeftRow);
                        joinedRow.add(currentRightRow);
                        joinedTable.add(joinedRow);
                    }
                }
            }
            if(selectAll){
                // 全选
                resp.columnsList = new ArrayList<>();
                resp.rowList = new ArrayList<>();
                for (Column leftColumn : leftColumns) {
                    resp.columnsList.add("L." + leftColumn.name());
                }
                for (Column rightColumn : rightColumns) {
                    resp.columnsList.add("R." + rightColumn.name());
                }
                if(whereAttrName == null){
                    //没有选择条件
                    for (ArrayList<Row> rows : joinedTable) {
                        ArrayList<String> tmpRow = new ArrayList<>();
                        tmpRow.add(rows.get(0).toString() + ", " + rows.get(1).toString());
                        resp.rowList.add(tmpRow);
                    }
                }
                else{
                    //有选择条件
                    //如果属性名重名，优先捕捉leftTable中的
                    boolean inLeft = false;
                    int whereAttrIndex = 0;
                    for(int i=0;i<leftColumns.size();i++){
                        if(leftColumns.get(i).name().equals(whereAttrName)){
                            inLeft = true;
                            whereAttrIndex = i;
                            break;
                        }
                    }
                    if(!inLeft){
                        for(int i=0;i<rightColumns.size();i++){
                            if(rightColumns.get(i).name().equals(whereAttrName)){
                                whereAttrIndex = i;
                                break;
                            }
                        }
                    }
                    Entry whereAttrValueEntry = new Entry(whereAttrValue);
                    switch (whereComparator){
                        case "=":
                            for (ArrayList<Row> rows : joinedTable) {
                                if (inLeft) {
                                    if (rows
                                            .get(0)
                                            .getEntries()
                                            .get(whereAttrIndex)
                                            .compareTo(whereAttrValueEntry) == 0) {
                                        ArrayList<String> tmpRow = new ArrayList<>();
                                        tmpRow.add(rows.get(0).toString() + ", "
                                                + rows.get(1).toString());
                                        resp.rowList.add(tmpRow);
                                    }
                                } else {
                                    if (rows
                                            .get(1)
                                            .getEntries()
                                            .get(whereAttrIndex)
                                            .compareTo(whereAttrValueEntry) == 0) {
                                        ArrayList<String> tmpRow = new ArrayList<>();
                                        tmpRow.add(rows.get(0).toString() + ", "
                                                + rows.get(1).toString());
                                        resp.rowList.add(tmpRow);
                                    }
                                }
                            }
                            break;
                        case "<":
                            for (ArrayList<Row> rows : joinedTable) {
                                if (inLeft) {
                                    if (rows
                                            .get(0)
                                            .getEntries()
                                            .get(whereAttrIndex)
                                            .compareTo(whereAttrValueEntry) < 0) {
                                        ArrayList<String> tmpRow = new ArrayList<>();
                                        tmpRow.add(rows.get(0).toString() + ", "
                                                + rows.get(1).toString());
                                        resp.rowList.add(tmpRow);
                                    }
                                } else {
                                    if (rows
                                            .get(1)
                                            .getEntries()
                                            .get(whereAttrIndex)
                                            .compareTo(whereAttrValueEntry) < 0) {
                                        ArrayList<String> tmpRow = new ArrayList<>();
                                        tmpRow.add(rows.get(0).toString() + ", "
                                                + rows.get(1).toString());
                                        resp.rowList.add(tmpRow);
                                    }
                                }
                            }
                            break;
                        case ">":
                            for (ArrayList<Row> rows : joinedTable) {
                                if (inLeft) {
                                    if (rows
                                            .get(0)
                                            .getEntries()
                                            .get(whereAttrIndex)
                                            .compareTo(whereAttrValueEntry) > 0) {
                                        ArrayList<String> tmpRow = new ArrayList<>();
                                        tmpRow.add(rows.get(0).toString() + ", "
                                                + rows.get(1).toString());
                                        resp.rowList.add(tmpRow);
                                    }
                                } else {
                                    if (rows
                                            .get(1)
                                            .getEntries()
                                            .get(whereAttrIndex)
                                            .compareTo(whereAttrValueEntry) > 0) {
                                        ArrayList<String> tmpRow = new ArrayList<>();
                                        tmpRow.add(rows.get(0).toString() + ", "
                                                + rows.get(1).toString());
                                        resp.rowList.add(tmpRow);
                                    }
                                }
                            }
                            break;
                        case "<=":
                            for (ArrayList<Row> rows : joinedTable) {
                                if (inLeft) {
                                    if (rows
                                            .get(0)
                                            .getEntries()
                                            .get(whereAttrIndex)
                                            .compareTo(whereAttrValueEntry) <= 0) {
                                        ArrayList<String> tmpRow = new ArrayList<>();
                                        tmpRow.add(rows.get(0).toString() + ", "
                                                + rows.get(1).toString());
                                        resp.rowList.add(tmpRow);
                                    }
                                } else {
                                    if (rows
                                            .get(1)
                                            .getEntries()
                                            .get(whereAttrIndex)
                                            .compareTo(whereAttrValueEntry) <= 0) {
                                        ArrayList<String> tmpRow = new ArrayList<>();
                                        tmpRow.add(rows.get(0).toString() + ", "
                                                + rows.get(1).toString());
                                        resp.rowList.add(tmpRow);
                                    }
                                }
                            }
                            break;
                        case ">=":
                            for (ArrayList<Row> rows : joinedTable) {
                                if (inLeft) {
                                    if (rows
                                            .get(0)
                                            .getEntries()
                                            .get(whereAttrIndex)
                                            .compareTo(whereAttrValueEntry) >= 0) {
                                        ArrayList<String> tmpRow = new ArrayList<>();
                                        tmpRow.add(rows.get(0).toString() + ", "
                                                + rows.get(1).toString());
                                        resp.rowList.add(tmpRow);
                                    }
                                } else {
                                    if (rows
                                            .get(1)
                                            .getEntries()
                                            .get(whereAttrIndex)
                                            .compareTo(whereAttrValueEntry) >= 0) {
                                        ArrayList<String> tmpRow = new ArrayList<>();
                                        tmpRow.add(rows.get(0).toString() + ", "
                                                + rows.get(1).toString());
                                        resp.rowList.add(tmpRow);
                                    }
                                }
                            }
                            break;
                        case "<>":
                            for (ArrayList<Row> rows : joinedTable) {
                                if (inLeft) {
                                    if (rows
                                            .get(0)
                                            .getEntries()
                                            .get(whereAttrIndex)
                                            .compareTo(whereAttrValueEntry) != 0) {
                                        ArrayList<String> tmpRow = new ArrayList<>();
                                        tmpRow.add(rows.get(0).toString() + ", "
                                                + rows.get(1).toString());
                                        resp.rowList.add(tmpRow);
                                    }
                                } else {
                                    if (rows
                                            .get(1)
                                            .getEntries()
                                            .get(whereAttrIndex)
                                            .compareTo(whereAttrValueEntry) != 0) {
                                        ArrayList<String> tmpRow = new ArrayList<>();
                                        tmpRow.add(rows.get(0).toString() + ", "
                                                + rows.get(1).toString());
                                        resp.rowList.add(tmpRow);
                                    }
                                }
                            }
                            break;
                        default:
                            break;
                    }
                }
            }
            else{
                //部分列
                ArrayList<Integer> leftAttrIndices = new ArrayList<>();
                ArrayList<Integer> rightAttrIndices = new ArrayList<>();
                for(int i=0;i<resultTables.size();i++){
                    String tmpTableName = resultTables.get(i);
                    String tmpColumnName = resultColumns.get(i);
                    if(tmpTableName.equals("*")){
                        boolean inLeft = false;
                        for(int j=0;j<leftColumns.size();j++){
                            if(leftColumns.get(j).name().equals(tmpColumnName)){
                                leftAttrIndices.add(j);
                                inLeft = true;
                                break;
                            }
                        }
                        if(!inLeft){
                            for(int j=0;j<rightColumns.size();j++){
                                if(rightColumns.get(j).name().equals(tmpColumnName)){
                                    rightAttrIndices.add(j);
                                    break;
                                }
                            }
                        }
                    }
                    else if(resultColumns.get(i).equals("*")){
                        if(tmpTableName.equals(leftTableName)){
                            for(int j=0;j<leftColumns.size();j++){
                                leftAttrIndices.add(j);
                            }
                        }
                        else{
                            for(int j=0;j<rightColumns.size();j++){
                                rightAttrIndices.add(j);
                            }
                        }
                    }
                    else{
                        if(tmpTableName.equals(leftTableName)){
                            for(int j=0;j<leftColumns.size();j++){
                                if(leftColumns.get(j).name().equals(tmpColumnName)){
                                    leftAttrIndices.add(j);
                                    break;
                                }
                            }
                        }
                        else{
                            for(int j=0;j<rightColumns.size();j++){
                                if(rightColumns.get(j).name().equals(tmpColumnName)){
                                    rightAttrIndices.add(j);
                                    break;
                                }
                            }
                        }
                    }
                }
                resp.columnsList = new ArrayList<>();
                for (Integer leftAttrIndex : leftAttrIndices) {
                    resp.columnsList.add("L." + leftColumns.get(leftAttrIndex).name());
                }
                for (Integer rightAttrIndex : rightAttrIndices) {
                    resp.columnsList.add("R." + rightColumns.get(rightAttrIndex).name());
                }
                resp.rowList = new ArrayList<>();
                if(whereAttrName == null){
                    //无条件
                    for (ArrayList<Row> rows : joinedTable) {
                        StringBuilder currentRow = new StringBuilder();
                        Row tmpLeftRow = rows.get(0);
                        Row tmpRightRow = rows.get(1);
                        for (int j = 0; j < leftAttrIndices.size() - 1; j++) {
                            currentRow.append(tmpLeftRow
                                    .getEntries()
                                    .get(leftAttrIndices.get(j))
                                    .toString())
                                    .append(", ");
                        }
                        currentRow.append(tmpLeftRow
                                .getEntries()
                                .get(leftAttrIndices.get(leftAttrIndices.size() - 1))
                                .toString());
                        if (!rightAttrIndices.isEmpty()) {
                            currentRow.append(", ");
                        }
                        for (int j = 0; j < rightAttrIndices.size() - 1; j++) {
                            currentRow.append(tmpRightRow
                                    .getEntries()
                                    .get(rightAttrIndices.get(j))
                                    .toString())
                                    .append(", ");
                        }
                        currentRow.append(tmpRightRow
                                .getEntries()
                                .get(rightAttrIndices.get(rightAttrIndices.size() - 1))
                                .toString());
                        ArrayList<String> tmpRow = new ArrayList<>();
                        tmpRow.add(currentRow.toString());
                        resp.rowList.add(tmpRow);
                    }
                }
                else{
                    //有条件
                    boolean whereInLeft = false;
                    int whereAttrIndex = 0;
                    Entry whereAttrEntry = new Entry(whereAttrValue);
                    for(int i=0;i<leftColumns.size();i++){
                        if(leftColumns.get(i).name().equals(whereAttrName)){
                            whereAttrIndex = i;
                            whereInLeft = true;
                            break;
                        }
                    }
                    if(!whereInLeft){
                        for(int i=0;i<rightColumns.size();i++){
                            if(rightColumns.get(i).name().equals(whereAttrName)){
                                whereAttrIndex = i;
                                break;
                            }
                        }
                    }
                    switch (whereComparator){
                        case "=":
                            for (ArrayList<Row> rows : joinedTable) {
                                Row tmpLeftRow = rows.get(0);
                                Row tmpRightRow = rows.get(1);
                                boolean valid = false; // 这一行是否满足where条件
                                if (whereInLeft) {
                                    if (tmpLeftRow.getEntries().get(whereAttrIndex).compareTo(whereAttrEntry) == 0) {
                                        valid = true;
                                    }
                                } else {
                                    if (tmpRightRow.getEntries().get(whereAttrIndex).compareTo(whereAttrEntry) == 0) {
                                        valid = true;
                                    }
                                }
                                if (valid) {
                                    StringBuilder currentRow = new StringBuilder();
                                    if(!leftAttrIndices.isEmpty()){
                                    for (int j = 0; j < leftAttrIndices.size() - 1; j++) {
                                        currentRow
                                                .append(tmpLeftRow.getEntries().get(leftAttrIndices.get(j)).toString())
                                                .append(", ");
                                    }
                                        currentRow
                                                .append(tmpLeftRow.getEntries()
                                                        .get(leftAttrIndices.get(leftAttrIndices.get(leftAttrIndices.size()-1))).toString());
                                    }
                                    if (!rightAttrIndices.isEmpty()) {
                                        currentRow.append(", ");
                                    for (int j = 0; j < rightAttrIndices.size() - 1; j++) {
                                        currentRow
                                                .append(tmpRightRow.getEntries().get(rightAttrIndices.get(j)).toString())
                                                .append(", ");
                                    }
                                        currentRow
                                                .append(tmpRightRow.getEntries()
                                                        .get(rightAttrIndices.get(rightAttrIndices.get(rightAttrIndices.size()-1))).toString());
                                    }
                                    ArrayList<String> tmpRow = new ArrayList<>();
                                    tmpRow.add(currentRow.toString());
                                    resp.rowList.add(tmpRow);
                                }
                            }
                            break;
                        case "<":
                            for (ArrayList<Row> rows : joinedTable) {
                                Row tmpLeftRow = rows.get(0);
                                Row tmpRightRow = rows.get(1);
                                boolean valid = false; // 这一行是否满足where条件
                                if (whereInLeft) {
                                    if (tmpLeftRow.getEntries().get(whereAttrIndex).compareTo(whereAttrEntry) < 0) {
                                        valid = true;
                                    }
                                } else {
                                    if (tmpRightRow.getEntries().get(whereAttrIndex).compareTo(whereAttrEntry) < 0) {
                                        valid = true;
                                    }
                                }
                                if (valid) {
                                    StringBuilder currentRow = new StringBuilder();
                                    if(!leftAttrIndices.isEmpty()){
                                    for (int j = 0; j < leftAttrIndices.size() - 1; j++) {
                                        currentRow.append(tmpLeftRow.getEntries().get(leftAttrIndices.get(j)).toString()).append(", ");
                                    }
                                        currentRow.append(tmpLeftRow.getEntries().get(leftAttrIndices.get(leftAttrIndices.size() - 1)).toString());
                                    }
                                    if (!rightAttrIndices.isEmpty()) {
                                        currentRow.append(", ");
                                    for (int j = 0; j < rightAttrIndices.size() - 1; j++) {
                                        currentRow.append(tmpRightRow.getEntries().get(rightAttrIndices.get(j)).toString()).append(", ");
                                    }
                                        currentRow.append(tmpRightRow.getEntries().get(rightAttrIndices.get(rightAttrIndices.size() - 1)).toString());
                                    }
                                    ArrayList<String> tmpRow = new ArrayList<>();
                                    tmpRow.add(currentRow.toString());
                                    resp.rowList.add(tmpRow);
                                }
                            }
                            break;
                        case ">":
                            for (ArrayList<Row> rows : joinedTable) {
                                Row tmpLeftRow = rows.get(0);
                                Row tmpRightRow = rows.get(1);
                                boolean valid = false; // 这一行是否满足where条件
                                if (whereInLeft) {
                                    if (tmpLeftRow.getEntries().get(whereAttrIndex).compareTo(whereAttrEntry) > 0) {
                                        valid = true;
                                    }
                                } else {
                                    if (tmpRightRow.getEntries().get(whereAttrIndex).compareTo(whereAttrEntry) > 0) {
                                        valid = true;
                                    }
                                }
                                if (valid) {
                                    StringBuilder currentRow = new StringBuilder();
                                    if(!leftAttrIndices.isEmpty()){
                                    for (int j = 0; j < leftAttrIndices.size() - 1; j++) {
                                        currentRow.append(tmpLeftRow.getEntries().get(leftAttrIndices.get(j)).toString()).append(", ");
                                    }
                                        currentRow.append(tmpLeftRow.getEntries().get(leftAttrIndices.get(leftAttrIndices.size() - 1)).toString());
                                    }
                                    if (!rightAttrIndices.isEmpty()) {
                                        currentRow.append(", ");
                                        for (int j = 0; j < rightAttrIndices.size() - 1; j++) {
                                            currentRow.append(tmpRightRow.getEntries().get(rightAttrIndices.get(j)).toString()).append(", ");
                                        }
                                        currentRow.append(tmpRightRow.getEntries().get(rightAttrIndices.get(rightAttrIndices.size() - 1)).toString());
                                    }
                                    ArrayList<String> tmpRow = new ArrayList<>();
                                    tmpRow.add(currentRow.toString());
                                    resp.rowList.add(tmpRow);
                                }
                            }
                            break;
                        case "<=":
                            for (ArrayList<Row> rows : joinedTable) {
                                Row tmpLeftRow = rows.get(0);
                                Row tmpRightRow = rows.get(1);
                                boolean valid = false; // 这一行是否满足where条件
                                if (whereInLeft) {
                                    if (tmpLeftRow.getEntries().get(whereAttrIndex).compareTo(whereAttrEntry) <= 0) {
                                        valid = true;
                                    }
                                } else {
                                    if (tmpRightRow.getEntries().get(whereAttrIndex).compareTo(whereAttrEntry) <= 0) {
                                        valid = true;
                                    }
                                }
                                if (valid) {
                                    StringBuilder currentRow = new StringBuilder();
                                    if(!leftAttrIndices.isEmpty()){
                                    for (int j = 0; j < leftAttrIndices.size() - 1; j++) {
                                        currentRow.append(tmpLeftRow.getEntries().get(leftAttrIndices.get(j)).toString()).append(", ");
                                    }
                                        currentRow.append(tmpLeftRow.getEntries().get(leftAttrIndices.get(leftAttrIndices.size() - 1)).toString());
                                    }
                                    if (!rightAttrIndices.isEmpty()) {
                                        currentRow.append(", ");
                                        for (int j = 0; j < rightAttrIndices.size() - 1; j++) {
                                            currentRow.append(tmpRightRow.getEntries().get(rightAttrIndices.get(j)).toString()).append(", ");
                                        }
                                        currentRow.append(tmpRightRow.getEntries().get(rightAttrIndices.get(rightAttrIndices.size() - 1)).toString());
                                    }
                                    ArrayList<String> tmpRow = new ArrayList<>();
                                    tmpRow.add(currentRow.toString());
                                    resp.rowList.add(tmpRow);
                                }
                            }
                            break;
                        case ">=":
                            for (ArrayList<Row> rows : joinedTable) {
                                Row tmpLeftRow = rows.get(0);
                                Row tmpRightRow = rows.get(1);
                                boolean valid = false; // 这一行是否满足where条件
                                if (whereInLeft) {
                                    if (tmpLeftRow.getEntries().get(whereAttrIndex).compareTo(whereAttrEntry) >= 0) {
                                        valid = true;
                                    }
                                } else {
                                    if (tmpRightRow.getEntries().get(whereAttrIndex).compareTo(whereAttrEntry) >= 0) {
                                        valid = true;
                                    }
                                }
                                if (valid) {
                                    StringBuilder currentRow = new StringBuilder();
                                    if(!leftAttrIndices.isEmpty()) {
                                        for (int j = 0; j < leftAttrIndices.size() - 1; j++) {
                                            currentRow.append(tmpLeftRow.getEntries().get(leftAttrIndices.get(j)).toString()).append(", ");
                                        }
                                        currentRow.append(tmpLeftRow.getEntries().get(leftAttrIndices.get(leftAttrIndices.size() - 1)).toString());
                                    }
                                    if (!rightAttrIndices.isEmpty()) {
                                        currentRow.append(", ");
                                        for (int j = 0; j < rightAttrIndices.size() - 1; j++) {
                                            currentRow.append(tmpRightRow.getEntries().get(rightAttrIndices.get(j)).toString()).append(", ");
                                        }
                                        currentRow.append(tmpRightRow.getEntries().get(rightAttrIndices.get(rightAttrIndices.size() - 1)).toString());
                                    }
                                    ArrayList<String> tmpRow = new ArrayList<>();
                                    tmpRow.add(currentRow.toString());
                                    resp.rowList.add(tmpRow);
                                }
                            }
                            break;
                        case "<>":
                            for (ArrayList<Row> rows : joinedTable) {
                                Row tmpLeftRow = rows.get(0);
                                Row tmpRightRow = rows.get(1);
                                boolean valid = false; // 这一行是否满足where条件
                                if (whereInLeft) {
                                    if (tmpLeftRow.getEntries().get(whereAttrIndex).compareTo(whereAttrEntry) != 0) {
                                        valid = true;
                                    }
                                } else {
                                    if (tmpRightRow.getEntries().get(whereAttrIndex).compareTo(whereAttrEntry) != 0) {
                                        valid = true;
                                    }
                                }
                                if (valid) {
                                    StringBuilder currentRow = new StringBuilder();
                                    if(!leftAttrIndices.isEmpty()) {
                                        for (int j = 0; j < leftAttrIndices.size() - 1; j++) {
                                            currentRow.append(tmpLeftRow.getEntries().get(leftAttrIndices.get(j)).toString()).append(", ");
                                        }
                                        currentRow.append(tmpLeftRow.getEntries().get(leftAttrIndices.get(leftAttrIndices.size() - 1)).toString());
                                    }
                                    if (!rightAttrIndices.isEmpty()) {
                                        currentRow.append(", ");
                                    for (int j = 0; j < rightAttrIndices.size() - 1; j++) {
                                        currentRow.append(tmpRightRow.getEntries().get(rightAttrIndices.get(j)).toString()).append(", ");
                                    }
                                        currentRow.append(tmpRightRow.getEntries().get(rightAttrIndices.get(rightAttrIndices.size() - 1)).toString());
                                    }
                                    ArrayList<String> tmpRow = new ArrayList<>();
                                    tmpRow.add(currentRow.toString());
                                    resp.rowList.add(tmpRow);
                                }
                            }
                            break;
                        default:
                            break;
                    }

                }
            }
        }
    }

    @Override
    public void exitParse(SQLParser.ParseContext ctx) {
        Pattern create_db = Pattern.compile(Global.CREATE_DATABASE);
        Pattern drop_db = Pattern.compile(Global.DROP_DATABASE);
        Pattern show_dbs = Pattern.compile(Global.SHOW_DATABASES);
        Pattern use = Pattern.compile(Global.USE);
        Pattern show_db = Pattern.compile(Global.SHOW_DATABASE);
        Pattern show_table = Pattern.compile(Global.SHOW_TABLE);

        Matcher create_db_match = create_db.matcher(command);
        Matcher drop_db_match = drop_db.matcher(command);
        Matcher show_dbs_match = show_dbs.matcher(command);
        Matcher use_match = use.matcher(command);
        Matcher show_db_match = show_db.matcher(command);
        Matcher show_table_match = show_table.matcher(command);

        if (!(create_db_match.matches()||drop_db_match.matches()||show_dbs_match.matches()||use_match.matches()||
                show_db_match.matches()||show_table_match.matches()))
            manager.appendLog(command, tnum);

        if (autoCommit) manager.quit();
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
