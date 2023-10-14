package com.ryan.widodo.MultiWayLoader.Writer;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ryan.widodo.MultiWayLoader.JDBCTools;

public class TableMeta {
    Logger logger = LogManager.getLogger(this.getClass());
    public List<String> columnName;
    public List<Integer> columnType;
    public List<Integer> columnNullable;
    public String tableName="";

    /**
     * Initialize the TableMeta struct by filling the data from JDBC {@link java.sql.ResultSetMetaData}
     * @param resultSetMetaData The {@link java.sql.ResultSetMetaData} from a select command.
     */
    public TableMeta(ResultSetMetaData resultSetMetaData) {
        columnName = new ArrayList<>();
        columnType = new ArrayList<>();
        columnNullable = new ArrayList<>();
        try {
            int colCount = resultSetMetaData.getColumnCount();
            for (int i = 1; i <= colCount; i++) {
                columnName.add(resultSetMetaData.getColumnName(i));
                columnType.add(resultSetMetaData.getColumnType(i));
                columnNullable.add(resultSetMetaData.isNullable(i));
            }
            tableName = resultSetMetaData.getSchemaName(1) + "." + resultSetMetaData.getTableName(1);
        } catch (Exception e) {
            JDBCTools.printException(logger, e);
        }
    }

    @Override
    public String toString(){
        StringBuilder stringBuilder=new StringBuilder();
        for(int i=0;i<columnName.size();i++){
            stringBuilder.append(columnName.get(i)+","+columnType.get(i)+","+columnNullable.get(i)+"\n");
        }
        return stringBuilder.toString();
    }
}
