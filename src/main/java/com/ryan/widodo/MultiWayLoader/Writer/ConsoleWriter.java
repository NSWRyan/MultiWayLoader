package com.ryan.widodo.MultiWayLoader.Writer;

import java.util.StringJoiner;

import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.orc.TypeDescription;

public class ConsoleWriter extends Writer{
    Schema schema;
    TypeDescription typeDescription;
    TableMeta tableMeta;

    public ConsoleWriter(TableMeta tableMeta) {
        super(tableMeta.columnType, LogManager.getLogger(FileWriter.class)); 
        this.tableMeta=tableMeta;
        StringJoiner sj = new StringJoiner(", ");
        for(String columnName:tableMeta.columnName){
            sj.add(columnName);
        }
        System.out.println(sj);
    }

    @Override
    public void writeRow(Object[] row) {
        StringJoiner sj = new StringJoiner(", ");
        for(int i=0;i<row.length;i++){
            sj.add(row.toString());
        }
        System.out.println(sj);
    }

    @Override
    public void close() {
        // Do nothing.
    }

}
