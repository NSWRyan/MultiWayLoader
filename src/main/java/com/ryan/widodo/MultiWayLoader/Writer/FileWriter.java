package com.ryan.widodo.MultiWayLoader.Writer;


import com.ryan.widodo.FormattedFileWriter.FormattedFileWriter;
import com.ryan.widodo.FormattedFileWriter.ORCWriter;
import com.ryan.widodo.MultiWayLoader.JDBCTools;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.logging.log4j.LogManager;
import org.apache.orc.TypeDescription;

public class FileWriter extends Writer{
    FormattedFileWriter formattedFileWriter;
    Schema schema;
    TypeDescription typeDescription;
    TableMeta tableMeta;
    int batchSize=10000;
    private int currentInsert=0;

    public FileWriter(TableMeta tableMeta, String writerClass, int batchSize) {
        super(tableMeta.columnType, LogManager.getLogger(FileWriter.class)); 
        this.tableMeta=tableMeta;
        this.schema=JDBCTools.tableMetaToAvroSchema(tableMeta);
        this.typeDescription=JDBCTools.tableMetaToTypeDescription(tableMeta);
        this.batchSize=batchSize;
        logger = LogManager.getLogger(this.getClass());
        formattedFileWriter=new ORCWriter(schema, true, true, 100, typeDescription, schema.getName());
    }

    @Override
    public void writeRow(Object[] row) {
        // Record record=new Record(schema);
        for(int i=0;i<row.length;i++){
            if(row[i] instanceof java.math.BigDecimal)
                row[i]=((BigDecimal)row[i]).doubleValue();
            // logger.debug(row[i].getClass().getName());
            // record.put(i, row[i]);
        }

        formattedFileWriter.writeObjects(Arrays.asList(row));
        if(++currentInsert>batchSize){
            formattedFileWriter.flush();
        }
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
        formattedFileWriter.close();
    }

    
}
