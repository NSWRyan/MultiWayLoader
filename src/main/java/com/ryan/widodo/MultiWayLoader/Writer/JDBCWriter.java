package com.ryan.widodo.MultiWayLoader.Writer;

import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;

import com.ryan.widodo.MultiWayLoader.JDBCTools;
import com.ryan.widodo.MultiWayLoader.JDBCDrivers.BaseDriver;


public class JDBCWriter extends Writer{
    int columnCount=0;
    BaseDriver genericDriver;
    int currentBatchInsert;
    int batchSize;

    public JDBCWriter(BaseDriver genericDriver, String targetTable, TableMeta sourceTableMeta, int batchSize) {
        super(sourceTableMeta.columnType, LogManager.getLogger(JDBCWriter.class));
        this.genericDriver=genericDriver;
        this.batchSize=batchSize;
        this.currentBatchInsert=0;
        try {
            genericDriver.prepareWrite(targetTable,targetTypes,batchSize);
        } catch (SQLException e) {
            JDBCTools.printException(logger, e);
        }
        columnCount=sourceTableMeta.columnName.size();
    }

    @Override
    public void writeRow(Object[] row) {
        try {
            genericDriver.writeRow(row);
        } catch (SQLException e) {
            JDBCTools.printException(logger, e);
        }
    }

    @Override
    public void close() {
        try {
            genericDriver.closeWriter();
        } catch (SQLException e) {
            JDBCTools.printException(logger, e);
        }
    }
}
