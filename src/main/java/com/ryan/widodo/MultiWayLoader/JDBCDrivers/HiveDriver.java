package com.ryan.widodo.MultiWayLoader.JDBCDrivers;

import java.sql.*;
import java.util.List;
import java.util.StringJoiner;

import org.apache.logging.log4j.LogManager;

/**
 * Hive JDBC driver class.
 * Handles the basic connections with generic DB.
 */
public class HiveDriver extends GenericDriver{
    public static StringJoiner batchRows;

    /**
     * A driver code for general JDBC code designed for data select and data read.
     * @param username = The DB login username.
     * @param password = The DB login password.
     * @param jdbcClass = The DB class example: {@link com.teradata.jdbc.TeraDriver}.
     * @param jdbcURL = The JDBC URL, containing connection settings.
     * @throws SQLException = Exception when connection fails.
     * @throws ClassNotFoundException = May occur when the JDBC driver is not in the classpath.
     */
    public HiveDriver(String username, String password, String jdbcClass, String jdbcURL)
            throws SQLException, ClassNotFoundException {
        super(username, password, jdbcClass, jdbcURL);
        logger = LogManager.getLogger(this.getClass());
        connect();
    }

    /**
     * A driver code for general JDBC code designed for data select and data read. This one is for login without user and password.
     * @param jdbcClass = The DB class example: {@link com.teradata.jdbc.TeraDriver}.
     * @param jdbcURL = The JDBC URL, containing connection settings.
     * @throws SQLException = Exception when connection fails.
     * @throws ClassNotFoundException = May occur when the JDBC driver is not in the classpath.
     */
    public HiveDriver(String jdbcClass, String jdbcURL)
            throws SQLException, ClassNotFoundException {
        super(jdbcClass, jdbcURL);
        logger = LogManager.getLogger(this.getClass());
        connect();
    }
    
    /**
     * Write using Hive JDBC is not recommended because it does not support batched insertion.
     * @{link org.apache.hive.jdbc.HivePreparedStatement}
     */
    @Override
    public void writeRow(Object[] row) throws SQLException{
        addRow(row);
        currentBatch++;
        if(currentBatch>batchSize){
            flushBatch();
        }
    }

    @Override
    public void prepareWrite(String tableName, List<Integer> targetTypes, long batchSize) throws SQLException{
        this.tableName=tableName;
        this.batchSize=batchSize;
    }

    private String getInsertionString(String tableName) throws SQLException {
        return "INSERT INTO "+tableName+" VALUES ";
    }

    /**
     * Flush the batch to the target server.
     * Hive flush does not use Java SQL PS batch.
     * 
     * @throws SQLException
     */
    @Override
    public void flushBatch() throws SQLException{
        logger.debug("Flushing writer batch");
        logger.debug(getInsertionString(tableName)+batchRows.toString());
        stmnt.executeQuery(getInsertionString(tableName)+batchRows.toString());
        batchRows=new StringJoiner(",");
        writtenCount+=currentBatch;
        currentBatch=0;
        logger.debug("Batch flushed");
    }

    @Override
    public void closeWriter() throws SQLException{
        if(currentBatch>0){
            flushBatch();
        }
        runSQL("SELECT COUNT(*) FROM " + tableName);
        long rowCount=0;
        while (rSet.next()) {
            rowCount = rSet.getLong(1);
        }
        logger.info("Destination table count = " + rowCount);
        logger.info("Total write count = " + writtenCount);

        pStatement.close();
        pStatement=null;
        writeMode=false;
        writtenCount=0;
        writeMode=false;
    }

    public static void addRow(Object[] objects){
        if(batchRows==null){
            batchRows=new StringJoiner(",");
        }
        StringJoiner stringJoiner=new StringJoiner(",");
        for(int i=0;i<objects.length;i++){
            if(objects[i]==null){
                objects[i]="null";
            }
            stringJoiner.add('"'+objects[i].toString()+'"');
        }
        batchRows.add("("+stringJoiner.toString()+")");
    }
}
