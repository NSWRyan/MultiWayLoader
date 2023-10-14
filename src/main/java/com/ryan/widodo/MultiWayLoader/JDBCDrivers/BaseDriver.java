package com.ryan.widodo.MultiWayLoader.JDBCDrivers;

import java.sql.*;
import java.util.List;

import org.apache.logging.log4j.Logger;

import com.ryan.widodo.MultiWayLoader.Writer.TableMeta;

/**
 * Generic JDBC driver class.
 * Handles the basic connections with Generic DB.
 */
public abstract class BaseDriver implements AutoCloseable {
    Logger logger;
    List<Integer> targetTypes=null;
    long batchSize=10000;
    int currentBatch=0;
    int writtenCount=0;

    // JDBC settings
    String jdbcURL;
    private String username = null, password = null;
    Connection con = null;
    Statement stmnt = null;
    PreparedStatement pStatement = null;
    public ResultSet rSet = null;
    public ResultSetMetaData rSmd = null;

    /**
     * Only used in Write operation.
     */
    public String tableName = "";
    boolean writeMode=false;
    TableMeta tableMeta=null;

    /**
     * The constructor class for connection without user pass login.
     * 
     * @param jdbcClass = The JDBC class.
     * @param jdbcURL = The JDBC URL.
     * @param logger = The log.
     * @throws SQLException = May occur if connection fails, check the URL or login info.
     * @throws ClassNotFoundException = Occurs then the JDBC driver is not in Java class paths (-cp)
.     */
    public BaseDriver(String jdbcClass, String jdbcURL) throws SQLException, ClassNotFoundException {
        this.jdbcURL = jdbcURL;
        // Load the driver (JAR)
        Class.forName(jdbcClass);
    }

    /**
     * The constructor class. It connects with Generic and set up the connection.
     * 
     * @param username = The username.
     * @param password = The password.
     * @param jdbcClass = The JDBC class.
     * @param jdbcURL = The JDBC URL.
     * @param logger = The log.
     * @throws SQLException = May occur if connection fails, check the URL or login info.
     * @throws ClassNotFoundException = Occurs then the JDBC driver is not in Java class paths (-cp)
.     */
    public BaseDriver(String username, String password, String jdbcClass, String jdbcURL) throws ClassNotFoundException {
        this.jdbcURL = jdbcURL;
        this.username=username;
        this.password=password;
        // Load the driver (JAR)
        Class.forName(jdbcClass);
    }

    /**
     * Connect to the DB server using the provided URL.
     * 
     * @throws SQLException
     */
    public void connect() throws SQLException{
        // Create the connection
        if(username==null)
            con = DriverManager.getConnection(jdbcURL);
        else
            con = DriverManager.getConnection(jdbcURL, username, password);
        logger.info("User " + username + " connected.");
        logger.info("Connection to DB established.");
        stmnt = con.createStatement();
    }

    /**
     * Get the insertion string for a prepared statement (batch load).
     * 
     * @param tableName = The destination target table name. Format = DB.table_name.
     * @param colCount  = The number of columns.
     * @return String insert SQL.
     * @throws SQLException
     */
    private String getInsertionString(String tableName, int colCount) throws SQLException {
        StringBuilder stringBuilder=new StringBuilder();
        stringBuilder.append("INSERT INTO ");
        stringBuilder.append(tableName);
        stringBuilder.append(" VALUES(?");
        for (int i = 1; i < colCount; i++) {
            stringBuilder.append(",?");
        }
        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    /**
     * Run SQL. The result(s) would be in rSet.
     * 
     * @param sql = The SQL to run.
     * @throws SQLException
     */
    public void runSQL(String sql) throws SQLException {
        if (rSet != null) {
            rSet.close();
        }
        rSet = stmnt.executeQuery(sql);
        rSmd = rSet.getMetaData();
        logger.info("Running : " + sql + " = > OK");
    }

    /**
     * Recreate table by dropping it and recreating it with the same format.
     * 
     * @param table = The table name.
     * @throws SQLException
     */
    public abstract void recreateTable(String table) throws SQLException;

    /**
     * Create a prepared statement for a batched write.
     * 
     * @param table = The table name.
     * @param targetTypes = A list of Java sql types.
     * @throws SQLException
     */
    public void prepareWrite(String tableName, List<Integer> targetTypes, long batchSize) throws SQLException{
        this.targetTypes=targetTypes;
        this.tableName=tableName;
        this.batchSize=batchSize;
        pStatement=con.prepareStatement(getInsertionString(tableName, targetTypes.size()));
        currentBatch=0;
        writeMode=true;
    }

    /**
     * Write a row to the target DB.
     * 
     * @param row = A list of objects (columns)
     * @throws SQLException
     */
    public void writeRow(Object[] row) throws SQLException{
        for(int i=0;i<row.length;i++){
            pStatement.setObject(i+1, row[i], targetTypes.get(i));
        }
        pStatement.addBatch();
        if(currentBatch>batchSize){
            flushBatch();
        }
        currentBatch++;
    }

    /**
     * Flush the batch to the target server.
     * 
     * @throws SQLException
     */
    public void flushBatch() throws SQLException{
        logger.debug("Flushing writer batch");
        writtenCount+=currentBatch;
        pStatement.executeBatch();
        pStatement.clearParameters();
        pStatement.clearBatch();
        currentBatch=0;
        logger.debug("Batch flushed");
    }

    /**
     * Close the preparedStatement, not the whole connection
     * @throws SQLException
     */
    public void closeWriter() throws SQLException{
        pStatement.close();
        pStatement=null;
        writeMode=false;
        writtenCount=0;
        writeMode=false;
    }

    /**
     * Safely close the connection.
     * 
     * @throws SQLException
     */
    public void close() throws SQLException {
        if(writeMode){
            closeWriter();
        }
        if (rSet != null) {
            rSet.close();
            logger.info("Closing the result statement... Done.");
        }
        if (stmnt != null) {
            stmnt.close();
            logger.info("Closing the statement... Done.");
        }
        if (pStatement != null) {
            pStatement.close();
            logger.info("Closing the read statement... Done.");
        }
        if (con != null) {
            con.close();
            logger.info("Closing the connection... Done.");
        }
    }

    /**
     * Set the batch size for read and write.
     * @param batchSize
     */
    public void setBatchSize(long batchSize){
        this.batchSize=batchSize;
    }
}
