package com.ryan.widodo.MultiWayLoader.JDBCDrivers;

import java.sql.*;
import java.util.List;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;

/**
 * Generic JDBC driver class.
 * Handles the basic connections with generic DB.
 */
public class GenericDriver extends BaseDriver{
    /**
     * A driver code for general JDBC code designed for data select and data read.
     * @param username = The DB login username.
     * @param password = The DB login password.
     * @param jdbcClass = The DB class example: {@link com.teradata.jdbc.TeraDriver}.
     * @param jdbcURL = The JDBC URL, containing connection settings.
     * @throws SQLException = Exception when connection fails.
     * @throws ClassNotFoundException = May occur when the JDBC driver is not in the classpath.
     */
    public GenericDriver(String username, String password, String jdbcClass, String jdbcURL)
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
    public GenericDriver(String jdbcClass, String jdbcURL)
            throws SQLException, ClassNotFoundException {
        super(jdbcClass, jdbcURL);
        logger = LogManager.getLogger(this.getClass());
        connect();
    }
    
    @Override
    public void recreateTable(String table) throws SQLException {
        throw new NotImplementedException("Not implemented, please do manually with runSQL instead");
    }
    
    @Override
    public void prepareWrite(String tableName, List<Integer> targetTypes, long batchSize) throws SQLException{
        super.prepareWrite(tableName, targetTypes, batchSize);
    }

    @Override
    public void closeWriter() throws SQLException{
        if(currentBatch>0){
            flushBatch();
        }
        con.commit();
        runSQL("SELECT COUNT(*) FROM " + tableName);
        long rowCount=0;
        while (rSet.next()) {
            rowCount = rSet.getLong(1);
        }
        logger.info("Destination table count = " + rowCount);
        logger.info("Total write count = " + writtenCount);
        super.closeWriter();
    }
}
