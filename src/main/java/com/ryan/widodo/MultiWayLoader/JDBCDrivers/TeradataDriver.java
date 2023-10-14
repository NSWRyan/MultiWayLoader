package com.ryan.widodo.MultiWayLoader.JDBCDrivers;

import java.sql.*;
import java.util.List;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;

/**
 * Teradata JDBC driver class.
 * Handles the basic connections with Teradata DB.
 */
public class TeradataDriver extends BaseDriver{
    private boolean fastload=false;
    /**
     * A driver code for Teradata JDBC code designed for data select and data read.
     * @param username = Teradata username.
     * @param password = Teradata password. If using LDAP or MECH, mention it in the URL.
     * @param jdbcClass = The Teradata class {@link com.teradata.jdbc.TeraDriver}.
     * @param jdbcURL = The JDBC URL, containing connection settings.
     * @throws SQLException = Exception when connection fails.
     * @throws ClassNotFoundException = May occur when the JDBC driver is not in the classpath.
     */
    public TeradataDriver(String username, String password, String jdbcClass, String jdbcURL)
            throws SQLException, ClassNotFoundException {
        super(username, password, jdbcClass, jdbcURL);
        if(jdbcURL.toLowerCase().contains("fastload")){
            fastload=true;
        }
        logger = LogManager.getLogger(this.getClass());
        connect();
    }

    /**
     * Not implemented yet. But this is planned for Teradata fastexport.
     * @param table
     * @throws SQLException
     */
    public void batchedRead(String table) throws SQLException{
        throw new NotImplementedException("Planned for future");
    }
    
    @Override
    public void recreateTable(String table) throws SQLException {
        runSQL("CREATE TABLE " + table + "_tmp AS " + table + " WITH NO DATA");
        runSQL("DROP TABLE " + table);
        runSQL("RENAME TABLE " + table + "_tmp AS " + table);
        logger.info("Recreate table done");
    }
    
    @Override
    public void prepareWrite(String tableName, List<Integer> targetTypes, long batchSize) throws SQLException{
        recreateTable(tableName);
        if(fastload)
            con.setAutoCommit(false);
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
