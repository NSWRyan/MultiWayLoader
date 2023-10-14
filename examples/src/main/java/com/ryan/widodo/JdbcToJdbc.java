package com.ryan.widodo;

import org.apache.logging.log4j.Logger;

import com.ryan.widodo.MultiWayLoader.JDBCDrivers.BaseDriver;
import com.ryan.widodo.MultiWayLoader.JDBCDrivers.GenericDriver;
import com.ryan.widodo.MultiWayLoader.JDBCDrivers.HiveDriver;
import com.ryan.widodo.MultiWayLoader.JDBCDrivers.TeradataDriver;
import com.ryan.widodo.MultiWayLoader.JDBCReader.BatchData;
import com.ryan.widodo.MultiWayLoader.JDBCReader.JDBCReader;
import com.ryan.widodo.MultiWayLoader.Writer.ConsoleWriter;
import com.ryan.widodo.MultiWayLoader.Writer.FormattedFileWriter;
import com.ryan.widodo.MultiWayLoader.Writer.JDBCWriter;
import com.ryan.widodo.MultiWayLoader.Writer.TableMeta;
import com.ryan.widodo.MultiWayLoader.Writer.Writer;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import org.apache.logging.log4j.LogManager;

/**
 * JDBC to JDBC example.
 */
public class JdbcToJdbc { 
    private static Logger logger = LogManager.getLogger(Main.class);
    
    public static void main( String[] args ){
        String jdbcClass="com.teradata.jdbc.TeraDriver";
        String jdbcURL="jdbc:teradata://db1/database=DBC,TMODE=ANSI,CHARSET=UTF8,RUNSTARTUP=ON,REDRIVE=4,RECONNECT_COUNT=5,SESSIONS=10";
        String jdbcURL2="jdbc:teradata://db2/database=DBC,TMODE=ANSI,CHARSET=UTF8,RUNSTARTUP=ON,REDRIVE=4,RECONNECT_COUNT=5,SESSIONS=10";
        BaseDriver driver1 = null, driver2 = null;
        try {
            driver1=new TeradataDriver("user1", "passwd1", jdbcClass, jdbcURL);
            driver2=new TeradataDriver("user1", "password2", jdbcClass, jdbcURL2);
            JDBCReader jdbcReader=new JDBCReader(driver1, "select top 10 * from db1.table_1", "db1.table_1", 100000, 2);
            jdbcReader.start();
            Writer writer=new JDBCWriter(driver2, "db1.table_2", new TableMeta(driver1.rSmd),10000);
            
            jdbcReader.setBatchSize(10000);
            long count=0;
            long timer1=System.currentTimeMillis();
            long timer2=System.currentTimeMillis();
            long writeTime=0;
            while(jdbcReader.finished!=true){
                logger.info("Current insert = "+count);
                BatchData batchData=jdbcReader.getBatchData();
                timer2=System.currentTimeMillis();
                for(Object[] row:batchData.rows){
                    writer.writeRow(row);
                }
                writeTime+=System.currentTimeMillis()-timer2;
                count+=batchData.rows.size();
                batchData.reset();
            }

            for(BatchData batchData:jdbcReader.batchData){
                if(batchData.written==true&&batchData.isBeingRead==false){
                    timer2=System.currentTimeMillis();
                    for(Object[] row:batchData.rows){
                        writer.writeRow(row);
                    }
                    writeTime+=System.currentTimeMillis()-timer2;
                    batchData.reset();
                }
            }
            System.out.println();
            writer.close();
            jdbcReader.close();
            logger.info("Writer work time: "+writeTime);
            logger.info("Writer sleep time: "+jdbcReader.writerSleepTime);
            logger.info("Total duration: "+(System.currentTimeMillis()-timer1));
        } catch (ClassNotFoundException e) {
            JDBCTools.printException(logger, e);
        } catch (SQLException e) {
            JDBCTools.printException(logger, e);
        } catch (InterruptedException e){
            JDBCTools.printException(logger, e);
        }finally{
            try {
                if(driver1!=null)
                    driver1.close();
                if(driver2!=null)
                    driver2.close();
            } catch (Exception e) {
                JDBCTools.printException(logger, e);
            }
        }
    }
    
    
}