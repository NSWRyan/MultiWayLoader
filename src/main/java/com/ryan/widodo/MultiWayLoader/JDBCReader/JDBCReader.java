package com.ryan.widodo.MultiWayLoader.JDBCReader;

import org.apache.logging.log4j.Logger;

import com.ryan.widodo.MultiWayLoader.JDBCTools;
import com.ryan.widodo.MultiWayLoader.JDBCDrivers.BaseDriver;
import com.ryan.widodo.MultiWayLoader.Writer.TableMeta;

import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;

public class JDBCReader extends Thread implements AutoCloseable{ 
    private Logger logger = LogManager.getLogger(JDBCReader.class);
    BaseDriver baseDriver;
    String sqlCommand;
    public BatchData[] batchData;
    long batchSize=10000;
    /**
     * Max batch count. One batch count may have multiple rows.
     */
    int batchCount=2;
    public volatile boolean finished;
    public TableMeta tableMeta;
    int columnCount=0;
    public boolean stop=false;
    private static final Semaphore semaphore=new Semaphore(1);

    public boolean ready=false;
    /**
     * <pre>
     * These are the possible 5for this class.
     * 0 = normal.
     * 1 = exception during read.
     * 2 = exception during closing <deprecated>.
     * 3 = exception in the thread.
     * 4 = sleep failure.
     * 5 = stopped by another thread via stop variable.
     * </pre>
     */
    int exitCode=0;
    /**
     * The sleep timer in ms.
     */
    private int sleepDurationms = 10;
    public long sleepTimems = 0;
    public long workTimems = 0;

    public long writerSleepTime = 0;

    /**
     * Initialize JDBCReader thread. This does not start the thread yet, just performing some select command.
     * To start reading, use start(), this will execute the code in run().
     * @param baseDriver = The generic driver.
     * @param sqlCommand
     * @param tableName
     * @param batchSize
     * @param batchCount
     */
    public JDBCReader(BaseDriver baseDriver, String sqlCommand, String tableName, long batchSize, int batchCount){
        this.baseDriver=baseDriver;
        this.sqlCommand=sqlCommand;
        this.batchSize=batchSize;
        this.batchCount=batchCount;

        try {
            baseDriver.runSQL(sqlCommand);
            this.tableMeta=new TableMeta(baseDriver.rSmd);
            columnCount=tableMeta.columnName.size();
        } catch (Exception e) {
            JDBCTools.printException(logger, e);
        }
        batchData=new BatchData[batchCount];
        for(int i=0;i<batchCount;i++){
            batchData[i]=new BatchData();
        }

        // Exception handling during run.
        Thread.setDefaultUncaughtExceptionHandler(
            new Thread.UncaughtExceptionHandler() {
                public void uncaughtException(Thread t, Throwable e) {
                    JDBCTools.printException(logger,new Exception(e));
                    exitCode=3;
                }
            }
        );
        
    }

    /**
     * Loop the JDBC result into BatchData.
     * Once a BatchData is full, it will swap to the next one.st
     */
    public void run() {
        finished=false;
        int currentBatch=0;
        int currentBatchSize=0;
        try {
            while(baseDriver.rSet.next()){
                long timer2 = System.currentTimeMillis();
                // Fill the row
                Object[] row=new Object[columnCount];
                for(int i=1;i<=columnCount;i++){
                    row[i-1]=baseDriver.rSet.getObject(i);
                }

                // Batch management
                // If it is still with written status
                // Loop until read is finished.
                while(batchData[currentBatch].written){
                    long timer1 = System.currentTimeMillis();
                    if (this.stop == true) {
                        this.finished = true;
                        this.exitCode=5;
                        return;
                    }
                    sleepMS(sleepDurationms);
                    sleepTimems += System.currentTimeMillis() - timer1;
                    currentBatch=(currentBatch + 1)%batchCount;
                }
                batchData[currentBatch].rows.add(row);
                currentBatchSize++;

                // If the batch is full, then move to the next batch.
                if(currentBatchSize>=batchSize){
                    currentBatchSize=0;
                    batchData[currentBatch].written=true;
                    currentBatch = (currentBatch + 1) % batchCount;
                }
                workTimems += System.currentTimeMillis() - timer2;
            }
            batchData[currentBatch].written=true;
            logger.info("Reader finished");
            this.finished=true;
        } catch (Exception e) {
            JDBCTools.printException(logger, e);
            exitCode=2;
        }
    }

    /**
     * Get {@link com.ryan.widodo.MultiWayLoader.JDBCReader.BatchData} from this JDBCReader.
     * @return {@link com.ryan.widodo.MultiWayLoader.JDBCReader.BatchData} Ready to read BatchData.
     * @throws InterruptedException For multithreading safety.
     */
    public synchronized BatchData getBatchData() throws InterruptedException{
        long timer1=System.currentTimeMillis();
        semaphore.acquire();
        int clientCurrentBatch=0;
        while((!batchData[clientCurrentBatch].written||batchData[clientCurrentBatch].isBeingRead)&&!finished){
            clientCurrentBatch=(clientCurrentBatch+1)%batchCount;
            sleep(sleepDurationms);
        }
        logger.info("Current batch = "+clientCurrentBatch);
        batchData[clientCurrentBatch].isBeingRead=true;
        semaphore.release();
        writerSleepTime+=System.currentTimeMillis()-timer1;
        return batchData[clientCurrentBatch];
    }

    /**
     * Sleep this thread for some ms.
     * 
     * @param ms duration of sleep in millisecond(s).
     */
    private void sleepMS(long ms) {
        try {
            sleep(ms);
        } catch (Exception e) {
            JDBCTools.printException(logger, e);
            exitCode=4;
        }
    }

    /**
     * Close this connection.
     */
    public void close(){
        stop=true;
        while(!this.finished){
            sleepMS(sleepDurationms);
        }
        batchData=null;
    }

    /**
     * Set the batch size for read and write.
     * @param batchSize
     */
    public void setBatchSize(long batchSize){
        this.batchSize=batchSize;
    }
}
