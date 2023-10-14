package com.ryan.widodo.MultiWayLoader.JDBCReader;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class BatchData { 
    public List<Object[]> rows;

    /**
     * <pre>
     * Table:
     * written | isBeingRead | state
     * false   | false       | table is ready for loading / still being loaded
     * true    | false       | table is ready for read
     * false   | true        | illegal state
     * true    | true        | table is still being read
     * </pre>
     * Indicates if write is done or not.
     * Do not write unless written = false.
     */
    public boolean written=false;
    
    /**
     * Indicates if read is done or not.
     * If false, then this batch is not worked on yet.
     */
    public boolean isBeingRead=false;

    /**
     * Initialize the BatchData.
     */
    BatchData(){
        rows=new ArrayList<>();
    }

    /**
     * Add a new row.
     * @param row An array of objects.
     */
    public void put(Object[] row){
        rows.add(row);
    }

    /**
     * Add a new row.
     * @param row An array of objects.
     */
    public void reset(){
        rows.clear();
        rows=null;
        rows=new ArrayList<>();
        written=false;
        isBeingRead=false;
    }

    @Override
    public String toString(){
        StringBuilder stringBuilder=new StringBuilder();
        StringJoiner stringJoiner=new StringJoiner(",");
        for(Object[] row:rows){
            stringJoiner=new StringJoiner(",");
            for(Object column:row){
                if(column==null)
                    stringJoiner.add("null");
                else
                    stringJoiner.add(column.toString());
            }
            stringBuilder.append(stringJoiner.toString()+"\n");
        }
        return stringBuilder.toString();
    }
}
