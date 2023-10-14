package com.ryan.widodo.MultiWayLoader.Writer;

import java.util.List;

import org.apache.logging.log4j.Logger;


public abstract class Writer implements AutoCloseable {
    List<Integer> targetTypes;
    Logger logger;

    Writer(List<Integer> targetTypes, Logger logger){
        this.targetTypes=targetTypes;
        this.logger=logger;
    }

    public abstract void writeRow(Object[] row);

    public abstract void close();
    
}
