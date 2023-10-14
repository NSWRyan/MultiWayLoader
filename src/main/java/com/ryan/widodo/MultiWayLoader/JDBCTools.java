package com.ryan.widodo.MultiWayLoader;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.BaseTypeBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.logging.log4j.Logger;
import org.apache.orc.TypeDescription;

import com.ryan.widodo.MultiWayLoader.Writer.TableMeta;

public class JDBCTools {
    /**
     * Convert exception to stacktrace string.
     * 
     * @param exception : the exception.
     */
    public static String getExceptionPrintStack(Exception exception) {
        StringWriter sw = new StringWriter();
        exception.printStackTrace(new PrintWriter(sw, true));
        return sw.toString();
    }

    /**
     * Print exception
     */
    public static void printException(Logger logger, Exception exception) {
        logger.error("Exception caught: " + exception.getMessage());
        logger.error(JDBCTools.getExceptionPrintStack(exception));
        // if(exception instanceof SQLException){
        //     SQLException nException;
        //     while(((nException=(SQLException)exception).getNextException())!=null){
        //         logger.error(nException.getMessage());
        //     }
        // }
    }

    /**
     * Convert JDBC to Avro Schema
     * 
     * @param resultSetMetaData = the ResultSetMetaData.
     * @return org.apache.avro.Schema
     * @throws SQLException
     */
    public static Schema tableMetaToAvroSchema(TableMeta tableMeta){
        FieldAssembler<Schema> fieldAssembler = SchemaBuilder.builder().record(tableMeta.tableName.replaceAll("\\.", "__")).fields();
        
        int colCount = tableMeta.columnName.size();
        for (int i = 0; i < colCount; i++) {
            fieldAssembler.name(tableMeta.columnName.get(i))
                    .type(jdbcToAvroType(tableMeta.columnType.get(i), fieldAssembler,
                            tableMeta.columnNullable.get(i)))
                    .noDefault();
        }
        return fieldAssembler.endRecord();
    }

    /**
     * Convert {@link java.sql.Types} to Avro Schema
     * 
     * @param JDBCType       = the int type from {@link java.sql.Types}
     * @param fieldAssembler = Avro FieldAssembler
     * @param nullable       = from ResultSetMetaData isNullable
     * @return
     */
    private static Schema jdbcToAvroType(int JDBCType, FieldAssembler<Schema> fieldAssembler, int nullable) {
        BaseTypeBuilder<Schema> baseTypeBuilder;
        if (nullable == ResultSetMetaData.columnNullable) {
            baseTypeBuilder = SchemaBuilder.builder().nullable();
        } else {
            baseTypeBuilder = SchemaBuilder.builder();
        }
        switch (JDBCType) {
            case java.sql.Types.SMALLINT:
            case java.sql.Types.INTEGER:
                return baseTypeBuilder.intType();
            case java.sql.Types.DATE:
            case java.sql.Types.BIGINT:
                return baseTypeBuilder.longType();
            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
                return baseTypeBuilder.stringType();
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
            case java.sql.Types.FLOAT:
                return baseTypeBuilder.floatType();
            default:
                return baseTypeBuilder.stringType();
        }
    }

    public static TypeDescription tableMetaToTypeDescription(TableMeta tableMeta) {
        TypeDescription typeDescription = TypeDescription.createStruct();
        int colCount = tableMeta.columnName.size();
        for (int i = 0; i < colCount; i++) {
            jdbcTypeToTypeDescription(typeDescription, tableMeta.columnName.get(i), tableMeta.columnType.get(i));
        }
        return typeDescription;
    }

    /**
     * Convert {@link java.sql.Types} to Orc TypeDescription
     * 
     * @param JDBCType = the int type from {@link java.sql.Types}
     * @return
     */
    private static TypeDescription jdbcTypeToTypeDescription(TypeDescription typeDescription, String fieldName, int JDBCType) {
        switch (JDBCType) {
            case java.sql.Types.SMALLINT:
                typeDescription.addField(fieldName, TypeDescription.createShort());
                break;
            case java.sql.Types.INTEGER:
                typeDescription.addField(fieldName, TypeDescription.createInt());
                break;
            case java.sql.Types.BIGINT:
                typeDescription.addField(fieldName, TypeDescription.createLong());
                break;
            case java.sql.Types.DATE:
            case java.sql.Types.TIMESTAMP:
            case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
                typeDescription.addField(fieldName, TypeDescription.createTimestamp());
                break;
            case java.sql.Types.CHAR:
            case java.sql.Types.VARCHAR:
                typeDescription.addField(fieldName, TypeDescription.createString());
                break;
            case java.sql.Types.NUMERIC:
            case java.sql.Types.DECIMAL:
            case java.sql.Types.DOUBLE:
                typeDescription.addField(fieldName, TypeDescription.createDouble());
                break;
            case java.sql.Types.FLOAT:
                typeDescription.addField(fieldName, TypeDescription.createFloat());
                break;
            default:
                typeDescription.addField(fieldName, TypeDescription.createString());
                break;
        }
        return typeDescription;
    }

    /**
     * Sleep this thread for some ms.
     * 
     * @param ms duration of sleep in millisecond(s).
     */
    public static void sleepMS(Logger logger, long ms) {
        try {
            Thread.sleep(ms);
        } catch (Exception e) {
            JDBCTools.printException(logger, e);
            System.exit(4);
        }
    }
}
