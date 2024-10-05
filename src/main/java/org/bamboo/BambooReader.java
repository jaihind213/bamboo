package org.bamboo;

import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.*;

import static org.apache.logging.log4j.LogManager.getLogger;

public class BambooReader {

    private static final Logger logger = getLogger(BambooReader.class.getName());
    private final SparkSession spark;
    private final Set<String> columnsToRead = new HashSet<>();

    BambooReader(SparkSession spark) {
        this.spark = spark;
    }


    public BambooReader format(String format) {
        //NO-OP, Bamboo can self detect the format
        return this;
    }

    public BambooReader columns(String... columnNames) {
        for(String column : columnNames){
            columnsToRead.add(column.toLowerCase());
        }
        return this;
    }

    public Dataset<Row> load(String path) {
        Dataset<Row> metaDataDf =  spark.read().format("json").load(path);
        if(logger.isDebugEnabled()){
            metaDataDf.show(1000, false);
        }
        Map<String, ColumnFamily> columnFamiliesFromMeta = getColumnFamiliesFromMetaData(metaDataDf);
        final boolean hasZionCf = columnFamiliesFromMeta.containsKey(Constants.ZION_CF);
        Map<String, ColumnFamily> columnFamiliesToLoad = new HashMap<>();


        if(columnsToRead.isEmpty()){
            logger.warn("No columns specified to be read = select * from ..,");
            if(hasZionCf){
                columnFamiliesToLoad.put(Constants.ZION_CF, columnFamiliesFromMeta.get(Constants.ZION_CF));
            } else {
                columnFamiliesToLoad = columnFamiliesFromMeta;
            }
        } else {
            Map<String,ColumnFamily> nonZionColumnFamiliesToLoad = canNonZionColumnFamiliesToSatisfyColumnsTobeRead(columnFamiliesFromMeta);
            if(nonZionColumnFamiliesToLoad.isEmpty()){
                if(hasZionCf){
                    columnFamiliesToLoad.put(Constants.ZION_CF, columnFamiliesFromMeta.get(Constants.ZION_CF));
                } else {
                    logger.warn("No column families contain the columns to be read. ColumnsToRead: {}.", columnsToRead);
                    throw new IllegalArgumentException("column families cant satisfy the columns to be read.");
                }
            } else {
                if(nonZionColumnFamiliesToLoad.size() > 1){
                    logger.warn("We have detected that multiple non-zion column families need to be loaded, to satisfy the columns to be read, which is not optimal."
                            + "Hence, We will load the zion column family to satisfy the query.");
                    columnFamiliesToLoad.put(Constants.ZION_CF, columnFamiliesFromMeta.get(Constants.ZION_CF));
                }else{
                    logger.info("sweet! We have detected that only one non-zion column family needs to be loaded, to satisfy the columns to be read.");
                    columnFamiliesToLoad = nonZionColumnFamiliesToLoad;
                }
            }
        }

        Dataset<Row> result = null;
        List<ColumnFamily> families = new ArrayList<>(columnFamiliesToLoad.values());
        result = spark.read().format(families.get(0).getFormat())
                .load(families.get(0).getPath());
        //todo: log.debug
        logger.info("Loaded column family: {} from path: {}", families.get(0).getName(), families.get(0).getPath());

        for (int i = 1; i < families.size(); i++) {
            Dataset<Row> columnFamilyDf = spark.read().format(families.get(i).getFormat())
                    .load(families.get(i).getPath());
            result = result.join(columnFamilyDf, Constants.ROW_ID);
            //todo: log.debug
            logger.info("Loaded column family: {} from path: {}", families.get(i).getName(), families.get(i).getPath());
        }
        return result.drop(Constants.ROW_ID);
    }

    private Map<String, ColumnFamily> getColumnFamiliesFromMetaData(Dataset<Row> metaData){
        Map<String, ColumnFamily> columnFamilies = new HashMap<>();
        List<Row> rows = metaData.collectAsList();
        for(Row row : rows) {
            String columnFamilyPath = row.getAs(Constants.PATH);
            String columnFamilyName = row.getAs(Constants.COLUMN_FAMILY_NAME);
            String format = row.getAs(Constants.FORMAT);
            List<String> columns =
                    scala.collection.JavaConverters.seqAsJavaList(row.getAs(Constants.COLUMNS));
            ColumnFamily columnFamily = new ColumnFamily(columnFamilyName, columns, format, columnFamilyPath);
            columnFamilies.put(columnFamilyName, columnFamily);
        }
        return columnFamilies;
    }

    private Map<String, ColumnFamily> canNonZionColumnFamiliesToSatisfyColumnsTobeRead(Map<String, ColumnFamily> columnFamiliesFromMeta){
        Map<String, ColumnFamily> columnFamiliesToLoad = new HashMap<>();
        int numColsFound = 0;
        for(String columnToRead : columnsToRead){
            boolean found = false;
            for(ColumnFamily cf : columnFamiliesFromMeta.values()){
                if(!Utils.isZionColumnFamily(cf) && cf.getColumnNames().contains(columnToRead)){
                    columnFamiliesToLoad.put(cf.getName(), cf);
                    numColsFound++;
                    found = true;
                    break;
                }
            }
            if(!found){
                logger.warn("Column: {} to be read is not present in any of the non-zion column families.", columnToRead);
            }
        }
        if(numColsFound == columnsToRead.size()){
            return columnFamiliesToLoad;
        }
        logger.warn("Some columns to be read are not present in the column families. ColumnsToRead: {}, ColumnsFThatCanBeLoaded: {}.", columnsToRead, columnFamiliesToLoad.keySet());
        return new HashMap<>();
    }
}