package org.bamboo;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.*;

import java.io.File;
import java.util.*;

public class BambooWriter {
    private final SparkSession spark;
    private final Set<String> srcDfColumnNames = new HashSet<>();
    private SaveMode saveMode = SaveMode.Overwrite;
    private Dataset<Row> data;
    private boolean writeZionCf = true;
    private final List<ColumnFamily> columnFamiliesToWrite = new ArrayList<>(10);
    private String format = "parquet";

    BambooWriter(SparkSession spark, Dataset<Row> data) {
        this.data = data;
        this.spark = spark;
        for(String column : data.columns()){
            srcDfColumnNames.add(column.toLowerCase());
        }
    }

    public BambooWriter mode(SaveMode saveMode) {
        this.saveMode = saveMode;
        return this;
    }

    public BambooWriter format(String format) {
        this.format = format;
        return this;
    }

    public BambooWriter writeZionCf(boolean writeZionCf) {
        this.writeZionCf = writeZionCf;
        return this;
    }

    public BambooWriter columnFamily(ColumnFamily columnFamily) {
        List<String> columnsInFamily = columnFamily.getColumnNames();
        for (String column : columnsInFamily) {
            if (!srcDfColumnNames.contains(column.toLowerCase())) {
                throw new IllegalArgumentException("Column " + column + " not found in the data.");
            }
        }
        List<String> cols = new ArrayList<>();
        for(String c: columnsInFamily){
            cols.add(c.toLowerCase());
        }
        cols.add(Constants.ROW_ID);
        columnFamiliesToWrite.add(new ColumnFamily(columnFamily.getName(), cols));
        return this;
    }

    public void save(String outputPath) {
        final String pathId = DigestUtils.md5Hex(outputPath);

        prepareColumnFamiliesForWriting(pathId);
        writeMetaData(outputPath, columnFamiliesToWrite);

        data = data.withColumn(Constants.ROW_ID, org.apache.spark.sql.functions.monotonically_increasing_id());
        data.persist(Bamboo.getWritingCacheStrategy());
        //data.show(false);
        final String viewName = "bamboo_" + pathId;
        data.createOrReplaceTempView(viewName);

        for (ColumnFamily cf : columnFamiliesToWrite) {
            String cols = String.join(",", cf.getColumnNames());
            Dataset<Row> columnFamilyDf = spark.sql("select " + cols + " from " + viewName);
            columnFamilyDf.write().format(format).mode(saveMode).save(cf.getPath());
        }
        data.unpersist();
    }


    private void prepareColumnFamiliesForWriting(String pathId){
        if (columnFamiliesToWrite.isEmpty()) {
            ColumnFamily zionCf = new ColumnFamily(Constants.ZION_CF, new ArrayList<>(srcDfColumnNames));
            columnFamiliesToWrite.add(zionCf);
        } else {
            if(writeZionCf){
                ColumnFamily zionCf = new ColumnFamily(Constants.ZION_CF, new ArrayList<>(srcDfColumnNames));
                columnFamiliesToWrite.add(zionCf);
            }
        }

        for(ColumnFamily cf : columnFamiliesToWrite){
            final String columnFamilyPath = Bamboo.getStorePath() + File.separator + pathId + File.separator + cf.getName();
            cf.setPath(columnFamilyPath);

            if(StringUtils.isBlank(cf.getFormat())){
                cf.setFormat(format);
            }
        }
    }

    private void writeMetaData(String outputPath, List<ColumnFamily> columnFamiliesToWrite){
        List<Row> rows = new ArrayList<>();
        for (ColumnFamily cf : columnFamiliesToWrite) {
            rows.add(RowFactory.create(cf.getPath(), cf.getFormat(), cf.getName(),
                    cf.getColumnNames().size() ,cf.getColumnNames().toArray()));
        }


        Dataset<Row> metaData = spark.createDataFrame(rows, Bamboo.getBambooMetaSchema());
        metaData.coalesce(1).write().mode(SaveMode.Overwrite).json(outputPath);
    }
}