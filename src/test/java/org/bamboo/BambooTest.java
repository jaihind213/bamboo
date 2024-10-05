package org.bamboo;


import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.storage.StorageLevel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BambooTest {

    @Before
    public void setUp() throws IOException {
        FileUtils.deleteDirectory(new File("/tmp/bamboo_farm"));
        FileUtils.forceMkdir(new File("/tmp/bamboo_farm"));
    }

    @Test
    public void testReadWriteSubsetOfColumns() throws Exception {
        Bamboo.setStorePath("/tmp/bamboo_farm");
        Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());

        try(SparkSession spark = SparkSession.builder().master("local").getOrCreate()){
            Dataset<Row> inputDataset = sampleDataset(SparkSession.builder().master("local").getOrCreate());
            inputDataset.show(false);

            Bamboo.write(spark, inputDataset)
                    .mode(SaveMode.Overwrite)
                    .format("parquet")
                    .columnFamily(new ColumnFamily("cf1", Arrays.asList("colA", "colB")))
                    .columnFamily(new ColumnFamily("cf2", Arrays.asList("colC")))
                    .save("/tmp/bamboo_input");

            Dataset<Row> inputReadByBamboo = Bamboo.read(spark)
                    .columns("colA", "colB")
                    .load("/tmp/bamboo_input");

            inputReadByBamboo.show(false);
            Assert.assertEquals(2, inputReadByBamboo.columns().length);
            Assert.assertEquals(2, inputReadByBamboo.count());
            Assert.assertEquals(1, inputReadByBamboo.where("colA = 'a0' and colB = 'b0'").count());
            Assert.assertEquals(1, inputReadByBamboo.where("colA = 'a1' and colB = 'b1'").count());
        }
        //have a look at /tmp/bamboo_input to see the output
    }

    @Test
    public void testReadWriteSubsetOfColumnsAcrossCf() throws Exception {
        Bamboo.setStorePath("/tmp/bamboo_farm");
        Bamboo.setWritingCacheStrategy(StorageLevel.MEMORY_ONLY());

        try(SparkSession spark = SparkSession.builder().master("local").getOrCreate()){
            Dataset<Row> inputDataset = sampleDataset(SparkSession.builder().master("local").getOrCreate());
            inputDataset.show(false);

            Bamboo.write(spark, inputDataset)
                    .mode(SaveMode.Overwrite)
                    .format("parquet")
                    .columnFamily(new ColumnFamily("cf1", Arrays.asList("colA", "colB")))
                    .columnFamily(new ColumnFamily("cf2", Arrays.asList("colC")))
                    .save("/tmp/bamboo_input2");

            Dataset<Row> inputReadByBamboo = Bamboo.read(spark)
                    .columns("colA", "colC")
                    .load("/tmp/bamboo_input2");

            //have a look at /tmp/bamboo_input2 to see the output

            inputReadByBamboo.show(false);
            Assert.assertEquals(3, inputReadByBamboo.columns().length);
            Assert.assertEquals(2, inputReadByBamboo.count());
            Assert.assertEquals(1, inputReadByBamboo.where("colA = 'a0' and colB = 'b0' and colC = 'c0'").count());
            Assert.assertEquals(1, inputReadByBamboo.where("colA = 'a1' and colB = 'b1' and colC = 'c1'").count());
        }
        //have a look at /tmp/bamboo_input to see the output
    }

    private static Dataset<Row> sampleDataset(SparkSession spark){
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            rows.add(RowFactory.create("a" + i, "b" + i, "c" + i));
        }

        return spark.createDataFrame(rows, DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("colA", DataTypes.StringType, false),
                DataTypes.createStructField("colB", DataTypes.StringType, false),
                DataTypes.createStructField("colC", DataTypes.StringType, false)})
        );
    }

}
