package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
public class MyBenchmark {

    private SparkSession spark;
    private Dataset<Row> rawDF;
    @Setup
    public void setup() {
        this.spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();

        String prefixPathToDataset = "/home/roma/dev/itmo/tahbd/spark-lab-2/orc-parquet/datasets";
        this.rawDF = this.spark.read().format("csv").option("header", true)
                .load(prefixPathToDataset + "/ds1kb.csv");
    }
    @Benchmark
    public void parquetBenchmark() {
        this.rawDF.write().mode(SaveMode.Overwrite)
                .parquet("/home/roma/dev/itmo/tahbd/spark-lab-2/orc-parquet/output_parquet/1kb");
    }

    @Benchmark
    public void orcBenchmark() {
        rawDF.write().mode(SaveMode.Overwrite)
                .format("orc")
                .save("/home/roma/dev/itmo/tahbd/spark-lab-2/orc-parquet/output_orc/1kb");
    }

    @TearDown
    public void tearDown() {
        this.spark.stop();
    }
}