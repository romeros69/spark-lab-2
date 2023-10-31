package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local")
                .getOrCreate();


        String path = "/home/roma/dev/itmo/tahbd/spark-lab-2/orc-parquet/test.csv";

        // Читаем сырые данные в DataFrame или Dataset
        Dataset<Row> rawDF = spark.read().format("csv").option("header", true)
                .load(path);

        rawDF.show();

        // Сохраняем в формате Parquet
        rawDF.write().mode(SaveMode.Overwrite)
                .parquet("/home/roma/dev/itmo/tahbd/spark-lab-2/orc-parquet/output_parquet");
//
        // Сохраняем в формате ORC
        rawDF.write().mode(SaveMode.Overwrite)
                .format("orc")
                .save("/home/roma/dev/itmo/tahbd/spark-lab-2/orc-parquet/output_orc");

        // Закрываем Spark сессию
        spark.stop();
    }
}