package com.test.app;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class streamoplog {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("multipleCollections")
                .getOrCreate();
        Dataset<Row> rows = spark
                .readStream()
                .format("mongodb")
                .option("spark.mongodb.connection.uri", "mongodb://root:rootROOT@10.0.16.20:27017")
                .option("spark.mongodb.database", "braavos")
                .option("spark.mongodb.collection", "cities")
                .option("spark.mongodb.change.stream.publish.full.document.only","true")
                .load();
        DataStreamWriter<Row> query = rows
                .writeStream()
                .format("console")
                .trigger(Trigger.Continuous(5000))
                .outputMode("append");

        StreamingQuery q = query.start();
        q.awaitTermination();
    }
}
