package com.test.app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeoutException;

public class multiplecollectionsstreaming {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("multipleCollections")
                .getOrCreate();
        Dataset<Row> rows = spark
                .readStream()
                .format("mongodb")
                .option("spark.mongodb.connection.uri", "mongodb://root:rootROOT@10.0.17.114:27017")
                .option("spark.mongodb.database", "local")
                .option("spark.mongodb.collection", "oplog.rs")
                .option("spark.mongodb.change.stream.publish.full.document.only","true")
                .load();

//        rows.filter(String.valueOf(rows.col("op").equals("i")));
        final Statement[] statement = new Statement[1];
        final Connection[] conn = new Connection[1];
        final int[] count = {0};
        DataStreamWriter<Row> dataStreamWriter = rows
                .writeStream()
                .format("")
                .foreach(new ForeachWriter<Row>() {
                    @Override
                    public boolean open(long partitionId, long epochId) {
                        try {
                            conn[0] = DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","abcd1234");
                            statement[0] = conn[0].createStatement();
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                        return true;
                    }

                    @Override
                    public void process(Row value) {

//                        String insertSql = String.format("insert into test(_id,country_code,country_id,id,latitude,longitude,name,state_code,state_id) values('%s' , '%s' , %f , %f , '%s' , '%s' , '%s' , '%s' , %f)");
//                        System.out.println("\n\n\n\n\n\n\n\n"+insertSql+"\n\n\n\n\n\n\n\n");
//                        try {
//                            statement[0].execute(insertSql);
//                        } catch (SQLException e) {
//                            throw new RuntimeException(e);
//                        }
                    }

                    @Override
                    public void close(Throwable errorOrNull) {
                        if(statement[0]!=null) {
                            try {
                                statement[0].close();
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        if(conn[0]!=null) {
                            try {
                                conn[0].close();
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                })
                .option("checkpointLocation", "/Users/hevo/Documents/Hevo/spark-example/checkpointDir")
                .trigger(Trigger.Continuous(500))
                .outputMode("append");

        StreamingQuery query = dataStreamWriter.start();
        query.awaitTermination();
    }
}
