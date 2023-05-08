package com.test.app;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import scala.reflect.ClassTag;

import java.sql.Timestamp;
import java.util.ArrayList;

public class OplogMode {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .getOrCreate();
        spark.sparkContext().setLogLevel("OFF");
        Dataset<Row> df = spark.read()
                .format("mongodb")
                .option("spark.mongodb.connection.uri", "mongodb://root:rootROOT@10.0.20.111:27017")
                .option("spark.mongodb.database", "local")
                .option("spark.mongodb.collection", "oplog.rs")
                .option("aggregation.pipeline","{" +
                            "$match:{" +
                                "ts:{" +
                                    "$gte: Timestamp(1682319965, 1)"+
                                "}," +
                                "op: \"i\"," +
                                "ns: \"braavos.cities\"" +
                            "}" +
                        "}")

                .load();
        StructType schema  = (StructType) df.select("o").schema().iterator().next().dataType();
        ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);
        Dataset<Row> df2 = df.map(new MapFunction<Row, Row>() {
            @Override
            public Row call(Row value) throws Exception {
                Row res = value.getAs("o");
                return res;
            }
        },encoder);

        df2.write().mode("append").json("./jsonSaveFiles");
//

        df2.write().format("jdbc")
                .option("url","jdbc:mysql://10.0.22.166/sparkSQL")
                .option("driver","com.mysql.cj.jdbc.Driver")
                .option("dbtable","cities")
                .mode("append")
                .option("user", "root")
                .option("password", "rootROOT")
                .save();
        spark.stop();
    }

}


