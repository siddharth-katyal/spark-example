package com.test.app;


import org.apache.hadoop.shaded.net.minidev.json.JSONObject;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.lang.reflect.Array;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;
import static javax.ws.rs.client.Entity.json;
import static org.apache.spark.sql.functions.col;

public class App {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .getOrCreate();
        spark.sparkContext().setLogLevel("OFF");
        long ts = Timestamp.valueOf("2023-04-21 14:06:20").getTime();
        Dataset<Row> df = spark.read()
                .format("mongodb")
                .option("spark.mongodb.connection.uri", "mongodb://root:rootROOT@10.0.24.94:27017")
                .option("spark.mongodb.database", "local")
                .option("spark.mongodb.collection", "oplog.rs")
                .option("aggregation.pipeline","{" +
                            "$match:{" +
                                "ts:{" +
                                    "$gte: Timestamp(1682319965, 1)"+
                                "}," +
                                "op: \"i\"," +
                            "}" +
                        "}")

                .load();
        final String[] tempns = new String[1];
        final String[] tempId = new String[1];
        df.write().mode("append").json("./jsonSaveFiles");
        ArrayList<String> ns = new ArrayList<String>();
        ArrayList<String> objId = new ArrayList<String>();
        df.foreach(new ForeachFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                ns.add(row.getAs("ns"));
                GenericRowWithSchema obj = row.getAs("o");
                System.out.println(obj.schema());
                objId.add(obj.getAs("_id"));
                tempns[0] = row.getAs("ns");
                tempId[0] = obj.getAs("_id");
            }
        });
        for(int i=0;i<ns.size();i++){
            String[] dbcol = ns.get(i).split("\\.",2);
            System.out.println(dbcol[0] + " " + dbcol[1]+ " " + objId.get(i));
            Dataset<Row> df2 = spark.read()
                    .format("mongodb")
                    .option("spark.mongodb.connection.uri", "mongodb://root:rootROOT@10.0.28.111:27017")
                    .option("spark.mongodb.database", dbcol[0])
                    .option("spark.mongodb.collection", dbcol[1])
                    .option("aggregation.pipeline","{" +
                                "$match:{" +
                                    "_id:ObjectId('"+objId.get(i)+"')" +
                                "}" +
                            "}")

                    .load();
            df2.write().mode("append").json("./jsonFinalWrite");
        }
//        Dataset<Row> dfInsert = df.filter(df.col("op").equalTo("i"));
//        dfInsert.write().mode("append").json("./csvSaveFiles/testData.json");
//        Dataset<Row> dfUpdate = df.filter(df.col("op").equalTo("u"));
////        dfUpdate.show(10);
//        Dataset<Row> dfDelete = df.filter(df.col("op").equalTo("d"));
////        dfDelete.show(10);
//        Dataset<Row> df2 = spark.read()
//                .format("mongodb")
//                .option("spark.mongodb.connection.uri", "mongodb://root:rootROOT@10.0.28.58:27017")
//                .option("spark.mongodb.database", "braavos")
//                .option("spark.mongodb.collection", "cities")
//                .load().filter(col("id").equalTo("521"));
//        df2.show(10);
////       dfInsert.foreach(new ForeachFunction<Row>() {
////           @Override
////           public void call(Row row) throws Exception {
////               row.fieldIndex("")
////           }
////       });
        spark.stop();
    }

}
