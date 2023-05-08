//package com.test.app;
//
//import org.apache.spark.api.java.function.MapFunction;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Encoders;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
//import org.apache.spark.sql.catalyst.encoders.RowEncoder;
//import org.apache.spark.sql.types.StructType;
//import scala.Tuple2;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.PreparedStatement;
//import java.util.Iterator;
//import java.util.UUID;
//
//public class oplogtest {public static void main(String[] args) {
//    SparkSession spark = SparkSession.builder()
//            .master("local")
//            .appName("MongoSparkConnectorIntro")
//            .getOrCreate();
////    spark.sparkContext().setLogLevel("OFF");
//    Dataset<Row> oplogDF = spark.read()
//            .format("mongodb")
//            .option("spark.mongodb.connection.uri", "mongodb://root:rootROOT@10.0.20.111:27017")
//            .option("spark.mongodb.database", "local")
//            .option("spark.mongodb.collection", "oplog.rs")
//            .option("aggregation.pipeline","{" +
//                        "$match:{" +
//                            "ts:{ $gte: Timestamp(1682319965, 1)}," +
//                            "op: \"i\"" +
//                        "}," +
//                        "$project:{ns: 1, o: 1}}" +
//                    "}")
//
//            .load();
//    Dataset<Tuple2<String, String>> nsAndODF = oplogDF.select("ns", "o")
//            .map((Row row) -> new Tuple2<>(row.getString(0), row.getString(1)),
//                    Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
//
//    nsAndODF.foreachPartition((Iterator<Tuple2<String, String>> iterator) -> {
//        Class.forName("com.mysql.jdbc.Driver");
//        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "user", "password");
//
//        while (iterator.hasNext()) {
//            Tuple2<String, String> nsAndO = iterator.next();
//            String[] nsParts = nsAndO._1().split("\\.");
//            String databaseName = nsParts[0];
//            String collectionName = nsParts[1];
//
//            PreparedStatement preparedStatement = connection.prepareStatement(
//                    "INSERT INTO `" + databaseName + "`.`" + collectionName + "` (`_id`, `o`) VALUES (?, ?)"
//            );
//            preparedStatement.setObject(1, UUID.randomUUID().toString());
//            preparedStatement.setString(2, nsAndO._2());
//            preparedStatement.execute();
//        }
//
//        connection.close();
//    });
//
////    StructType schema  = (StructType) df.select("o").schema().iterator().next().dataType();
////    ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);
////    Dataset<Row> df2 = df.map(new MapFunction<Row, Row>() {
////        @Override
////        public Row call(Row value) throws Exception {
////            Row res = value.getAs("o");
////            return res;
////        }
////    },encoder);
////
////    df2.write().mode("append").json("./jsonSaveFiles");
//////
////
////    df2.write().format("jdbc")
////            .option("url","jdbc:mysql://10.0.22.166/sparkSQL")
////            .option("driver","com.mysql.cj.jdbc.Driver")
////            .option("dbtable","cities")
////            .mode("append")
////            .option("user", "root")
////            .option("password", "rootROOT")
////            .save();
//    spark.stop();
//}
//
//}
//
//
