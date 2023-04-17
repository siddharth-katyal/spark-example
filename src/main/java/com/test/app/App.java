package com.test.app;


import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class App {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .getOrCreate();
        StructType readSchema = new StructType()
                .add("_id", DataTypes.StringType)
                .add("country_code", DataTypes.StringType)
                .add("country_id", DataTypes.DoubleType)
                .add("id", DataTypes.DoubleType)
                .add("latitude", DataTypes.DoubleType)
                .add("longitude", DataTypes.DoubleType)
                .add("name", DataTypes.StringType)
                .add("state_code", DataTypes.StringType)
                .add("state_id", DataTypes.DoubleType);
        Dataset<Row> df = spark.read()
                .format("mongodb")
                .option("spark.mongodb.connection.uri", "mongodb://root:rootROOT@10.0.27.225:27017")
                .option("spark.mongodb.database", "braavos")
                .option("spark.mongodb.collection", "cities")
                .schema(readSchema)
                .load();
        df.show(10);
        spark.stop();
    }

}
