package com.test.app;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.types.DataTypes.StringType;

public class CollectionMode {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .getOrCreate();
        spark.sparkContext().setLogLevel("OFF");

        ConnectionString connString = new ConnectionString(
                "mongodb://root:rootROOT@10.0.20.111:27017"
        );
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connString)
                .retryWrites(true)
                .build();
        MongoClient mongo = MongoClients.create(settings);


        //Retrieving the list of collections
        MongoIterable<String> dBList = mongo.listDatabaseNames();
        Map<String, List<String>> dbToCollectionsMap = new HashMap<>();
        List<StreamingQuery> queries = new ArrayList<>();
        for (String DBName : dBList) {
            MongoDatabase database = mongo.getDatabase(DBName);
            if (!(DBName.equalsIgnoreCase("admin") || DBName.equalsIgnoreCase("local") || DBName.equalsIgnoreCase("config"))) {
                MongoIterable<String> collectionList = database.listCollectionNames();
                for (String ColName : collectionList) {
                    final Statement[] statement = new Statement[1];
                    final Connection[] conn = new Connection[1];
                     queries.add(spark.readStream()
                            .format("mongodb")
                            .option("spark.mongodb.connection.uri", "mongodb://root:rootROOT@10.0.20.111:27017")
                            .option("spark.mongodb.database", DBName)
                            .option("spark.mongodb.collection", ColName)
                            .option("spark.mongodb.change.stream.publish.full.document.only", "true")
                            .load()
                            .writeStream()
//                            .foreach(new ForeachWriter<Row>() {
//                                @Override
//                                public boolean open(long partitionId, long epochId) {
//                                    try {
//                                        conn[0] = DriverManager.getConnection("jdbc:mysql://10.0.28.133/sparkSQL", "root", "rootROOT");
//                                        statement[0] = conn[0].createStatement();
//                                    } catch (SQLException e) {
//                                        throw new RuntimeException(e);
//                                    }
//                                    return true;
//                                }
//
//                                @Override
//                                public void process(Row value) {
//                                    Iterator<StructField> sf = value.schema().iterator();
//                                    String insertSql = "insert into "+ColName+" (";
//                                    while (sf.hasNext()) {
//                                        insertSql += sf.next().name() + " , ";
//                                    }
//                                    insertSql = insertSql.substring(0, insertSql.length() - 3);
//                                    insertSql += ") values(";
//                                    sf = value.schema().iterator();
//                                    int i = 0;
//                                    while (sf.hasNext()) {
//                                        DataType dtype = sf.next().dataType();
//                                        if (dtype == StringType)
//                                            insertSql += "'" + value.getAs(i) + "' , ";
//                                        else
//                                            insertSql += value.getAs(i) + " , ";
//                                        i++;
//                                    }
//                                    insertSql = insertSql.substring(0, insertSql.length() - 3);
//                                    insertSql += ")";
//                                    System.out.println(insertSql);
//                                    try {
//                                        statement[0].execute(insertSql);
//                                    } catch (SQLException e) {
//                                        throw new RuntimeException(e);
//                                    }
//                                }
//
//                                @Override
//                                public void close(Throwable errorOrNull) {
//                                    if (statement[0] != null) {
//                                        try {
//                                            statement[0].close();
//                                        } catch (SQLException e) {
//                                            throw new RuntimeException(e);
//                                        }
//                                    }
//                                    if (conn[0] != null) {
//                                        try {
//                                            conn[0].close();
//                                        } catch (SQLException e) {
//                                            throw new RuntimeException(e);
//                                        }
//                                    }
//                                }
//                            })
                            .format("json")
                             .option("path", "/Users/hevo/Documents/Hevo/spark-example/jsonFinalWrite/"+DBName+"_"+ColName)
                            .option("checkpointLocation", "/Users/hevo/Documents/Hevo/spark-example/checkpointDir/"+DBName+"_"+ColName)
//                            .trigger(Trigger.Continuous(5000))
                            .outputMode("append").queryName(DBName+"."+ColName).start());
                }
            }
        }
//        for(StreamingQuery query: queries){
//            System.out.println(query.name() + query.isActive());
//        }
        spark.streams().awaitAnyTermination();
        spark.stop();
    }
}
