package com.test.app;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.sql.*;
import java.util.concurrent.TimeoutException;

public class StreamReadExample {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        // create a local SparkSession
        SparkConf conf = new SparkConf().setAppName("readExample")
                .setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);
        SparkSession spark = new SparkSession(context.sc());
//        spark.sparkContext().setLogLevel("OFF");
        // define the schema of the source collection
        StructType readSchema = new StructType()
                .add("_id", DataTypes.StringType)
                .add("country_code", DataTypes.StringType)
                .add("country_id", DataTypes.DoubleType)
                .add("id", DataTypes.DoubleType)
                .add("latitude", DataTypes.StringType)
                .add("longitude", DataTypes.StringType)
                .add("name", DataTypes.StringType)
                .add("state_code", DataTypes.StringType)
                .add("state_id", DataTypes.DoubleType);

        Dataset<Row> rows = spark.readStream()
                .format("mongodb")
                .option("spark.mongodb.connection.uri", "mongodb://root:rootROOT@10.0.22.186:27017")
                .option("spark.mongodb.database", "braavos")
                .option("spark.mongodb.collection", "cities")
                .option("spark.mongodb.change.stream.publish.full.document.only","true")
                .load();
        final long[] start_time = {System.currentTimeMillis()};

        final long[] end_time = new long[1];
        final long[] difference = {0};
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
                        String _id = value.getAs("_id");
                        System.out.println(_id);
                        String country_code = value.getAs("country_code");
                        Double country_id = value.getAs("country_id");
                        Double id = value.getAs("id");
                        String latitude = value.getAs("latitude");
                        String longitude = value.getAs("longitude");
                        String name = value.getAs("name");
                        String state_code = value.getAs("state_code");
                        Double state_id = value.getAs("state_id");
                        count[0]++;
                        if(count[0]==1)
                        {
                            start_time[0] = System.currentTimeMillis();
                            System.out.println("\nSTART TIME: " + start_time[0] + "\n");

                        }
                        if(count[0]%10000==0) {
                            end_time[0] = System.currentTimeMillis();
                            difference[0] = end_time[0] - start_time[0];
                            System.out.println("\n"+count[0]+"TIME: " + difference[0]/1000 + "s\n");
                        }
                        if(count[0]==100000){
                            System.out.println("\nEND TIME: " + difference[0]/1000 + "s\n");
                        }
                        String insertSql = String.format("insert into test(_id,country_code,country_id,id,latitude,longitude,name,state_code,state_id) values('%s' , '%s' , %f , %f , '%s' , '%s' , '%s' , '%s' , %f)", _id, country_code, country_id, id, latitude, longitude, name, state_code, state_id);
                        System.out.println("\n\n\n\n\n\n\n\n"+insertSql+"\n\n\n\n\n\n\n\n");
                        try {
                            statement[0].execute(insertSql);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
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


         /** Script for Generating Sample Data
            async function insertRecords(numRecords) {
                var docs = [];
                for (var i = 0; i < numRecords; i++) {
                    var doc = {
                        "id": i,
                        "name": Math.random().toString(36).substring(2, 15),
                        "state_id":  Math.floor(Math.random() * (100000000000 - 0) + 0),
                        "state_code": Math.random().toString(36).substring(2, 4),
                        "country_id":  Math.floor(Math.random() * (100000000000 - 0) + 0),
                        "country_code": Math.random().toString(36).substring(2, 4),
                        "latitude": Math.random().toString(36).substring(2, 10),
                        "longitude": Math.random().toString(36).substring(2, 10)
                    }
                    docs.push(doc);
                };
                return await db.getCollection("test").insertMany(docs);
            }
          */

    }
}
