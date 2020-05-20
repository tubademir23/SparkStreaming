package kafka;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class StreamingConsumer {

	public static void main(String[] args) throws StreamingQueryException {
		System.setProperty("hadoop.home.dir", "D:\\hadoop-3.0.0");
		SparkSession sparkSession = SparkSession.builder()
				.appName("SparkStreamingConsumer").master("local")
				.getOrCreate();
		StructType schema = new StructType().add("product", "string")
				.add("time", DataTypes.TimestampType);
		// kafka server bootstrap important

		Dataset<Row> data_load = sparkSession.readStream().format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", SearchProductModel.TOPIC_NAME).load();

		Dataset<SearchProductModel> ds = data_load
				.selectExpr("CAST(value AS STRING) as message")
				.select(functions.from_json(functions.col("message"), schema)
						.as("json"))
				.select("json.*").as(Encoders.bean(SearchProductModel.class));

		Dataset<Row> windowedCounts = ds
				.groupBy(functions.window(ds.col("time"), "1 minute"),
						ds.col("product"))
				.count();

		StreamingQuery query = windowedCounts.writeStream()
				.outputMode("complete").format("console").start();
		try {
			query.awaitTermination();
		} catch (StreamingQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
