import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class AppStreaming {
	// These sample analysis live time on messageing system and count them
	public static final SparkSession sparkSession = SparkSession.builder().appName("SparkStreaming").master("local").getOrCreate();

	public static final String TIMESTAMP_COL_NAME = "timestamp";
	public static final String PRODUCT_COL_NAME = "product";
	public static final String TIMESTAMP_DUR_FUNC = "1 minute";

	public static void main(String[] args) throws StreamingQueryException {
		// System.setProperty("hadoop.home.dir", "D:\\hadoop-3.0.0");

		timeStampSample();
	}

	public static void sample() {
		// listening on port so we nead socket
		// use netcat application for test with below lines
		// nc -l localhost -p 8005
		Dataset<Row> rowData = sparkSession.readStream().format("socket").option("host", "localhost").option("port", "8005").load();

		Dataset<String> strData = rowData.as(Encoders.STRING());

		// String includes line so we have to split the line
		Dataset<String> strMapData = strData.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s) throws Exception {
				// s=hello how are you
				return Arrays.asList(s.split(" ")).iterator();
				// return {hello, how, are, your}
			}

		}, Encoders.STRING());

		// count of all words
		Dataset<Row> groupData = strMapData.groupBy("value").count();
		StreamingQuery query = groupData.writeStream().outputMode("update").format("console").start();
		try {
			query.awaitTermination();
		} catch (StreamingQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void timeStampSample() {
		Dataset<Row> rowData = sparkSession.readStream().format("socket").option("host", "localhost").option("port", "8005").option("includeTimeStamp", "true").load();

		// searching word-> XYZ then {XYZ, timestamp}
		Dataset<Row> productData = rowData.as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())).toDF(PRODUCT_COL_NAME, TIMESTAMP_COL_NAME);

		Dataset<Row> groupData = productData.groupBy(functions.window(productData.col(TIMESTAMP_COL_NAME), TIMESTAMP_DUR_FUNC), productData.col(PRODUCT_COL_NAME)).count().orderBy("window");

		// StreamingQuery query = groupData.writeStream().queryName("streamQuery").format("memory").outputMode("complete").start();
		// sparkSession.sql("select * from streamQuery").show();
		StreamingQuery query2 = groupData.writeStream().outputMode("complete").format("console").start();
		try {
			query2.awaitTermination();
		} catch (StreamingQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
