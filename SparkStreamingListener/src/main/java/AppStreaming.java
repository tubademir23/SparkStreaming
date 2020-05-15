import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class AppStreaming {
	// These sample analysis live time on messageing system and count them

	public static void main(String[] args) throws StreamingQueryException {
		// TODO Auto-generated method stub
		// System.setProperty("hadoop.home.dir", "D:\\hadoop-3.0.0");
		SparkSession sparkSession = SparkSession.builder().appName("SparkStreaming").master("local").getOrCreate();

		// listening on port so we nead socket
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
		StreamingQuery query = groupData.writeStream().outputMode("complete").format("console").start();
		query.awaitTermination();

	}

}
