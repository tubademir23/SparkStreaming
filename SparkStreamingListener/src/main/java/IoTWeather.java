import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

public class IoTWeather {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\hadoop-3.0.0");
		SparkSession sparkSession = SparkSession.builder().appName("SparkIoTWeather").master("local").getOrCreate();

		StructType weatherType = new StructType().add("quarter", "string").add("heatType", "string").add("heat", "integer").add("windType", "string").add("wind", "integer");

		// listening on port so we nead socket
		Dataset<Row> rowData = sparkSession.readStream().schema(weatherType).option("sep", ",").csv("files//*");

		heatSample(rowData);

	}

	// be careful about outputmode=complete wants any aggregation op.
	public static void heatSample(Dataset<Row> rowData) {
		Dataset<Row> heatData = rowData.select("quarter").filter("heat>29");
		if (heatData.isStreaming())
			System.out.println("heatData is streaming...");
		heatData.printSchema();
		StreamingQuery query = heatData.writeStream().format("console").start();
		try {
			query.awaitTermination();
		} catch (StreamingQueryException e) { // TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void windSample(Dataset<Row> rowData) {
		Dataset<Row> windData = rowData.groupBy("windType").count();
		if (windData.isStreaming())
			System.out.println("windData is streaming...");
		windData.printSchema();
		StreamingQuery query = windData.writeStream().outputMode("complete").format("console").start();
		try {
			query.awaitTermination();
		} catch (StreamingQueryException e) { // TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
