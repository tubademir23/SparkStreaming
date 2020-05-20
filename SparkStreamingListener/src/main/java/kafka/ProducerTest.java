package kafka;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.Gson;

public class ProducerTest implements Serializable {

	public static void main(String[] args) {
		Scanner read = new Scanner(System.in);
		String topic = SearchProductModel.TOPIC_NAME;

		Properties prop = new Properties();

		Gson gson = new Gson();
		prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.ByteArraySerializer");
		prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");

		Producer producer = new KafkaProducer<String, String>(prop);

		while (true) {
			System.out.println("giriþ yapýnýz: ");
			String product = read.nextLine();
			System.out.println("P :" + product);
			SearchProductModel model = new SearchProductModel();
			model.setProduct(product);
			String time = new SimpleDateFormat("yyyy-MM-d HH:mm:ss")
					.format(new Date());
			model.setTime(time);
			String strJson = gson.toJson(model);
			System.out.println(strJson);
			// first string refers to topic, and second refers to data
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(
					topic, strJson);
			producer.send(record);
		}

	}

}
