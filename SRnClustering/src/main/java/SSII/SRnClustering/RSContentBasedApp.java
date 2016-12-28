package SSII.SRnClustering;

import java.util.Properties;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

public class RSContentBasedApp {
	private static final String NAME = "JavaSQLMLlib2";

	public static void main(String[] args) throws AnalysisException {

		String master = System.getProperty("spark.master");

		System.setProperty("hadoop.home.dir",
				System.getProperty("user.dir") + "/resources/winutil/".replaceAll("\\\\", "/"));
		System.setProperty("spark.sql.warehouse.dir",
				"file:///${System.getProperty(\"user.dir\")}/spark-warehouse".replaceAll("\\\\", "/"));

		JavaSparkContext ctx = new JavaSparkContext(SparkConfigs.create(NAME, master == null ? "local[*]" : master)
				.set("spark.sql.crossJoin.enabled", "true"));
		SQLContext sql = SQLContext.getOrCreate(ctx.sc());

		Properties properties = new Properties();
		properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");
		properties.setProperty("user", "root");
		properties.setProperty("password", "root");
		properties.setProperty("allowMultiQueries", "true");
		properties.setProperty("rewriteBatchedStatements", "true");

		Dataset<Row> dataset = sql.read().option("inferSchema", true).json("resources//rates.json");

		JavaRDD<RatioCB> aux = dataset.select(dataset.col("user_ID").as("user1"), dataset.col("item_ID").as("film1"))
				.join(dataset.select(dataset.col("user_ID").as("user2"), dataset.col("item_ID").as("film2")))
				.where("user1 = user2").where("film1 <> film2").select("film1", "film2").javaRDD()
				.mapToPair(x -> new Tuple2<Row, Integer>(x, 1)).reduceByKey((x, y) -> x + y).map(x -> new RatioCB(x));

		sql.createDataFrame(aux, RatioCB.class).write().json("output//ContentPrediction");

		ctx.stop();
		ctx.close();

	}
}
