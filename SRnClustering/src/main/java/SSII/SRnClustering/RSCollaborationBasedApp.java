package SSII.SRnClustering;

import java.util.Properties;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class RSCollaborationBasedApp {
	private static final String NAME = "JavaSQLMLlib";

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

		ALS als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("user_ID").setItemCol("item_ID")
				.setRatingCol("rating");
		ALSModel model = als.fit(dataset);

		Dataset<Row> completa = dataset.select(dataset.col("user_ID")).distinct()
				.join(dataset.select(dataset.col("item_ID")).distinct());
		completa = model.transform(completa);

		completa.write().json("output//CollabPrediction");

		ctx.stop();
		ctx.close();

	}
}
