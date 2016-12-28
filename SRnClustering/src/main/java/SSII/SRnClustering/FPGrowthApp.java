package SSII.SRnClustering;

import java.util.Arrays;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

public class FPGrowthApp {
	private static final String NAME = "JavaSQLMLlib";

	public static void main(String[] args) {

		double MIN_SUPPORT = 0.3;
		int NUM_PARTITIONS = 3;
		double MIN_CONFIDENCE = 0.95;

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
		Dataset<Row> completa = dataset.select(dataset.col("user_ID"), dataset.col("item_ID"));
		JavaRDD<Iterable<String>> aux = completa.javaRDD()
				.mapToPair(x -> new Tuple2<String, String>("" + x.getLong(0), "" + x.getLong(1)))
				.reduceByKey((z, y) -> (String) (z + " " + y)).map(x -> Arrays.asList(x._2.split(" ")));

		FPGrowth fp = new FPGrowth().setMinSupport(MIN_SUPPORT).setNumPartitions(NUM_PARTITIONS);
		FPGrowthModel<String> model = fp.run(aux);
		for (FPGrowth.FreqItemset<String> itemset : model.freqItemsets().toJavaRDD().collect()) {
			System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
		}

		AssociationRules arules = new AssociationRules().setMinConfidence(MIN_CONFIDENCE);
		JavaRDD<AssociationRules.Rule<String>> results = arules
				.run(new JavaRDD<>(model.freqItemsets(), model.freqItemsets().org$apache$spark$rdd$RDD$$evidence$1));
		for (AssociationRules.Rule<String> rule : results.collect()) {
			System.out.println(rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
		}

		ctx.stop();
		ctx.close();

	}
}
