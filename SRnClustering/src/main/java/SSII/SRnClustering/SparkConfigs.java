package SSII.SRnClustering;

import org.apache.spark.SparkConf;

public class SparkConfigs {
	static {
		Utils.disableSparkLogging();
	}

	public static SparkConf create(String name, String master) {
		return new SparkConf().setAppName(name).setMaster(master);
	}
}
