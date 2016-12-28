package SSII.SRnClustering;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Utils {
	public static void disableSparkLogging() {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
	}
}
