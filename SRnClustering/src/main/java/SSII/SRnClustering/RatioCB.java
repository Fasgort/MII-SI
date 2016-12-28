package SSII.SRnClustering;

import org.apache.spark.sql.Row;
import scala.Tuple2;

public class RatioCB {

	private Long item1;
	private Long item2;
	private Integer value;

	public RatioCB(Tuple2<Row, Integer> x) {
		this.item1 = x._1().getLong(0);
		this.item2 = x._1().getLong(1);
		this.value = x._2();
	}

	public String toString() {
		return "[" + item1 + " - " + item2 + ": " + value + "]";
	}

	public Long getItem1() {
		return item1;
	}

	public void setItem1(Long item1) {
		this.item1 = item1;
	}

	public Long getItem2() {
		return item2;
	}

	public void setItem2(Long item2) {
		this.item2 = item2;
	}

	public Integer getValue() {
		return value;
	}

	public void setValue(Integer value) {
		this.value = value;
	}

}