/* App.java */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import scala.Function0;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import java.util.Arrays;
import java.util.List;
import scala.Tuple2;

public class App {
  public static void main(String[] args) {
    
    if(args.length < 1){
     System.err.println("Usage: App <filename>");
     System.exit(1);
    }

    SparkConf conf = new SparkConf()
		.setAppName("Spark App");

    //Simple Word count   
	JavaSparkContext context = new JavaSparkContext(conf);

	JavaRDD<String> file = 	context.textFile(args[0]);
	
	//map
	JavaRDD<String> words = file.flatMap(
		new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String line) {
				return Arrays.asList(line.split(" "));
			}
		}
	);
	
	JavaPairRDD<String, Integer> pairs = words.mapToPair(
		new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(String word) {
				return new Tuple2<String, Integer>(word, 1);
			}
		}
	);
	
	//group and reduce
	JavaPairRDD<String, Integer> counts = pairs.reduceByKey(
		new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer a, Integer b) { 
				return a + b; 
			}
		}
	);
	
	List<Tuple2<String, Integer>> list1 = pairs.collect();
	List<Tuple2<String, Integer>> list = counts.collect();
	
	System.out.println("BEFORE REDUCE");
	for(Tuple2<String, Integer> item : list1){
		System.out.println(item.toString());
	}
	
	System.out.println("AFTER REDUCE");
	for(Tuple2<String, Integer> item : list){
		System.out.println(item.toString());
	}
  }
}
