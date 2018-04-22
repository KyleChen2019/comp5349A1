package ml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import scala.Tuple3;
//import scala.*;
//import scala.Double;


public class MovieLensLarge {

	public static void main(String[] args) {

		//The program arguments are input and output path
		//Using absolute path is always preferred
		//For windows system, the path value should be something like "C:\\data\\ml-100k\\"
		//For unix system, the path value should something like "/home/user1/data/ml-100k/"
		//For HDFS, the path value should be something like "hdfs://localhost/user/abcd1234/movies/"

	    String inputDataPath = args[0], outputDataPath = args[1];
	    SparkConf conf = new SparkConf();

	    conf.setAppName("testA2");

	    JavaSparkContext sc = new JavaSparkContext(conf);

	    JavaRDD<String>  phase0 = sc.textFile(inputDataPath+"out1.csv");
			System.out.println("phase0_good");
					/*	for(String tmp : values)
						{							lt.add(tmp);						}
					JavaRDD<String> phase0 = js.parallelizePairs(lt);
					return
						new Tuple2<String>(values[1],Float.parseFloat(values[2]));*/


/*			JavaPairRDD<String> phase0 = initString.flatMapToPair(s->{
	    	String[] values = s.split("\n");
	    	int length = values.length;
	    	ArrayList<String> results = new ArrayList<String>();

	    	if (values.length >0 ){
	    		for (String tmp: values){
	    				results.add(tmp);
	    		}

					return results.iterator();
	    	}
	    	else
					return results.iterator();
	    });*/

			//JavaPairRDD<String, Tuple2<String,Float>>
	    JavaPairRDD<String, String> phase1 = phase0.mapToPair(s ->
	    	{

					try{
						String[] values = s.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1);
						if(values.length == 18)
						{
						if(values[0].equals("video_id"))
						{
													System.out.println("line0");
												return	new Tuple2<String, String>("line0","");
						}
						else{
			    		return	new Tuple2<String,String>(values[0]+"---"+values[17]+"   ",values[1]+"---"+values[8]	);
					//	return	new Tuple2<String, Float>("line22222",5.0f);
							}
						}
						else{
																		return	new Tuple2<String,String>("length_wrong","");
						}
					}
					catch(Exception e)
					{
						System.out.println("phase1_ERROR");
					return	new Tuple2<String,String>("phase1_error","");
					}
	    	}
	    );

			JavaPairRDD<String, String> phase2 = phase1.reduceByKey((x,y) -> x+"|"+y);

			JavaPairRDD<String, String> phase3 = phase2.mapToPair(x ->
				{
					String[] values = x._2.split("\|");
					int tmp = values.length;
					String tmpS = String.valueOf(tmp);
					if(tmp >=2)
					{
							return	new Tuple2<String,String>(x._1,"GG"+x._2);
					}
					else
					{
							return	new Tuple2<String,String>(x._1,"-F-"+x._2);
					}

				//	return	new Tuple2<String,String>("1","2");
				}
			);

	    phase3.saveAsTextFile(outputDataPath);
	    sc.close();
	  }
}
