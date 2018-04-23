package ViewNumberTrend;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.Serializable;
import java.util.Comparator;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;


public class TrendingOnViewNumber {

	public static void main(String[] args) {


	    String inputDataPath = args[0], outputDataPath = args[1];
	    SparkConf conf = new SparkConf();
	    conf.setAppName("A1_Spark_Impact_Of_Trending_On_View_Number");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    JavaRDD<String>  phase0 = sc.textFile(inputDataPath+"ALLvideos.csv");

	    JavaPairRDD<String, String> phase1 = phase0.mapToPair(s ->
	    	{
					try{
						String[] phase0Array = s.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1);
						if(phase0Array.length == 18)
						{
						if(phase0Array[0].equals("video_id"))
						{
													System.out.println("working on phase1!");
												return	new Tuple2<String, String>("error_firstLine","");
						}
						else{
			    		return	new Tuple2<String,String>(phase0Array[0]+"---"+phase0Array[17],phase0Array[1]+"---"+phase0Array[8]	);
							}
						}
						else{
																		return	new Tuple2<String,String>("error_length","");
						}
					}
					catch(Exception e)
					{
						System.out.println("error_phase1");
					return	new Tuple2<String,String>("error_phase1","");
					}
	    	}
	    );

			JavaPairRDD<String, String> phase2 = phase1.reduceByKey((x,y) -> x+"##"+y);

			//find the RDD which dates are more than two
			JavaPairRDD<String, String> phase3 = phase2.filter(new Function<Tuple2<String,String>,Boolean>(){

			            private static final long serialVersionUID = 1L;

			            @Override
			            public Boolean call(Tuple2<String,String> x) throws Exception {
			                // TODO Auto-generated method stub
								String[] phase2Array = x._2.split("##");
								int tmp = phase2Array.length;
			                return tmp>=2?true:false;
			            }
			        });

							JavaPairRDD<String, String> phase4 = phase3.mapToPair(s ->
								{
									String[] phase3Array = s._2.split("##");
									int tmpLength = phase3Array.length;
									if(tmpLength>2)
									{
										String inputPart2 = phase3Array[0]+"##"+phase3Array[1];
										return	new Tuple2<String, String>(s._1,inputPart2);
									}
									else
									{
										return s;
									}
								}
							);


							//18.26.01---493296##18.27.01---685038
							JavaPairRDD<String, Double> phase5 = phase4.mapToPair(s ->
								{
									java.text.DecimalFormat dfForAssignment1 = new java.text.DecimalFormat("#.0");
									String[] phase4Array = s._2.split("##");
									Double per = Double.parseDouble( phase4Array[1].substring(11) )/
																Double.parseDouble( phase4Array[0].substring(11) )*100;
									Double tmpPercentage = Double.valueOf( dfForAssignment1.format(per) );
									return	new Tuple2<String, Double>(s._1,tmpPercentage);
								}
							);

							JavaPairRDD<String, Double> phase6 = phase5.filter(new Function<Tuple2<String,Double>,Boolean>(){

							            private static final long serialVersionUID = 1L;

							            @Override
							            public Boolean call(Tuple2<String,Double> x) throws Exception {
							                // TODO Auto-generated method stub
							                return x._2>=1000?true:false;
							            }
							        });

								JavaPairRDD<Tuple2<String,Double>, String> phase7 = phase6.mapToPair(s ->
									{
										String[] phase6Array = s._1.split("---");
										String tmpCountry = phase6Array[1];
										String tmpId = phase6Array[0];
										return	new Tuple2<Tuple2<String,Double>,String>(
                                new Tuple2<String, Double>(tmpCountry, s
                                        ._2()),tmpId  );
									}
								);


							class comp implements Comparator<Tuple2<String,Double>>, Serializable{
						 public int compare(Tuple2<String,Double> x, Tuple2<String,Double> y) {
							 int check = x._1.compareTo(y._1); //country
							 if(check == 0)
									 {return -(x._2.compareTo(y._2)); }   //percentage
								else
										 return check;

						 }
					 }


					 JavaPairRDD< Tuple2<String,Double >, String> phase8 = phase7.sortByKey(new  comp(),true,1);


								//make JavaPairRDD to JavaRDD for output_format
								JavaRDD<String> phase9 = phase8.map(s ->
								{
									String tmpDouble = String.valueOf(s._1._2);
									String tmpOutPutValue = s._1._1+"; "+s._2+", "+tmpDouble+"%";
									return tmpOutPutValue;
								}
								);

	    phase9.saveAsTextFile(outputDataPath);
	    sc.close();
	  }
}
