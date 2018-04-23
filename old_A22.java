package ml;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.*;
import java.util.*;
import java.io.Serializable;
import java.util.Comparator;

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
													System.out.println("phase1 ing!");
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

			JavaPairRDD<String, String> phase2 = phase1.reduceByKey((x,y) -> x+"##"+y);

		/*	JavaPairRDD<String, String> phase3 = phase2.mapToPair(x ->
				{
					String[] values = x._2.split("##");
					int tmp = values.length;
					String tmpS = String.valueOf(tmp);
					if(tmp >=2)
					{
							return	new Tuple2<String,String>(x._1,"-G-"+x._2);
					}
					else
					{
							return	new Tuple2<String,String>(x._1,"-F-"+x._2);
					}

				//	return	new Tuple2<String,String>("1","2");
				}
			);*/

			//find the RDD which dates are more than two
			JavaPairRDD<String, String> phase3 = phase2.filter(new Function<Tuple2<String,String>,Boolean>(){

			            private static final long serialVersionUID = 1L;

			            @Override
			            public Boolean call(Tuple2<String,String> x) throws Exception {
			                // TODO Auto-generated method stub
								String[] values = x._2.split("##");
								int tmp = values.length;
								//String tmpS = String.valueOf(tmp);
			                return tmp>=2?true:false;
			            }
			        });

							JavaPairRDD<String, String> phase4 = phase3.mapToPair(s ->
								{
									String[] values = s._2.split("##");
									int tmp = values.length;
									if(tmp>2)
									{
										String inputPart2 = values[0]+"##"+values[1];
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
									/*java.text.DecimalFormat df = new java.text.DecimalFormat("#.0");*/
									String[] values = s._2.split("##");
									Double per = Double.parseDouble( values[1].substring(11) )/
																Double.parseDouble( values[0].substring(11) )*100;
								/*	String  finalPart2 = df.format(per)+"%";*/

									return	new Tuple2<String, Double>(s._1,per);
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

								JavaPairRDD<String, Tuple2<String,Double>> phase7 = phase6.mapToPair(s ->
									{
										/*java.text.DecimalFormat df = new java.text.DecimalFormat("#.0");*/
										String[] values = s._1.split("---");


										return	new Tuple2<String, Tuple2<String,Double>>(
                                values[1],new Tuple2<String, Double>(values[0], s
                                        ._2()));
									}
								).sortByKey(true);


								//successful!
								/*JavaPairRDD< Tuple2<Tuple2<String,String>,Double>,Double > phase8 = phase7.mapToPair(s ->

								{
									return	new Tuple2< Tuple2<Tuple2<String,String>,Double>,Double >(
															new Tuple2<Tuple2<String,String>, Double>(new Tuple2<String, String>(s._1, s
																			._2._1)
																			, s._2._2)
																			, s._2._2);
								}
								);*/

								//successful!
								JavaRDD<Tuple2<Tuple2<String,String>,Double>> phase8 = phase7.map(s ->
								{
										return new Tuple2<Tuple2<String,String>,Double>(
											new Tuple2<String,String>(
											s._1,s._2._1
											),s._2._2
										);
								}


								);

								JavaRDD<Tuple2<Tuple2<String,String>,Double>> phase9 = phase8.sortBy(._2,false).collect();

								/*JavaPairRDD<Tuple<Tuple2<String,String>, Double>,Double> phase8 = phase7.mapToPair(s ->
									{

										String tmp1 = s._1+s._2._1;
										Double tmp2 = s._2._2;


										return	new Tuple2<Tuple2<String,String>, Double>(
																new Tuple2<String, String>(s._1, s
																				._2._1()) , tmp2);
									}
								);



								 class Comp implements Comparator<Double>,Serializable{
							    @Override
							    public int compare(Double o1s, Double o2s) {

							          if(o1s.compareTo(o2s) == 0)
							              return o1s.compareTo(o2s);
							           else
							                return -o1s.compareTo(o2s);
							  }
							}
							JavaPairRDD<String,Integer> phase9 = phase8.sortByKey(new Comp());*/


				/*				JavaPairRDD<String,Double> phase8 = phase7.sortByKey(new Comparator<Tuple2<String,Double>> implements Serializable {
								    @Override
								    public int compare(Tuple2<String,Double> o1s, Tuple2<String,Double> o2s) {

								        if(o1s._1.compareTo(o2s._2) == 0)
								              return o1s._1.compareTo(o2s._2);
								        else
								              return -o1s._1.compareTo(o2s._2);
								  }
								});*/
							/*	JavaPairRDD<String, Tuple2<String,Double>> phase8 = phase7.mapToPair(s ->
									{

										String[] values = s._1.split("---");


										return	new Tuple2<String, Tuple2<String,Double>>(
																values[1],new Tuple2<String, Double>(values[0], s
																				._2()));
									}
								);*/

	    phase8.saveAsTextFile(outputDataPath);
	    sc.close();
	  }
}
