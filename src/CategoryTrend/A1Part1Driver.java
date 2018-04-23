package CategoryTrend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class A1Part1Driver {

	public static void main(String[] args) throws Exception {
		//获得Configuration配置 Configuration: core-default.xml, core-site.xml
		Configuration conf = new Configuration();
		//获得输入参数 [hdfs://localhost:9000/user/dat/input,
		//hdfs://localhost:9000/user/dat/output]
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("Usage: TagDriver <in> <out> <CountryA> <CountryB>");
			System.exit(2);
		}
		String tmpCountryA = otherArgs[2];
		String tmpCountryB = otherArgs[3];

		conf.set("countryAA",tmpCountryA);
		conf.set("countryBB",tmpCountryB);
		//设置Job属性
		Job job = new Job(conf, "tag owner inverted list");
		job.setNumReduceTasks(1);
		job.setJarByClass(A1Part1Driver.class);
		job.setMapperClass(A1Part1Mapper.class);
		//将结果进行局部合并
		job.setReducerClass(A1Part1Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		TextInputFormat.addInputPath(job, new Path(otherArgs[0]));//传入input path
/* 		传入output path，输出路径应该为空，
		否则报错org.apache.hadoop.mapred.FileAlreadyExistsException */
		TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
