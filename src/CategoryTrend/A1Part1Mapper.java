package CategoryTrend;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class A1Part1Mapper extends Mapper<Object, Text, Text, Text> {

	private Text word = new Text(),owner = new Text();

	// a mechanism to filter out non ascii tags 使用Charset进行编码
	static CharsetEncoder asciiEncoder = Charset.forName("US-ASCII").newEncoder();

	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {


		Configuration conf = context.getConfiguration();
		String countryA= conf.get("countryAA");
		String countryB= conf.get("countryBB");
		if(value.toString().substring(value.toString().length()-2).equals(countryA) ||
			value.toString().substring(value.toString().length()-2).equals(countryB)
		)
		{
			String[] dataArray = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1);
			if (dataArray.length < 18){ //  record with incomplete data
				return; // don't emit anything
			}
			String categoryString = dataArray[5];//category
			String countryVideoIdString = dataArray[17]+dataArray[0];//country+video_id

			if (categoryString.length() > 0){

					if (asciiEncoder.canEncode(categoryString)){
						if(dataArray[17].equals(countryA)||dataArray[17].equals(countryB))
						{
							word.set(categoryString);
							owner.set(countryVideoIdString);
							context.write(word, owner);
						}
						else{}
					}
			}
		}
	}
}
