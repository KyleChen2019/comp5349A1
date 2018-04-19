package usertag;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * input record format
 * 2048252769	48889082718@N01	dog francis lab	2007-11-19 17:49:49	RRBihiubApl0OjTtWA	16
 *
 * output key value pairs for the above input
 * dog -> 48889082718@N01
 * francis -> 48889082718@N01
 *
 * @author Ying Zhou
 *
 */
public class TagMapper extends Mapper<Object, Text, Text, Text> {

	private Text word = new Text(),owner = new Text();

	// a mechanism to filter out non ascii tags 使用Charset进行编码
	static CharsetEncoder asciiEncoder = Charset.forName("US-ASCII").newEncoder();

	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {

//	String[] dataArray = value.toString().split("(?!\"[^,]+),(?![^,]+\")"); //split the data into array
		String[] dataArray = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)",-1);

		//args[0],argus[1], testvalue is hardcode.
	//	String countryA="GB",countryB="US";

		Configuration conf = context.getConfiguration();
		String countryA= conf.get("countryAA");
		String countryB= conf.get("countryBB");
	//	conf.set("countryAA","GB");

		if (dataArray.length < 18){ //  record with incomplete data
			return; // don't emit anything
		}
		String tagString = dataArray[5]+dataArray[17];//category,country
		String ownerString = dataArray[0];//video_id

		if (tagString.length() > 0){
		//	String[] tagArray = tagString.split(" ");
//			for(String tag: tagArray) {
				if (asciiEncoder.canEncode(tagString)){
					if(dataArray[17].equals(countryA)||dataArray[17].equals(countryB))
					{
						word.set(tagString);
						owner.set(ownerString);
						context.write(word, owner);
					}
					else{}
				}
//			}
		}
	}
}
