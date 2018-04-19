package usertag;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
/**
 * 
 * This does similar thing to TagMapper, except that
 * we make the map output value format exactly the same as
 * reduce output value format. So that we can use the reducer as combiner
 *   
 * input record format
 * 2048252769	48889082718@N01	dog francis lab	2007-11-19 17:49:49	RRBihiubApl0OjTtWA	16
 * 
 * output key value pairs for the above input
 * dog -> 48889082718@N01=1,
 * francis -> 48889082718@N01=1,
 * 
 * 
 * @see TagSmartReducer
 * @see TagSmartDriver
 * @author Ying Zhou
 *
 */
public class TagSmartMapper extends Mapper<Object, Text, Text, Text> {
	private Text word = new Text(),owner = new Text();
	static CharsetEncoder asciiEncoder = Charset.forName("US-ASCII").newEncoder(); 
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		String[] dataArray = value.toString().split("\t"); //split the data into array
		if (dataArray.length < 5){ //  record with incomplete data
			return; // don't emit anything
		}
		String tagString = dataArray[2];
		String ownerString = dataArray[1];
		if (tagString.length() > 0){
			String[] tagArray = tagString.split(" ");
			for(String tag: tagArray) {
				if (asciiEncoder.canEncode(tag)){
					word.set(tag);
					owner.set(ownerString+"=1,");
					context.write(word, owner);
				}
			}
		}
	}
}
