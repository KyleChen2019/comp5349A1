package usertag;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Input record format
 * dog -> {48889082718@N01, 48889082718@N01, 3423249@N01}
 *
 * Output for the above input key valueList
 * dog -> 48889082718@N01=2,3423249@N01=1,
 * 
 * @author Ying Zhou
 *
 */
public class TagReducer extends Reducer<Text, Text, Text, Text> {
	Text result = new Text();
	public void reduce(Text key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {

		// create a map to remember the owner frequency
		// keyed on owner id
		Map<String, Integer> ownerFrequency = new HashMap<String,Integer>();
		
		for (Text text: values){
			String ownerId = text.toString();
			if (ownerFrequency.containsKey(ownerId)){
				ownerFrequency.put(ownerId, ownerFrequency.get(ownerId) +1);
			}else{
				ownerFrequency.put(ownerId, 1);
			}
		}
		StringBuffer strBuf = new StringBuffer();
		for (String ownerId: ownerFrequency.keySet()){
			strBuf.append(ownerId + "="+ownerFrequency.get(ownerId)+",");
		}
		result.set(strBuf.toString());
		context.write(key, result);
	}
}
