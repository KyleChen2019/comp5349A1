package usertag;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * Input record format
 * dog -> {(48889082718@N01=1,48889082718@N01=3,), 423249@N01=4,}
 *
 * Output for the above input key valueList
 * dog -> 48889082718@N01=4,3423249@N01=4,
 * 
 * @see TagSmartMapper
 * @see TagSmartDriver
 * 
 * @author Ying Zhou
 *
 */
public class TagSmartReducer extends Reducer<Text, Text, Text, Text> {
	Text result = new Text();
	
	public void reduce(Text key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {

		// create a map to remember the owner frequency
		// keyed on owner id
		Map<String, Integer> ownerFrequency = new HashMap<String,Integer>();
		
		for (Text text: values){
			String dataString = text.toString();
			
			// each value may contains several owner's data
			// separated by "," after the combiner
			// dataArray stores ownData in the format of
			// ownerName = freq
			
			String[] dataArray = dataString.split(",");
			for (int i = 0; i < dataArray.length; i++){
				String[] ownerData =dataArray[i].split("=");

				String ownerId = ownerData[0];
				try{
					int ownerCount = Integer.parseInt(ownerData[1]);
					if (ownerFrequency.containsKey(ownerId)){
						ownerFrequency.put(ownerId, ownerFrequency.get(ownerId) +ownerCount);
					}else{
						ownerFrequency.put(ownerId, ownerCount);
					}
				}catch (NumberFormatException e){
					System.out.println(text.toString());
				}
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
