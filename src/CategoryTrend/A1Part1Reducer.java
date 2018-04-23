package CategoryTrend;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class A1Part1Reducer extends Reducer<Text, Text, Text, Text> {
	Text result = new Text();
	public void reduce(Text key, Iterable<Text> values,
			Context context
	) throws IOException, InterruptedException {

		// create a map to remember the owner frequency
		// keyed on owner id
		Map<String, Integer> ownerFrequency = new HashMap<String,Integer>();
		Configuration conf = context.getConfiguration();
		String countryA= conf.get("countryAA");
		String countryB= conf.get("countryBB");
		ArrayList<String> listA = new ArrayList<String>();
		ArrayList<String> listB = new ArrayList<String>();
		Double percentage = 0.0;
		Double count = 0.0;

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
			int len = ownerId.length();
			if(ownerId.substring(0,2).equals(countryA))
			{
				listA.add(ownerId.substring(2));
			}
			else
			{
				listB.add(ownerId.substring(2));
			}
		}

		//when finish add to the list, match the ID from GB to US, if match->count++
		//percentage = count/ownerFrequency.size() *100

		Map<String, Integer> map = new HashMap<String,Integer>();
		Integer ifmatch;
		for(String string : listA)
		{
			map.put(string,0);
		}
		for(String string : listB)
		{
			ifmatch = map.get(string);
			if(null != ifmatch)
				{count++;}
		}
		java.text.DecimalFormat df = new java.text.DecimalFormat("#0.0");

			if(listA.size() == 0)
			{
				percentage = 0.0;
			}
			else
			{
			percentage = (count/listA.size())*100;
			}
		strBuf.insert(0,"; total: "+listA.size()+"; ");
		strBuf.append(df.format(percentage)+"% in "+countryB);

		result.set(strBuf.toString());
		context.write(key, result);
	}
}
