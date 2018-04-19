package usertag.test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import usertag.TagReducer;

/**
 * A simple unit test for TagReducer
 * It contains a single test method.
 * It test the valid intermediate result with one owner occuring
 * twice in the value list.
 * 
 * @author Ying Zhou
 *
 */
public class TagReducerTest {
	@Test
	public void processValidRecord() throws IOException{
		
		TagReducer reducer = new TagReducer();
		
		Text key = new Text("protest");
		ArrayList<Text> values = new ArrayList<Text>();
		values.add(new Text("7556490@N05"));
		values.add(new Text("7556490@N05"));
		Iterable<Text> iterableValue = values;
		Reducer.Context mock = mock(Reducer.Context.class);
		Reducer<Text, Text, Text, Text>.Context context = mock;
		try{
		reducer.reduce(key, iterableValue, context);
		verify(context).write(key, new Text("7556490@N05=2,") );
		}catch (Exception e){
			e.printStackTrace();
		}
	}
}
