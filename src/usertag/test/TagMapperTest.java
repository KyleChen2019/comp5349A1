package usertag.test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import usertag.TagMapper;
/**
 * A simple unit test for TagMapper
 * it contains a single test method for valid record that 
 * emit one key value pair
 * @author Ying Zhou
 *
 */
public class TagMapperTest {
	@Test
	public void processValidRecord() throws IOException{
		TagMapper tagMapper = new TagMapper();
		Text input = new Text("509657344	7556490@N05	protest	2007-02-21 02:20:03	xbxI9VGYA5oZH8tLJA	14");
		//模拟Context的一个对象
		Mapper.Context context = mock(Mapper.Context.class);
		try{
		tagMapper.map(null, input, context);
		verify(context).write(new Text("protest"), new Text("7556490@N05"));
		}catch (Exception e){
			e.printStackTrace();
		}
	}

}
