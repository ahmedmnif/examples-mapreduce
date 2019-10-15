package linecount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LineCountMapper extends Mapper<Object, Text, Text, IntWritable> {


	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		
		if (line != null && !line.isEmpty() ) {
			context.write(new Text("Nombre de lignes :"), new IntWritable(1));
		}
	}
}
