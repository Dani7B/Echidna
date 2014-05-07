package it.cybion.es.hadoop.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SnapshotExtractorMapper extends Mapper<Text, MapWritable, LongWritable, MapWritable> {
                
        public void map(Text key, MapWritable value, Context context)
                        throws IOException, InterruptedException {
                
                LongWritable userId = (LongWritable) value.get(new Text("id"));
                context.write(userId, value);
        }
}