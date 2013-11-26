package it.cybion.snapshots.mapper;

import it.cybion.communityInteraction.landingArea.util.MapWritableArrayWritable;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class UserMapper extends Mapper<LongWritable, MapWritableArrayWritable, LongWritable, IntWritable> {

                
        public void map(LongWritable key, MapWritableArrayWritable value, Context context)
                        throws IOException, InterruptedException {
                
                Writable[] snapshots = (Writable[]) value.get();
                context.write(key, new IntWritable(snapshots.length));
        }
}