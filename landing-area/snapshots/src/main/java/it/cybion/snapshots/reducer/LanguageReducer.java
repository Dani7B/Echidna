package it.cybion.snapshots.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LanguageReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable totalCount = new IntWritable();
        
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                        throws IOException, InterruptedException {
                
                int count = 0;
                for(IntWritable v : values){
                        count += v.get();
                }
        totalCount.set(count);
                context.write(key, totalCount);
        }

}