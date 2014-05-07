package it.cybion.snapshots.mapper;

import it.cybion.communityInteraction.landingArea.util.MapWritableArrayWritable;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class LanguageMapper extends Mapper<LongWritable, MapWritableArrayWritable, Text, IntWritable> {

        private static final IntWritable one = new IntWritable(1);
                
        public void map(LongWritable key, MapWritableArrayWritable value, Context context)
                        throws IOException, InterruptedException {
                
                Writable[] snapshots = (Writable[]) value.get();
                Text lang;
                MapWritable snapshot, user;
                for(int i=0; i<snapshots.length; i++){
                        snapshot = (MapWritable) snapshots[i];
                        user = (MapWritable) snapshot.get(new Text("user"));
                        lang = (Text) user.get(new Text("lang"));
                        context.write(lang, one);
                }
        }
}

