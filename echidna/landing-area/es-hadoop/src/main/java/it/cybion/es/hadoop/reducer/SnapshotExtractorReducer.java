package it.cybion.es.hadoop.reducer;

import it.cybion.communityInteraction.landingArea.util.MapWritableArrayWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SnapshotExtractorReducer extends Reducer<LongWritable,MapWritable,LongWritable,MapWritableArrayWritable> {
        
        protected void reduce(LongWritable key, Iterable<MapWritable> values, Context context)
                        throws IOException, InterruptedException {
                
                MapWritableArrayWritable mw = new MapWritableArrayWritable();
                List<MapWritable> snapshots = new ArrayList<MapWritable>();
                for(MapWritable v : values) {
                        snapshots.add(v);
                }
                mw.set(snapshots.toArray(new MapWritable[0]));
                context.write(key, mw);
	}

}
