package it.cybion.es.hadoop;

import it.cybion.es.hadoop.mapper.SnapshotExtractorMapper;
import it.cybion.es.hadoop.reducer.SnapshotExtractorReducer;
import it.cybion.communityInteraction.landingArea.util.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.elasticsearch.hadoop.mr.ESInputFormat;

/**
* @author Daniele Morgantini
*/
public class Extractor {
                
    public static void main(String[] args) throws HadoopException{
            
            /* Job for exporting snapshots */
            Configuration conf = new Configuration();
            conf.set("es.resource", "profiles/snapshot/_search?q=*:*"); // replace this with the relevant query
                
            try {
                        Job job = new Job(conf);
                 job.setInputFormatClass(ESInputFormat.class);
                 job.setJarByClass(Extractor.class);
                 job.setJobName("Snapshots export");
                 
                 job.setOutputFormatClass(SequenceFileOutputFormat.class);
                 FileOutputFormat.setOutputPath(job, new Path("snapshots"));
                 
                 job.setMapperClass(SnapshotExtractorMapper.class);
            job.setReducerClass(SnapshotExtractorReducer.class);
            
                 job.setMapOutputKeyClass(LongWritable.class);
                 job.setMapOutputValueClass(MapWritable.class);
                 
                 job.setOutputKeyClass(LongWritable.class);
                 job.setOutputValueClass(MapWritableArrayWritable.class);
        
         long startTime = System.currentTimeMillis();                 
                 job.waitForCompletion(true);
         System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
                
                } catch (IOException e) {
                        throw new HadoopException("Could not create job or completing it", e);
                } catch (ClassNotFoundException e) {
                         throw new HadoopException("Could not find ESInputFormat.class", e);
                } catch (InterruptedException e) {
                        throw new HadoopException("Been interrupted", e);
                }
	}

}
