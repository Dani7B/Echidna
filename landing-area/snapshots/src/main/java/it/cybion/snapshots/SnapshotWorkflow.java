package it.cybion.snapshots;

import it.cybion.snapshots.mapper.LanguageMapper;
import it.cybion.snapshots.mapper.UserMapper;
import it.cybion.snapshots.reducer.LanguageReducer;
import it.cybion.snapshots.reducer.UserReducer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import it.cybion.communityInteraction.landingArea.util.HadoopException;

/**
* @author Daniele Morgantini
*/
public class SnapshotWorkflow {
                
    public static void main(String[] args) throws HadoopException{
            Configuration conf = new Configuration();
            
                try {
                        Job languageCount = new Job(conf);
                 languageCount.setInputFormatClass(SequenceFileInputFormat.class);
                 languageCount.setJarByClass(SnapshotWorkflow.class);
                 languageCount.setJobName("Language count");
                 
                 FileInputFormat.setInputPaths(languageCount, new Path("snapshots"));
                 FileOutputFormat.setOutputPath(languageCount, new Path("language-count"));
                 
                 languageCount.setMapperClass(LanguageMapper.class);
            languageCount.setReducerClass(LanguageReducer.class);
            languageCount.setCombinerClass(LanguageReducer.class);
            
                 languageCount.setMapOutputKeyClass(Text.class);
                 languageCount.setMapOutputValueClass(IntWritable.class);
                 
                 languageCount.setOutputKeyClass(Text.class);
                 languageCount.setOutputValueClass(IntWritable.class);
        
                 languageCount.waitForCompletion(true);
                 
                 Job snapshotCount = new Job(conf);
                 snapshotCount.setInputFormatClass(SequenceFileInputFormat.class);
                 snapshotCount.setJarByClass(SnapshotWorkflow.class);
                 snapshotCount.setJobName("Snapshot count");
                 
                 FileInputFormat.setInputPaths(snapshotCount, new Path("snapshots"));
                 FileOutputFormat.setOutputPath(snapshotCount, new Path("snapshot-count"));
                 
                 snapshotCount.setMapperClass(UserMapper.class);
                 snapshotCount.setReducerClass(UserReducer.class);
                 snapshotCount.setCombinerClass(UserReducer.class);
            
                 snapshotCount.setMapOutputKeyClass(LongWritable.class);
                 snapshotCount.setMapOutputValueClass(IntWritable.class);
                 
                 snapshotCount.setOutputKeyClass(LongWritable.class);
                 snapshotCount.setOutputValueClass(IntWritable.class);
        
                 snapshotCount.waitForCompletion(true);
                
                } catch (IOException e) {
                        throw new HadoopException("Could not create job or completing it", e);
                } catch (ClassNotFoundException e) {
                         throw new HadoopException("Could not find SequenceFileInputFormat.class", e);
                } catch (InterruptedException e) {
                        throw new HadoopException("Been interrupted", e);
                }
    }
}