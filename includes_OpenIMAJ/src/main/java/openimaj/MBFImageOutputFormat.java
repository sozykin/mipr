package openimaj;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Epanchee on 26.04.15.
 */
public class MBFImageOutputFormat extends FileOutputFormat<NullWritable, MBFImageWritable> {
    @Override
    public RecordWriter<NullWritable, MBFImageWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MBFImageRecordWriter(taskAttemptContext);
    }
}
