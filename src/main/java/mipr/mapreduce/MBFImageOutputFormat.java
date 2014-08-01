package mipr.mapreduce;

import mipr.image.MBFImageWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * OutputFormat for writing MBFImageWritable to filesystem
 * Write every image as a separate file with the name and format from
 * MBFImageWritable
 */
public class MBFImageOutputFormat extends FileOutputFormat<NullWritable, MBFImageWritable> {

    @Override
    public RecordWriter<NullWritable, MBFImageWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MBFImageRecordWriter(taskAttemptContext);
    }
}
