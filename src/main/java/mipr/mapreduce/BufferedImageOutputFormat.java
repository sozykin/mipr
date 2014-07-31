package mipr.mapreduce;

import mipr.image.BufferedImageWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * OutputFormat for writing BufferedImageWritable to filesystem
 * Write every image as a separate file with the name and format from
 * BufferedImageWritable
 */

public class BufferedImageOutputFormat extends FileOutputFormat<NullWritable,BufferedImageWritable> {

    @Override
    public RecordWriter<NullWritable,BufferedImageWritable> getRecordWriter(TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return new BufferedImageRecordWriter(taskAttemptContext);
    }
}
