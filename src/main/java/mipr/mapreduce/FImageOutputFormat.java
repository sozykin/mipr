package mipr.mapreduce;

import mipr.image.FImageWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


/**
 * OutputFormat for writing FImageWritable to filesystem
 * Write every image as a separate file with the name and format from
 * FImageWritable
 */
public class FImageOutputFormat extends FileOutputFormat<NullWritable, FImageWritable> {

    @Override
    public RecordWriter<NullWritable, FImageWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new FImageRecordWriter(taskAttemptContext);
    }
}
