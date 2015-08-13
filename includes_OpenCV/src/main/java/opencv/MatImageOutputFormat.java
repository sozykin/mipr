package opencv;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Epanchee on 24.02.15.
 */
public class MatImageOutputFormat extends FileOutputFormat<NullWritable, MatImageWritable> {

    @Override
    public RecordWriter<NullWritable, MatImageWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MatImageRecordWriter(taskAttemptContext);
    }
}
