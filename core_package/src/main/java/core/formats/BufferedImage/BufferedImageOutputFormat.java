package core.formats.BufferedImage;

import core.recordReaders.BufferedImage.BufferedImageRecordWriter;
import core.writables.BufferedImageWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Epanchee on 19.02.15.
 */
public class BufferedImageOutputFormat extends FileOutputFormat<NullWritable, BufferedImageWritable> {

    @Override
    public RecordWriter<NullWritable, BufferedImageWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new BufferedImageRecordWriter(taskAttemptContext);
    }
}
