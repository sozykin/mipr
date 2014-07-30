package mipr.mapreduce;

import mipr.image.FImageWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;

/**
 * InputFormat for FImageWritable
 * Read one image per Map. Not suitable for small images
 *
 * @see mipr.mapreduce.ImageInputFormat
 * @see mipr.image.FImageWritable
 */

public class FImageInputFormat extends ImageInputFormat<NullWritable,FImageWritable> {

    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return new FImageRecordReader();
    }




}