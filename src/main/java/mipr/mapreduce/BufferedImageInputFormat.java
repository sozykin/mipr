package mipr.mapreduce;

import mipr.image.BufferedImageWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;

/**
 * InputFormat for BufferedImageWritable
 * Read one image per Map. Not suitable for small images
 *
 * @see mipr.mapreduce.ImageInputFormat
 * @see mipr.image.BufferedImageWritable
 */

public class BufferedImageInputFormat extends ImageInputFormat<NullWritable,BufferedImageWritable> {

    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return new BufferedImageRecordReader();
    }


}


