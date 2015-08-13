package opencv;

import core.formats.CombineImageInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * Created by Epanchee on 26.05.2015.
 */
public class CombineMatImageInputFormat extends CombineImageInputFormat<NullWritable, MatImageWritable> {
    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        return new CombineFileRecordReader<NullWritable, MatImageWritable>((CombineFileSplit) inputSplit, taskAttemptContext, CombineMatImageRecordReader.class);
    }
}
