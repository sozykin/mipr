package core.recordReaders;

import core.writables.ImageWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by Epanchee on 22.02.15.
 */
public abstract class ImageRecordReader<I extends ImageWritable> extends RecordReader<NullWritable, I> {

    protected String fileName;
    protected FSDataInputStream fileStream;
    protected boolean processed = false;
    private I im;

    protected abstract I readImage(FSDataInputStream fileStream) throws IOException;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) inputSplit;
        Configuration job = taskAttemptContext.getConfiguration();
        final Path file = fileSplit.getPath();
        fileName = file.getName();
        FileSystem fs = file.getFileSystem(job);
        fileStream = fs.open(file);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {
            im = readImage(fileStream);
            if (im != null) {
                setFilenameAndFormat();
                processed = true;
                return true;
            }
        }

        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public I getCurrentValue() throws IOException, InterruptedException {
        return im;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
        fileStream.close();
    }

    private void setFilenameAndFormat() {
        if ((fileName != null) && (im !=null)) {
            // Determining image format
            int dotPos = fileName.lastIndexOf(".");
            if (dotPos > -1) {
                im.setFileName(fileName.substring(0, dotPos));
                im.setFormat(fileName.substring(dotPos + 1));
            } else {
                im.setFileName(fileName);
            }
        }
    }
}
