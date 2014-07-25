package mipr.mapreduce;

import mipr.image.FImageWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.openimaj.image.FImage;
import org.openimaj.image.ImageUtilities;
import java.io.IOException;
import java.io.InputStream;

/**
 * InputFormat for FImageWritable
 * Read one image per Map. Not suitable for small images
 *
 * @see mipr.mapreduce.ImageInputFormat
 * @see mipr.image.FImageWritable
 */

public class FImageInputFormat extends ImageInputFormat<NullWritable,FImageWritable> {

    /**
     *  Inner class which reads single FImage
     */

    protected static class FImageRecordReader extends RecordReader<NullWritable, FImageWritable> {

        protected boolean processed = false;
        protected FImageWritable fiw = null;
        protected FSDataInputStream fileStream = null;
        protected String fileName;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
                throws IOException, InterruptedException {
            FileSplit split = (FileSplit) inputSplit;
            Configuration job = taskAttemptContext.getConfiguration();
            final Path file = split.getPath();
            fileName = file.getName();
            FileSystem fs = file.getFileSystem(job);
            fileStream = fs.open(file);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (!processed) {
                fiw = new FImageWritable();
                // Read image from filesystem using OpenIMAJ
                try {
                    FImage inputImage = ImageUtilities.readF((InputStream)fileStream);
                    fiw.setFImage(inputImage);
                } catch (Exception e) {
                    fiw.setFImage(null);
                }

                // Determining image format
                int dotPos = fileName.lastIndexOf(".");
                if (dotPos > -1) {
                    fiw.setFileName(fileName.substring(0, dotPos));
                    fiw.setFormat(fileName.substring(dotPos + 1));
                } else {
                    fiw.setFileName(fileName);
                }

                processed = true;
                return true;
            }
            return false;
        }

        @Override
        public NullWritable getCurrentKey() throws IOException, InterruptedException {
            return NullWritable.get();
        }

        @Override
        public FImageWritable getCurrentValue() throws IOException, InterruptedException {
            return fiw;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return processed ? 1.0f : 0.0f;
        }

        @Override
        public void close() throws IOException {
            fileStream.close();
        }
    }

    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        return new FImageRecordReader();
    }




}