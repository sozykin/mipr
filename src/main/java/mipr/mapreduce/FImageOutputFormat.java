package mipr.mapreduce;

import mipr.image.FImageWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.openimaj.image.ImageUtilities;

import java.io.IOException;
import java.io.OutputStream;

/**
 * OutputFormat for writing FImageWritable to filesystem
 * Write every image as a separate file with the name and format from
 * FImageWritable
 */
public class FImageOutputFormat extends FileOutputFormat<NullWritable, FImageWritable> {
    /**
     *  Inner class which writes single image
     */

    protected static class FImageRecordWriter extends RecordWriter<NullWritable,FImageWritable> {
        private TaskAttemptContext taskAttemptContext = null;

        FImageRecordWriter(TaskAttemptContext taskAttemptContext){
            this.taskAttemptContext = taskAttemptContext;
        }

        @Override
        public void write(NullWritable nullWritable, FImageWritable fImageWritable) throws IOException, InterruptedException {
            if (fImageWritable.getImage() != null) {
                FSDataOutputStream imageFile = null;
                Configuration job = taskAttemptContext.getConfiguration();
                Path file = FileOutputFormat.getOutputPath(taskAttemptContext);
                FileSystem fs = file.getFileSystem(job);
                // Constructing image filename and path
                Path imageFilePath = new Path(file, fImageWritable.getFileName() + "." + fImageWritable.getFormat());
                try {
                    // Creating file
                    imageFile = fs.create(imageFilePath);
                    // Write image to file using OpenIMAJ ImageUtilities
                    ImageUtilities.write(fImageWritable.getImage(),
                            fImageWritable.getFormat(), (OutputStream)imageFile);
                } finally {
                    IOUtils.closeStream(imageFile);
                }
            }

        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        }
    }



    @Override
    public RecordWriter<NullWritable, FImageWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new FImageRecordWriter(taskAttemptContext);
    }
}
