package mipr.mapreduce;

import mipr.image.BufferedImageWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.imageio.ImageIO;
import java.io.IOException;

/**
 * OutputFormat for writing BufferedImageWritable to filesystem
 * Write every image as a separate file with the name and format from
 * BufferedImageWritable
 */

public class BufferedImageOutputFormat extends FileOutputFormat<NullWritable,BufferedImageWritable> {

    /**
     *  Inner class which writes single image
    */

    protected static class BufferedImageRecordWriter extends RecordWriter<NullWritable,BufferedImageWritable> {
        private TaskAttemptContext taskAttemptContext = null;

        BufferedImageRecordWriter(TaskAttemptContext taskAttemptContext){
            this.taskAttemptContext = taskAttemptContext;
        }

        @Override
        public void write(NullWritable nullWritable, BufferedImageWritable bufferedImageWritable) throws IOException, InterruptedException {
            if (bufferedImageWritable.getImage() != null) {
                FSDataOutputStream imageFile = null;
                Configuration job = taskAttemptContext.getConfiguration();
                Path file = FileOutputFormat.getOutputPath(taskAttemptContext);
                FileSystem fs = file.getFileSystem(job);
                // Constructing iamge filename and path
                Path imageFilePath = new Path(file, bufferedImageWritable.getFileName() + "." + bufferedImageWritable.getFormat());
                try {
                    // Creating file
                    imageFile = fs.create(imageFilePath);
                    // Write image to file using ImageIO
                    ImageIO.write(bufferedImageWritable.getImage(), bufferedImageWritable.getFormat(), imageFile);
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
    public RecordWriter<NullWritable,BufferedImageWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new BufferedImageRecordWriter(taskAttemptContext);
    }
}
