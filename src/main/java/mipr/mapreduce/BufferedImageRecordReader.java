package mipr.mapreduce;

import mipr.image.BufferedImageWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;

/**
 * RecordReader which read images from filesystem and create Key-Value Pair
 * Key - NullWritable
 * Value - BufferedImageWritable
 */
public class BufferedImageRecordReader  extends RecordReader<NullWritable,BufferedImageWritable> {

    protected boolean processed = false;
    protected BufferedImageWritable biw = null;
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
            biw = new BufferedImageWritable();
            // Read image from filesystem using ImageIO
            try {
                BufferedImage inputImage = ImageIO.read(fileStream);
                biw.setBufferedImage(inputImage);
            } catch (Exception e) {
                biw.setBufferedImage(null);
            }


            // Determining image format
            int dotPos = fileName.lastIndexOf(".");
            if (dotPos > -1) {
                biw.setFileName(fileName.substring(0, dotPos));
                biw.setFormat(fileName.substring(dotPos + 1));
            } else {
                biw.setFileName(fileName);
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
    public BufferedImageWritable getCurrentValue() throws IOException, InterruptedException {
        return biw;
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