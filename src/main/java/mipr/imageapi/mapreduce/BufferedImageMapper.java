package mipr.imageapi.mapreduce;

import mipr.image.BufferedImageWritable;
import mipr.imageapi.ImageProcessor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.awt.image.BufferedImage;
import java.io.IOException;

/**
 * Created by u1213 on 5/3/14.
 */
public class BufferedImageMapper extends Mapper<NullWritable,BufferedImageWritable,
        NullWritable,BufferedImageWritable> {

    BufferedImage image, processedImage;
    BufferedImageWritable outImage;

    public void map(NullWritable key, BufferedImageWritable value, Context context)
            throws IOException, InterruptedException {
        image = value.getImage();
        if (image != null) {
            Configuration conf = context.getConfiguration();
            String processorClassName = conf.get("mipr.biprocessor.class");
            try {
                Class ipClass = Class.forName(processorClassName);
                ImageProcessor<BufferedImage> ip = (ImageProcessor)ipClass.newInstance();
                processedImage = ip.processImage(image);
                outImage = new BufferedImageWritable(processedImage, value.getFileName(), value.getFormat());
                context.write(NullWritable.get(), outImage);
            } catch (ClassNotFoundException e){
                System.err.println("Class not found " + processorClassName);
            } catch (InstantiationException e) {
                System.err.println("Cannot create object of class " + processorClassName);
            } catch (IllegalAccessException e) {
                System.err.println("Illegal access for class " + processorClassName);
            }


        }
    }
}
