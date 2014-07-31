package mipr.mapreduce;

import mipr.image.BufferedImageWritable;
import org.apache.hadoop.fs.FSDataInputStream;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;


/**
 * RecordReader which read images from filesystem and create Key-Value Pair
 * Key - NullWritable
 * Value - BufferedImageWritable
 */

public class BufferedImageRecordReader extends ImageRecordReader<BufferedImageWritable> {

    protected BufferedImageWritable createImageWritable(){
        return new BufferedImageWritable();
    }

    protected BufferedImageWritable readImage(FSDataInputStream fsDataInputStream){
        BufferedImageWritable biw = new BufferedImageWritable();
        BufferedImage bi;
        try {
            bi = ImageIO.read(fsDataInputStream);
        } catch (Exception e) {
            bi = null;
        }
        biw.setImage(bi);
        return biw;
    }

}
