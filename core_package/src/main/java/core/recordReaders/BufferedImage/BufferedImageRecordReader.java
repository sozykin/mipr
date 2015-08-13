package core.recordReaders.BufferedImage;

import core.recordReaders.ImageRecordReader;
import core.writables.BufferedImageWritable;
import org.apache.hadoop.fs.FSDataInputStream;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;

/**
 * Created by Epanchee on 22.02.15.
 */
public class BufferedImageRecordReader extends ImageRecordReader<BufferedImageWritable> {
    @Override
    protected BufferedImageWritable readImage(FSDataInputStream fileStream) {
        BufferedImageWritable biw = new BufferedImageWritable();
        BufferedImage bi;
        try {
            bi = ImageIO.read(fileStream);
        } catch (Exception e) {
            bi = null;
        }
        biw.setImage(bi);
        return biw;
    }
}
