package mipr.lib.bi;

import mipr.imageapi.ImageProcessor;

import java.awt.*;
import java.awt.image.BufferedImage;

/**
 * Image processor which converts BufferedImage to Grayscale
 *
 * @see mipr.imageapi.ImageProcessor
 */
public class BI2Gray implements ImageProcessor<BufferedImage> {

    @Override
    public BufferedImage processImage(BufferedImage image) {
        if (image != null){
            BufferedImage grayImage = new BufferedImage(image.getWidth(), image.getHeight(),
                    BufferedImage.TYPE_BYTE_GRAY);
            Graphics g = grayImage.getGraphics();
            g.drawImage(image, 0, 0, null);
            g.dispose();
            return grayImage;
        }
        return null;
    }
}
