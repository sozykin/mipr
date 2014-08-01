package mipr.utils;

import org.openimaj.image.FImage;
import org.openimaj.image.MBFImage;

import java.nio.ByteBuffer;

/**
 * Class containing utilities for processing images in OpenIMAJ format
 */

public class OpenIMAJUtilities {

    // Convert FImage pixels to byte array
    public static byte[] fImagePixelsToByteArray(FImage im){
        int width = im.getWidth();
        int height = im.getHeight();
        ByteBuffer buffer = ByteBuffer.allocate(4 * width * height);
        for (int i = 0; i < height; i++)
            for (int j = 0; j < width; j++)
                buffer.putFloat(im.pixels[i][j]);

        return buffer.array();
    }

    // Convert MBFImage pixels to byte array
    public static byte[] mbfImagePixelsToByteArray(MBFImage im){
        int width = im.getWidth();
        int height = im.getHeight();
        int bands = im.numBands();
        ByteBuffer buffer = ByteBuffer.allocate(4 * width * height * bands);
        for (int b = 0; b < bands; b++) {
            FImage fim = im.bands.get(b);
            for (int i = 0; i < height; i++)
                for (int j = 0; j < width; j++)
                    buffer.putFloat(fim.pixels[i][j]);
        }
        return buffer.array();
    }

    // Convert byte array to FImage pixels
    public static void setFImagePixelsFromByteArray(FImage im, byte[] bPixels){
        ByteBuffer buffer = ByteBuffer.wrap(bPixels);
        for (int i = 0; i < im.height; i++)
            for (int j = 0; j < im.width; j++)
                im.pixels[i][j] = buffer.getFloat();
    }

    // Convert byte array to FImage pixels
    public static void setMBFImagePixelsFromByteArray(MBFImage im, byte[] bPixels){
        ByteBuffer buffer = ByteBuffer.wrap(bPixels);
        int bands = im.numBands();
        for (int b = 0; b < bands; b++) {
            FImage fim = im.bands.get(b);
            for (int i = 0; i < fim.height; i++)
                for (int j = 0; j < fim.width; j++)
                    fim.pixels[i][j] = buffer.getFloat();
        }
    }

}
