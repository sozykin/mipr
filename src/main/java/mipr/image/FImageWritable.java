package mipr.image;

import org.openimaj.image.FImage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * MIPr image representation based on FImage -
 * a grayscale image from OpenIMAJ.
 * FImageWritable implements Hadoop Writable interface
 * and can be used as a value in MapReduce programs
 */

public class FImageWritable extends ImageWritable  {

    // Internal image representation
    FImage fi = null;

    public FImage getFImage() {
        return fi;
    }

    public void setFImage(FImage fi) {
        this.fi = fi;
    }

    public void write(DataOutput out) throws IOException {
        super.write(out);

        // Determining size of image
        int width = fi.getWidth();
        int height = fi.getHeight();
        // Getting pixel array
        float [] pixels = fi.getFloatPixelVector();

        // Writing size of image
        out.writeInt(width);
        out.writeInt(height);

        // Write pixels
        out.write(fImagePixelsToByteArray());
    }

    public void readFields(DataInput in) throws IOException{
        super.readFields(in);

        // Reading image size
        int width = in.readInt();
        int height = in.readInt();

        // Creating buffer for pixels
        byte[] bPixels = new byte[4 * width * height];

        // Reading pixel data
        in.readFully(bPixels);

        fi = new FImage(width, height);
        fImagePixelsFromByteArray(bPixels);
    }

    // Convert FImage pixels to byte array
    private byte[] fImagePixelsToByteArray(){
        int width = fi.getWidth();
        int height = fi.getHeight();
        ByteBuffer buffer = ByteBuffer.allocate(4 * width * height);
        for (int i = 0; i < height; i++)
            for (int j = 0; j < width; j++)
                buffer.putFloat(fi.pixels[i][j]);

        return buffer.array();
    }

    // Convert byte array to image pixels
    private void fImagePixelsFromByteArray(byte[] bPixels){
        ByteBuffer buffer = ByteBuffer.wrap(bPixels);
        for (int i = 0; i < fi.height; i++)
            for (int j = 0; j < fi.width; j++)
                fi.pixels[i][j] = buffer.getFloat();

    }



}
