package mipr.image;

import org.openimaj.image.FImage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import mipr.utils.OpenIMAJUtilities;

/**
 * MIPr image representation based on FImage -
 * a grayscale image from OpenIMAJ.
 * FImageWritable implements Hadoop Writable interface
 * and can be used as a value in MapReduce programs
 */

public class FImageWritable extends ImageWritable<FImage>  {

    public FImage getImage() {
        return im;
    }

    public void setImage(FImage fi) {
        this.im = fi;
    }

    public void write(DataOutput out) throws IOException {
        super.write(out);

        // Determining size of image
        int width = im.getWidth();
        int height = im.getHeight();

        // Writing size of image
        out.writeInt(width);
        out.writeInt(height);

        // Write pixels
        out.write(OpenIMAJUtilities.fImagePixelsToByteArray(im));
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

        im = new FImage(width, height);
        OpenIMAJUtilities.setFImagePixelsFromByteArray(im, bPixels);
    }
}
