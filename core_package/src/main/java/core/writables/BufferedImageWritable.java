package core.writables;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;

/**
 * Created by Epanchee on 17.02.15.
 */
public class BufferedImageWritable extends ImageWritable<BufferedImage> {

    public BufferedImageWritable() {

    }
    public BufferedImageWritable(BufferedImage _im){
        this();
        im = _im;
    }

    public BufferedImageWritable(BufferedImage _im, String _filename, String _format){
        this(_im);
        fileName = _filename;
        format = _format;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        // Write image
        // Convert image to byte array
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(im, this.getFormat(), baos);
        baos.flush();
        byte[] bytes = baos.toByteArray();
        baos.close();
        // Write byte array size
        out.writeInt(bytes.length);
        // Write image bytes
        out.write(bytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        super.readFields(in);

        // Read image byte array size
        int bArraySize = in.readInt();
        // Read image byte array
        byte[] bArray = new byte[bArraySize];
        in.readFully(bArray);
        // Read image from byte array
        im = ImageIO.read(new ByteArrayInputStream(bArray));
    }
}
