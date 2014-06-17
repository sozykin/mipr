package mipr.image;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.openimaj.image.FImage;
import org.openimaj.image.ImageUtilities;
import static org.junit.Assert.*;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;

@RunWith(JUnit4.class)
public class FImageWritableTest {

    private String IMAGE_FILE_NAME = "test";
    private String IMAGE_FILE_FORMAT = "jpg";

    @Test
    public void testSerialization() throws IOException {
        try {
            URL resourceUrl = getClass().getResource("/1_gray.jpg");
            File f = new File(resourceUrl.toURI());
            FImage image = ImageUtilities.readF(f);
            FImageWritable fiw = new FImageWritable();
            fiw.setFileName(IMAGE_FILE_NAME);
            fiw.setFormat(IMAGE_FILE_FORMAT);
            fiw.setFImage(image);
            ByteArrayOutputStream bao = new ByteArrayOutputStream();
            fiw.write(new DataOutputStream(bao));
            ByteArrayInputStream bai = new ByteArrayInputStream(bao.toByteArray());
            FImageWritable fiw2 = new FImageWritable();
            fiw2.readFields(new DataInputStream(bai));

            assertEquals("Image width serialization error",
                    fiw.getFImage().getWidth(),
                    fiw2.getFImage().getWidth());

            assertEquals("Image height serialization error",
                    fiw.getFImage().getHeight(),
                    fiw2.getFImage().getHeight());

            assertArrayEquals("Image pixel array serialization error",
                    fiw.getFImage().getFloatPixelVector(),
                    fiw2.getFImage().getFloatPixelVector(),
                    0.001f);

        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

    }
}
