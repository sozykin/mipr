package mipr.imageapi;

/**
 * Interface for image processors in MIPr
 */
public interface ImageProcessor <ImageType> {

    /**
     * Process source image
     * Used from MIPr Jobs
     * @param image     the source image
     * @return          processed image
     */
    public ImageType processImage(ImageType image);

}
