[![Build Status](https://travis-ci.org/sozykin/mipr.svg?branch=master)](https://travis-ci.org/sozykin/mipr)

MIPr - MapReduce Image Processing framework for Hadoop
======================================================

MIPr provides the ability to process images in Hadoop.

MIPr includes:
* Writable Wrappers for images
* InputFormat and OutputFormat for images
* Several Jobs for image processing

## Installation


### Prerequisites


* Java 7 (preferably Oracle)
* Maven 3

### Building


1. Download and unzip MIPr source
2. Run the command:

    mvn package

   It will build jar file and place it in the target directory.

### Testing


1. Copy image files to HDFS:

    $ hadoop fs -copyFromLocal local_image_folder hdfs_image_folder

2. Run test MIPr Job which convert color images to grayscale:

    $ hadoop jar mipr-0.1-jar-with-dependencies.jar mipr.experiments.img2gray.Img2GrayJob hdfs_image_folder hdfs_output_folder

3. Copy processed images back from HDFS to the local filesystem:

    $ hadoop fs -copyToLocal hdfs_output_folder local_output_folder

4. Check that images were converted correctly.

