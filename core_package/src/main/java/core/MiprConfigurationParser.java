package core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

/**
 * Created by Epanchee on 28.04.15.
 */
public class MiprConfigurationParser {

    MiprConfigurationParser() throws IOException, ParseException {
        JSONParser jparser = new JSONParser();
        JSONObject settings = (JSONObject) jparser.parse(new FileReader(getClass().getClassLoader().getResource("main.json").getFile()));

        opencvpath = (String) settings.get("opencvpath");
        maxsplitsize = (long) settings.get("maxsplitsize");
    }

    public static String opencvpath = "";
    public static long maxsplitsize = 134217728;

    public static URI getOpenCVUri(){
        return new Path(opencvpath).toUri();
    }

    public static long getMaxSplitSize(){
        return maxsplitsize;
    }

    public static Job getOpenCVJobTemplate() throws IOException {
        Configuration conf = new Configuration();
        return getOpenCVJobTemplate(conf);
    }

    public static Job getOpenCVJobTemplate(Configuration conf) throws IOException {
        DistributedCache.addCacheFile(MiprConfigurationParser.getOpenCVUri(), conf);
        Job job = new Job(conf);
        job.setNumReduceTasks(0);

        return job;
    }
}