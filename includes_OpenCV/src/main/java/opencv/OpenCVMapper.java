package opencv;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Epanchee on 13.08.2015.
 */
public class OpenCVMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Path[] myCacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        System.load(myCacheFiles[0].toUri().getPath());
    }
}
