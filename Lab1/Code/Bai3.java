import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai3 {

    public static class GenderMapper extends Mapper<Object, Text, Text, Text> {

        Map<String, String> userGender = new HashMap<>();
        Map<String, String> movieNames = new HashMap<>();

        protected void setup(Context context) throws IOException {

            URI[] cacheFiles = context.getCacheFiles();

            for (URI uri : cacheFiles) {

                BufferedReader br = new BufferedReader(
                        new FileReader(new File(uri.getPath()).getName()));

                String line;

                while ((line = br.readLine()) != null) {

                    if (line.contains("|")) {

                        String[] p = line.split(",\\s*", 3);
                        movieNames.put(p[0], p[1]);

                    } else {

                        String[] p = line.split(",\\s*");
                        if (p.length >= 2)
                            userGender.put(p[0], p[1]);
                    }
                }

                br.close();
            }
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",\\s*");

            if (parts.length < 4) return;

            String userId = parts[0];
            String movieId = parts[1];
            String rating = parts[2];

            String gender = userGender.get(userId);
            String movie = movieNames.get(movieId);

            if (gender == null || movie == null) return;

            context.write(new Text(movie), new Text(gender + ":" + rating));
        }
    }

    public static class GenderReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {

            double mSum = 0;
            int mCount = 0;

            double fSum = 0;
            int fCount = 0;

            for (Text v : values) {

                String[] p = v.toString().split(":");

                double rating = Double.parseDouble(p[1]);

                if (p[0].equals("M")) {

                    mSum += rating;
                    mCount++;

                } else {

                    fSum += rating;
                    fCount++;
                }
            }

            String mAvg = mCount > 0 ? String.format("%.2f", mSum / mCount) : "N/A";
            String fAvg = fCount > 0 ? String.format("%.2f", fSum / fCount) : "N/A";

            context.write(
                    key,
                    new Text("Male:" + mAvg + " Female:" + fAvg)
            );
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Gender Analysis");

        job.setJarByClass(Bai3.class);

        job.setMapperClass(GenderMapper.class);
        job.setReducerClass(GenderReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI(args[1]));
        job.addCacheFile(new URI(args[2]));

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
