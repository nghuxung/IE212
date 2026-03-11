import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai2 {

    public static class GenreMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private Map<String, String> movieGenres = new HashMap<>();

        protected void setup(Context context) throws IOException {

            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null) {

                for (URI uri : cacheFiles) {

                    BufferedReader br = new BufferedReader(
                            new FileReader(new File(uri.getPath()).getName()));

                    String line;

                    while ((line = br.readLine()) != null) {

                        String[] parts = line.split(",\\s*", 3);

                        if (parts.length < 3) continue;

                        movieGenres.put(parts[0], parts[2]);
                    }

                    br.close();
                }
            }
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.isEmpty()) return;

            String[] parts = line.split(",\\s*");

            if (parts.length < 4) return;

            String movieId = parts[1];
            double rating = Double.parseDouble(parts[2]);

            String genres = movieGenres.get(movieId);

            if (genres == null) return;

            String[] genreList = genres.split("\\|");

            for (String g : genreList) {

                context.write(new Text(g), new DoubleWritable(rating));
            }
        }
    }

    public static class GenreReducer
            extends Reducer<Text, DoubleWritable, Text, Text> {

        public void reduce(Text key, Iterable<DoubleWritable> values,
                Context context) throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;

            for (DoubleWritable v : values) {

                sum += v.get();
                count++;
            }

            double avg = sum / count;

            context.write(
                    key,
                    new Text(String.format("%.2f (Count: %d)", avg, count))
            );
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Genre Analysis");

        job.setJarByClass(Bai2.class);

        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI(args[1]));

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
