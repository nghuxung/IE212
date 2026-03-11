import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai4 {

    public static class AgeMapper extends Mapper<Object, Text, Text, Text> {

        private Map<String, Integer> userAge = new HashMap<>();
        private Map<String, String> movieNames = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    File file = new File(uri.getPath());
                    String fileName = file.getName();

                    try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
                        String line;

                        if (fileName.contains("users")) {
                            while ((line = br.readLine()) != null) {
                                String[] p = line.split(",\\s*");
                                if (p.length >= 3) {
                                    try {
                                        userAge.put(p[0].trim(), Integer.parseInt(p[2].trim()));
                                    } catch (NumberFormatException e) {
                                        // bỏ qua dòng lỗi
                                    }
                                }
                            }
                        } else if (fileName.contains("movies")) {
                            while ((line = br.readLine()) != null) {
                                String[] p = line.split(",\\s*", 3);
                                if (p.length >= 2) {
                                    movieNames.put(p[0].trim(), p[1].trim());
                                }
                            }
                        }
                    }
                }
            }
        }

        private String getAgeBucket(int age) {
            if (age <= 18) return "0-18";
            if (age <= 35) return "18-35";
            if (age <= 50) return "35-50";
            return "50+";
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",\\s*");
            if (parts.length < 4) return;

            String userId = parts[0].trim();
            String movieId = parts[1].trim();
            String rating = parts[2].trim();

            Integer age = userAge.get(userId);
            String movie = movieNames.get(movieId);

            if (age == null || movie == null) return;

            context.write(new Text(movie), new Text(getAgeBucket(age) + ":" + rating));
        }
    }

    public static class AgeReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double sum0_18 = 0.0;
            int count0_18 = 0;

            double sum18_35 = 0.0;
            int count18_35 = 0;

            double sum35_50 = 0.0;
            int count35_50 = 0;

            double sum50 = 0.0;
            int count50 = 0;

            for (Text v : values) {
                String[] p = v.toString().split(":");
                if (p.length != 2) continue;

                String bucket = p[0].trim();
                double rating;

                try {
                    rating = Double.parseDouble(p[1].trim());
                } catch (NumberFormatException e) {
                    continue;
                }

                switch (bucket) {
                    case "0-18":
                        sum0_18 += rating;
                        count0_18++;
                        break;
                    case "18-35":
                        sum18_35 += rating;
                        count18_35++;
                        break;
                    case "35-50":
                        sum35_50 += rating;
                        count35_50++;
                        break;
                    case "50+":
                        sum50 += rating;
                        count50++;
                        break;
                }
            }

            String avg0_18 = count0_18 > 0 ? String.format("%.2f", sum0_18 / count0_18) : "NA";
            String avg18_35 = count18_35 > 0 ? String.format("%.2f", sum18_35 / count18_35) : "NA";
            String avg35_50 = count35_50 > 0 ? String.format("%.2f", sum35_50 / count35_50) : "NA";
            String avg50 = count50 > 0 ? String.format("%.2f", sum50 / count50) : "NA";

            String result = "0-18: " + avg0_18
                    + "  18-35: " + avg18_35
                    + "  35-50: " + avg35_50
                    + "  50+: " + avg50;

            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: Bai4 <ratingsInput> <usersFile> <moviesFile> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Age Analysis");

        job.setJarByClass(Bai4.class);

        job.setMapperClass(AgeMapper.class);
        job.setReducerClass(AgeReducer.class);

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
