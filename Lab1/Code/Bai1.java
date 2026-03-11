import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Bai1 {

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {

        private final Text movieIdKey = new Text();
        private final Text ratingValue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",\\s*");
            if (parts.length < 4) return;

            try {
                String movieId = parts[1].trim();
                double rating = Double.parseDouble(parts[2].trim());

                movieIdKey.set(movieId);
                ratingValue.set("Rate:" + rating);
                context.write(movieIdKey, ratingValue);
            } catch (Exception e) {
                // bỏ qua dòng lỗi
            }
        }
    }

    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {

        private final Text movieIdKey = new Text();
        private final Text movieNameValue = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",\\s*", 3);
            if (parts.length < 3) return;

            String movieId = parts[0].trim();
            String movieName = parts[1].trim();

            movieIdKey.set(movieId);
            movieNameValue.set("Movie:" + movieName);
            context.write(movieIdKey, movieNameValue);
        }
    }

    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {

        private String maxMovie = "";
        private double maxRating = 0.0;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0.0;
            int count = 0;
            String movieName = "";

            for (Text val : values) {
                String value = val.toString();

                if (value.startsWith("Rate:")) {
                    double rating = Double.parseDouble(value.substring(5));
                    sum += rating;
                    count++;
                } else if (value.startsWith("Movie:")) {
                    movieName = value.substring(6);
                }
            }

            if (count == 0) return;

            if (movieName.isEmpty()) {
                movieName = "UnknownMovie-" + key.toString();
            }

            double avg = sum / count;

            context.write(
                new Text(movieName),
                new Text(String.format("Average Rating: %.2f (Total Ratings: %d)", avg, count))
            );

            if (count >= 5 && avg > maxRating) {
                maxRating = avg;
                maxMovie = movieName;
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            if (!maxMovie.isEmpty()) {
                context.write(
                    new Text("RESULT"),
                    new Text(String.format(
                        "%s is the highest rated movie with an average rating of %.2f among movies with at least 5 ratings.",
                        maxMovie, maxRating
                    ))
                );
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: Bai1 <ratings_input> <movies_input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Rating Analysis");

        job.setJarByClass(Bai1.class);

        job.setReducerClass(RatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MovieMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
