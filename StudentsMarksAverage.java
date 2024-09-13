import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StudentMarksAverage {

    // Mapper class
    public static class MarksMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length == 9) {  // Ensure correct number of columns
                String section = fields[4];
                double science = Double.parseDouble(fields[5]);
                double english = Double.parseDouble(fields[6]);
                double history = Double.parseDouble(fields[7]);
                double maths = Double.parseDouble(fields[8]);

                // Calculate total marks for each student
                double totalMarks = science + english + history + maths;

                // Emit section as key and total marks as value
                context.write(new Text(section), new DoubleWritable(totalMarks));

                // Emit "Overall" as key to compute overall average
                context.write(new Text("Overall"), new DoubleWritable(totalMarks));
            }
        }
    }

    // Reducer class
    public static class MarksReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }
            double average = sum / count;
            context.write(key, new DoubleWritable(average));
        }
    }

    // Driver class
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: StudentMarksAverage <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Student Marks Average");

        job.setJarByClass(StudentMarksAverage.class);
        job.setMapperClass(MarksMapper.class);
        job.setReducerClass(MarksReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
