/*
 * WordCount.java
 * ---------------
 * Counts how many times each word appears in a text file.
 * Uses Hadoop MapReduce in local (standalone) mode.
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    /*
     * MAPPER
     * - Reads each line of input
     * - Splits it into words
     * - Outputs (word, 1) for every word
     */
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);   // emit (word, 1)
            }
        }
    }

    /*
     * REDUCER
     * - Receives all counts for a word
     * - Adds them up
     * - Outputs (word, total)
     */
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);     // emit (word, total)
        }
    }

    /*
     * MAIN / DRIVER
     * - Sets up the job and runs it
     */
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));   // input folder
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // output folder

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


/* ============================================================
 *  HOW TO RUN — Terminal Commands (Ubuntu)
 *  (Hadoop is already installed)
 * ============================================================
 *
 *  1. Check Hadoop is working:
 *
 *       hadoop version
 *
 *  -----------------------------------------------------------
 *  2. Set the classpath (needed for compiling):
 *
 *       export HADOOP_CLASSPATH=$(hadoop classpath)
 *
 *  -----------------------------------------------------------
 *  3. Create a sample input file:
 *
 *       mkdir -p ~/wordcount_input
 *       echo "Hello World Hello Hadoop World MapReduce Hello" > ~/wordcount_input/input.txt
 *
 *       → File created at: ~/wordcount_input/input.txt
 *
 *  -----------------------------------------------------------
 *  4. Go to the folder where this file is saved:
 *
 *       cd <path-to-this-file>
 *       Example: cd ~/hadoop/
 *
 *  -----------------------------------------------------------
 *  5. Compile the Java file:
 *
 *       javac -classpath $HADOOP_CLASSPATH -d . WordCount.java
 *
 *  -----------------------------------------------------------
 *  6. Make a JAR file:
 *
 *       jar -cvf wordcount.jar *.class
 *
 *  -----------------------------------------------------------
 *  7. Run the program:
 *
 *       hadoop jar wordcount.jar WordCount ~/wordcount_input ~/wordcount_output
 *
 *       → Arg 1 (input):  ~/wordcount_input   (folder with input.txt)
 *       → Arg 2 (output): ~/wordcount_output  (must NOT exist yet)
 *
 *  -----------------------------------------------------------
 *  8. See the result:
 *
 *       cat ~/wordcount_output/part-r-00000
 *
 *       Output will look like:
 *         Hadoop       1
 *         Hello        3
 *         MapReduce    1
 *         World        2
 *
 *  -----------------------------------------------------------
 *  9. To run again, first delete old output:
 *
 *       rm -rf ~/wordcount_output
 *
 * ============================================================ */
