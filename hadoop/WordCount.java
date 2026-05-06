/*
 * ============================================
 *  WordCount.java
 * ============================================
 *  What it does:
 *    Counts how many times each word appears
 *    in a text file using Hadoop MapReduce.
 *
 *  How it works:
 *    1. MAPPER reads each line → splits into words
 *       → sends out (word, 1) for each word
 *    2. REDUCER collects all (word, 1) pairs
 *       → adds them up → gives (word, total)
 *
 *  Example:
 *    Input:  "Hello World Hello"
 *    Output: Hello 2
 *            World 1
 * ============================================
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

    // ──────────── MAPPER ────────────
    // Input:  one line of text
    // Output: (word, 1) for each word in that line
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Split the line into words
            StringTokenizer itr = new StringTokenizer(value.toString());

            // For each word, output (word, 1)
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    // ──────────── REDUCER ────────────
    // Input:  (word, [1, 1, 1, ...])
    // Output: (word, total count)
    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // Add up all the 1s for this word
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // ──────────── MAIN (Driver) ────────────
    // Sets up and runs the MapReduce job
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // args[0] = input folder path
        // args[1] = output folder path (must not exist yet)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


/*
 * ================================================================
 *  HOW TO RUN THIS PROGRAM — Ubuntu Terminal Commands
 *  (Hadoop is already installed on your system)
 * ================================================================
 *
 *  Open your Ubuntu terminal and follow these steps one by one.
 *  Copy-paste each command exactly as shown.
 *
 * ────────────────────────────────────────────────────────────────
 *  STEP 1: First, check if Hadoop is working
 * ────────────────────────────────────────────────────────────────
 *
 *    hadoop version
 *
 *    → You should see something like "Hadoop 3.3.6"
 *    → If you see an error, Hadoop is not installed properly
 *
 * ────────────────────────────────────────────────────────────────
 *  STEP 2: Set the Hadoop classpath
 *          (This tells Java where Hadoop's files are)
 * ────────────────────────────────────────────────────────────────
 *
 *    export HADOOP_CLASSPATH=$(hadoop classpath)
 *
 *    → No output means it worked. This is needed for compiling.
 *
 * ────────────────────────────────────────────────────────────────
 *  STEP 3: Create a folder and a sample input text file
 * ────────────────────────────────────────────────────────────────
 *
 *    mkdir -p ~/wordcount_input
 *
 *    echo "Hello World Hello Hadoop World MapReduce Hello" > ~/wordcount_input/input.txt
 *
 *    → This creates a file at:  ~/wordcount_input/input.txt
 *    → The file contains: "Hello World Hello Hadoop World MapReduce Hello"
 *    → You can also put your own text file here instead
 *
 * ────────────────────────────────────────────────────────────────
 *  STEP 4: Go to the folder where WordCount.java is saved
 *          ⚠️ REPLACE the path below with YOUR actual path!
 * ────────────────────────────────────────────────────────────────
 *
 *    cd /home/YourUsername/hadoop
 *       ↑
 *       PUT YOUR PATH HERE — this is the folder that has
 *       WordCount.java inside it.
 *
 *    To check if you're in the right folder, type:
 *      ls
 *    → You should see "WordCount.java" in the list
 *
 * ────────────────────────────────────────────────────────────────
 *  STEP 5: Compile the Java file
 *          (Converts .java → .class files)
 * ────────────────────────────────────────────────────────────────
 *
 *    javac -classpath $HADOOP_CLASSPATH -d . WordCount.java
 *
 *    → If no errors appear, compilation is successful
 *    → This creates .class files in the current folder
 *
 * ────────────────────────────────────────────────────────────────
 *  STEP 6: Package into a JAR file
 *          (Hadoop needs a .jar file to run)
 * ────────────────────────────────────────────────────────────────
 *
 *    jar -cvf wordcount.jar *.class
 *
 *    → This creates "wordcount.jar" in the current folder
 *
 * ────────────────────────────────────────────────────────────────
 *  STEP 7: Run the MapReduce program
 *          ⚠️ Output folder must NOT exist before running!
 * ────────────────────────────────────────────────────────────────
 *
 *    hadoop jar wordcount.jar WordCount ~/wordcount_input ~/wordcount_output
 *                                       ↑                 ↑
 *                                       INPUT PATH        OUTPUT PATH
 *                                       (folder with      (Hadoop will
 *                                       your text file)   create this)
 *
 *    → Wait for it to finish (you'll see progress %)
 *    → If output folder already exists, you'll get an error.
 *      Delete it first: rm -rf ~/wordcount_output
 *
 * ────────────────────────────────────────────────────────────────
 *  STEP 8: See the results!
 * ────────────────────────────────────────────────────────────────
 *
 *    cat ~/wordcount_output/part-r-00000
 *
 *    → Expected output:
 *        Hadoop       1
 *        Hello        3
 *        MapReduce    1
 *        World        2
 *
 * ────────────────────────────────────────────────────────────────
 *  STEP 9: Want to run again? Delete old output first!
 * ────────────────────────────────────────────────────────────────
 *
 *    rm -rf ~/wordcount_output
 *
 *    → Then repeat from Step 7
 *
 * ================================================================
 *  QUICK SUMMARY (all commands in order):
 * ================================================================
 *
 *    hadoop version
 *    export HADOOP_CLASSPATH=$(hadoop classpath)
 *    mkdir -p ~/wordcount_input
 *    echo "Hello World Hello Hadoop World MapReduce Hello" > ~/wordcount_input/input.txt
 *    cd /home/YourUsername/hadoop              ← PUT YOUR PATH
 *    javac -classpath $HADOOP_CLASSPATH -d . WordCount.java
 *    jar -cvf wordcount.jar *.class
 *    hadoop jar wordcount.jar WordCount ~/wordcount_input ~/wordcount_output
 *    cat ~/wordcount_output/part-r-00000
 *
 * ================================================================
 */
