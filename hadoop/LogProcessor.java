/*
 * ============================================
 *  LogProcessor.java
 * ============================================
 *  What it does:
 *    Reads a system log file and counts how many
 *    times each log level appears.
 *    Log levels: INFO, WARN, ERROR, DEBUG, FATAL
 *
 *  How it works:
 *    1. MAPPER reads each log line → finds the
 *       log level → sends out (level, 1)
 *    2. REDUCER adds up all counts for each level
 *
 *  Example log line:
 *    "2024-01-15 10:23:45 INFO Server started"
 *     ↑ date      ↑ time  ↑ level  ↑ message
 *
 *  Example output:
 *    ERROR  4
 *    INFO   7
 *    WARN   5
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

public class LogProcessor {

    // ──────────── MAPPER ────────────
    // Input:  one log line like "2024-01-15 10:23:45 INFO Server started"
    // Output: (INFO, 1) — extracts the log level and emits count 1
    public static class LogMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text logLevel = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip empty lines
            if (line.trim().isEmpty()) return;

            // Go through each word in the line
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken().toUpperCase();

                // Check if this word is a log level
                if (token.equals("INFO") || token.equals("ERROR") ||
                    token.equals("WARN") || token.equals("DEBUG") ||
                    token.equals("FATAL")) {

                    logLevel.set(token);
                    context.write(logLevel, one);  // output: (level, 1)
                    return;  // only one level per line
                }
            }
        }
    }

    // ──────────── REDUCER ────────────
    // Input:  (INFO, [1, 1, 1, ...])
    // Output: (INFO, total count)
    public static class LogReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // Add up all the 1s for this log level
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
        Job job = Job.getInstance(conf, "Log File Processor");

        job.setJarByClass(LogProcessor.class);
        job.setMapperClass(LogMapper.class);
        job.setCombinerClass(LogReducer.class);
        job.setReducerClass(LogReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // args[0] = input folder path (folder with your log file)
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
 *  STEP 1: Check if Hadoop is working
 * ────────────────────────────────────────────────────────────────
 *
 *    hadoop version
 *
 *    → You should see "Hadoop 3.3.6" or similar
 *
 * ────────────────────────────────────────────────────────────────
 *  STEP 2: Set the Hadoop classpath
 *          (Tells Java where Hadoop's files are)
 * ────────────────────────────────────────────────────────────────
 *
 *    export HADOOP_CLASSPATH=$(hadoop classpath)
 *
 * ────────────────────────────────────────────────────────────────
 *  STEP 3: Create a folder for the input log file
 * ────────────────────────────────────────────────────────────────
 *
 *    mkdir -p ~/logprocessor_input
 *
 * ────────────────────────────────────────────────────────────────
 *  STEP 4: Create a sample log file
 *          (Copy-paste this entire block into terminal)
 * ────────────────────────────────────────────────────────────────
 *
 *    cat > ~/logprocessor_input/system.log << 'EOF'
 *    2024-01-15 10:23:45 INFO Server started successfully
 *    2024-01-15 10:23:46 INFO Loading configuration files
 *    2024-01-15 10:24:12 ERROR Connection refused to database
 *    2024-01-15 10:24:15 WARN Retrying database connection
 *    2024-01-15 10:24:20 WARN Retrying attempt 2
 *    2024-01-15 10:24:25 INFO Database connected
 *    2024-01-15 10:25:00 WARN Memory usage above 80%
 *    2024-01-15 10:26:30 ERROR Disk write failure on sda1
 *    2024-01-15 10:27:00 DEBUG Checking health metrics
 *    2024-01-15 10:27:05 DEBUG CPU usage at 45%
 *    2024-01-15 10:28:00 INFO User login from 192.168.1.100
 *    2024-01-15 10:29:00 ERROR Auth failed for user admin
 *    2024-01-15 10:30:00 FATAL System kernel panic detected
 *    2024-01-15 10:30:01 INFO Emergency shutdown initiated
 *    2024-01-15 10:30:05 WARN Unsaved data in buffer
 *    2024-01-15 10:31:00 INFO System restarted
 *    2024-01-15 10:31:30 DEBUG Running diagnostics
 *    2024-01-15 10:32:00 INFO All services restored
 *    2024-01-15 10:33:00 ERROR Network timeout on eth0
 *    2024-01-15 10:34:00 WARN High latency 250ms
 *    EOF
 *
 *    → File created at: ~/logprocessor_input/system.log
 *    → You can also use your own log file instead
 *
 * ────────────────────────────────────────────────────────────────
 *  STEP 5: Go to the folder where LogProcessor.java is saved
 *          ⚠️ REPLACE the path below with YOUR actual path!
 * ────────────────────────────────────────────────────────────────
 *
 *    cd /home/YourUsername/hadoop
 *       ↑
 *       PUT YOUR PATH HERE — this is the folder that has
 *       LogProcessor.java inside it.
 *
 *    To check you're in the right folder, type:
 *      ls
 *    → You should see "LogProcessor.java" in the list
 *
 * ────────────────────────────────────────────────────────────────
 *  STEP 6: Compile the Java file
 * ────────────────────────────────────────────────────────────────
 *
 *    javac -classpath $HADOOP_CLASSPATH -d . LogProcessor.java
 *
 *    → If no errors, compilation was successful
 *
 * ────────────────────────────────────────────────────────────────
 *  STEP 7: Package into a JAR file
 * ────────────────────────────────────────────────────────────────
 *
 *    jar -cvf logprocessor.jar *.class
 *
 *    → Creates "logprocessor.jar" in current folder
 *
 * ────────────────────────────────────────────────────────────────
 *  STEP 8: Run the MapReduce program
 *          ⚠️ Output folder must NOT exist before running!
 * ────────────────────────────────────────────────────────────────
 *
 *    hadoop jar logprocessor.jar LogProcessor ~/logprocessor_input ~/logprocessor_output
 *                                              ↑                    ↑
 *                                              INPUT PATH           OUTPUT PATH
 *                                              (folder with         (Hadoop creates
 *                                              system.log)          this for you)
 *
 *    → Wait for it to finish (you'll see progress %)
 *
 * ────────────────────────────────────────────────────────────────
 *  STEP 9: See the results!
 * ────────────────────────────────────────────────────────────────
 *
 *    cat ~/logprocessor_output/part-r-00000
 *
 *    → Expected output:
 *        DEBUG    3
 *        ERROR    4
 *        FATAL    1
 *        INFO     7
 *        WARN     5
 *
 * ────────────────────────────────────────────────────────────────
 *  STEP 10: Want to run again? Delete old output first!
 * ────────────────────────────────────────────────────────────────
 *
 *    rm -rf ~/logprocessor_output
 *
 *    → Then repeat from Step 8
 *
 * ================================================================
 *  QUICK SUMMARY (all commands in order):
 * ================================================================
 *
 *    hadoop version
 *    export HADOOP_CLASSPATH=$(hadoop classpath)
 *    mkdir -p ~/logprocessor_input
 *    cat > ~/logprocessor_input/system.log << 'EOF'
 *    ...paste log lines here...
 *    EOF
 *    cd /home/YourUsername/hadoop              ← PUT YOUR PATH
 *    javac -classpath $HADOOP_CLASSPATH -d . LogProcessor.java
 *    jar -cvf logprocessor.jar *.class
 *    hadoop jar logprocessor.jar LogProcessor ~/logprocessor_input ~/logprocessor_output
 *    cat ~/logprocessor_output/part-r-00000
 *
 * ================================================================
 */
