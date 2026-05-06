/*
 * LogProcessor.java
 * ------------------
 * Reads a system log file and counts how many times
 * each log level appears (INFO, WARN, ERROR, DEBUG, FATAL).
 * Uses Hadoop MapReduce in local (standalone) mode.
 *
 * Expected log format:
 *   2024-01-15 10:23:45 INFO  Server started successfully
 *   2024-01-15 10:24:12 ERROR Connection refused
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

    /*
     * MAPPER
     * - Reads each log line
     * - Finds the log level (INFO, ERROR, WARN, etc.)
     * - Outputs (logLevel, 1)
     */
    public static class LogMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text logLevel = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            if (line.trim().isEmpty()) return;

            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken().toUpperCase();

                // Check if this word is a log level
                if (token.equals("INFO") || token.equals("ERROR") ||
                    token.equals("WARN") || token.equals("DEBUG") ||
                    token.equals("FATAL")) {

                    logLevel.set(token);
                    context.write(logLevel, one);  // emit (level, 1)
                    return;
                }
            }
        }
    }

    /*
     * REDUCER
     * - Receives all counts for a log level
     * - Adds them up
     * - Outputs (logLevel, total)
     */
    public static class LogReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);    // emit (level, total)
        }
    }

    /*
     * MAIN / DRIVER
     * - Sets up the job and runs it
     */
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Log File Processor");

        job.setJarByClass(LogProcessor.class);
        job.setMapperClass(LogMapper.class);
        job.setCombinerClass(LogReducer.class);
        job.setReducerClass(LogReducer.class);

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
 *  3. Create a sample log file:
 *
 *       mkdir -p ~/logprocessor_input
 *
 *       cat > ~/logprocessor_input/system.log << 'EOF'
 *       2024-01-15 10:23:45 INFO Server started successfully
 *       2024-01-15 10:23:46 INFO Loading configuration files
 *       2024-01-15 10:24:12 ERROR Connection refused to database
 *       2024-01-15 10:24:15 WARN Retrying database connection
 *       2024-01-15 10:24:20 WARN Retrying attempt 2
 *       2024-01-15 10:24:25 INFO Database connected
 *       2024-01-15 10:25:00 WARN Memory usage above 80%
 *       2024-01-15 10:26:30 ERROR Disk write failure on sda1
 *       2024-01-15 10:27:00 DEBUG Checking health metrics
 *       2024-01-15 10:27:05 DEBUG CPU usage at 45%
 *       2024-01-15 10:28:00 INFO User login from 192.168.1.100
 *       2024-01-15 10:29:00 ERROR Auth failed for user admin
 *       2024-01-15 10:30:00 FATAL System kernel panic detected
 *       2024-01-15 10:30:01 INFO Emergency shutdown initiated
 *       2024-01-15 10:30:05 WARN Unsaved data in buffer
 *       2024-01-15 10:31:00 INFO System restarted
 *       2024-01-15 10:31:30 DEBUG Running diagnostics
 *       2024-01-15 10:32:00 INFO All services restored
 *       2024-01-15 10:33:00 ERROR Network timeout on eth0
 *       2024-01-15 10:34:00 WARN High latency 250ms
 *       EOF
 *
 *       → File created at: ~/logprocessor_input/system.log
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
 *       javac -classpath $HADOOP_CLASSPATH -d . LogProcessor.java
 *
 *  -----------------------------------------------------------
 *  6. Make a JAR file:
 *
 *       jar -cvf logprocessor.jar *.class
 *
 *  -----------------------------------------------------------
 *  7. Run the program:
 *
 *       hadoop jar logprocessor.jar LogProcessor ~/logprocessor_input ~/logprocessor_output
 *
 *       → Arg 1 (input):  ~/logprocessor_input   (folder with system.log)
 *       → Arg 2 (output): ~/logprocessor_output  (must NOT exist yet)
 *
 *  -----------------------------------------------------------
 *  8. See the result:
 *
 *       cat ~/logprocessor_output/part-r-00000
 *
 *       Output will look like:
 *         DEBUG    3
 *         ERROR    4
 *         FATAL    1
 *         INFO     7
 *         WARN     5
 *
 *  -----------------------------------------------------------
 *  9. To run again, first delete old output:
 *
 *       rm -rf ~/logprocessor_output
 *
 * ============================================================ */
