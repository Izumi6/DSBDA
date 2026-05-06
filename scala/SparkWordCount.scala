/*
 * ============================================
 *  SparkWordCount.scala
 * ============================================
 *  What it does:
 *    Counts how many times each word appears
 *    in a text file using Apache Spark (Scala).
 *
 *  How it works:
 *    1. Read the text file
 *    2. Split each line into words
 *    3. Count each word
 *    4. Print the result
 * ============================================
 */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkWordCount {

  def main(args: Array[String]): Unit = {

    // Set up Spark to run on your local machine
    val conf = new SparkConf()
      .setAppName("Word Count")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // Read the input text file (path given as argument)
    val inputFile = args(0)
    val textFile = sc.textFile(inputFile)

    // Count words:
    //   split line into words → make lowercase → count
    val wordCounts = textFile
      .flatMap(line => line.split(" "))
      .filter(word => word.nonEmpty)
      .map(word => (word.toLowerCase, 1))
      .reduceByKey(_ + _)

    // Print results
    println("\n===== WORD COUNT RESULTS =====")
    wordCounts.collect().foreach {
      case (word, count) => println(s"  $word -> $count")
    }
    println("==============================\n")

    // Stop Spark
    sc.stop()
  }
}


/*
 * ================================================================
 *  HOW TO RUN — Terminal Commands (Ubuntu / VS Code Terminal)
 * ================================================================
 *
 * ────────────────────────────────────────────────────────────────
 *  INSTALL REQUIREMENTS (one-time setup)
 * ────────────────────────────────────────────────────────────────
 *
 *  1. Install Java:
 *       sudo apt update
 *       sudo apt install openjdk-8-jdk -y
 *       java -version
 *
 *  2. Install Scala:
 *       sudo apt install scala -y
 *       scala -version
 *
 *  3. Download & install Apache Spark:
 *       cd ~
 *       wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
 *       tar -xzf spark-3.5.1-bin-hadoop3.tgz
 *       sudo mv spark-3.5.1-bin-hadoop3 /usr/local/spark
 *
 *  4. Add Spark to your PATH (open bashrc):
 *       nano ~/.bashrc
 *
 *       Add these two lines at the bottom:
 *         export SPARK_HOME=/usr/local/spark
 *         export PATH=$PATH:$SPARK_HOME/bin
 *
 *       Save (Ctrl+O, Enter) and exit (Ctrl+X), then run:
 *         source ~/.bashrc
 *
 *  5. Check everything works:
 *       java -version
 *       scala -version
 *       spark-shell --version
 *
 * ────────────────────────────────────────────────────────────────
 *  VS CODE EXTENSIONS (optional but helpful)
 * ────────────────────────────────────────────────────────────────
 *
 *    Open VS Code → Extensions (Ctrl+Shift+X)
 *    → Search "Scala Metals" → Install
 *    → Search "Scala Syntax" → Install
 *
 * ────────────────────────────────────────────────────────────────
 *  RUN THE PROGRAM
 * ────────────────────────────────────────────────────────────────
 *
 *  6. Create a sample input file:
 *       mkdir -p ~/spark_input
 *       echo "Hello World Hello Spark World Apache Spark Hello" > ~/spark_input/input.txt
 *       → File at: ~/spark_input/input.txt
 *
 *  7. Go to folder where this file is saved:
 *       cd /home/YourUsername/scala    ← PUT YOUR PATH HERE
 *
 *  8. Compile:
 *       scalac -classpath "$SPARK_HOME/jars/*" SparkWordCount.scala
 *
 *  9. Create JAR:
 *       jar -cvf sparkwordcount.jar *.class
 *
 *  10. Run with spark-submit:
 *       spark-submit --class SparkWordCount --master local[*] sparkwordcount.jar ~/spark_input/input.txt
 *                                                                                ↑
 *                                                                       PUT YOUR INPUT FILE PATH
 *
 *       → Output on screen:
 *           hello  -> 3
 *           world  -> 2
 *           spark  -> 2
 *           apache -> 1
 *
 * ================================================================
 */
