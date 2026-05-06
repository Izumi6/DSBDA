/*
 * SparkWordCount.scala
 * ---------------------
 * Counts how many times each word appears in a text file.
 * Uses Apache Spark in Scala.
 * Simple and easy to understand.
 */

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkWordCount {

  def main(args: Array[String]): Unit = {

    // Step 1: Set up Spark (runs locally on your machine)
    val conf = new SparkConf()
      .setAppName("Word Count")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // Step 2: Read the input text file
    val inputFile = args(0)
    val textFile = sc.textFile(inputFile)

    // Step 3: Count words
    //   - Split each line into words
    //   - Make each word lowercase
    //   - Count how many times each word appears
    val wordCounts = textFile
      .flatMap(line => line.split(" "))
      .filter(word => word.nonEmpty)
      .map(word => (word.toLowerCase, 1))
      .reduceByKey(_ + _)

    // Step 4: Print the results
    println("\n===== WORD COUNT RESULTS =====")
    wordCounts.collect().foreach {
      case (word, count) => println(s"  $word -> $count")
    }
    println("==============================\n")

    // Step 5: Stop Spark
    sc.stop()
  }
}


/* ============================================================
 *  HOW TO RUN — Terminal Commands (Ubuntu / VS Code Terminal)
 * ============================================================
 *
 *  ---- INSTALL WHAT YOU NEED ----
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
 *  3. Install Apache Spark:
 *       cd ~
 *       wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
 *       tar -xzf spark-3.5.1-bin-hadoop3.tgz
 *       sudo mv spark-3.5.1-bin-hadoop3 /usr/local/spark
 *
 *  4. Add to your bashrc (open with: nano ~/.bashrc):
 *       export SPARK_HOME=/usr/local/spark
 *       export PATH=$PATH:$SPARK_HOME/bin
 *
 *     Then run:  source ~/.bashrc
 *
 *  5. Check everything works:
 *       java -version
 *       scala -version
 *       spark-shell --version
 *
 *  ---- VS CODE EXTENSIONS (optional) ----
 *     Open VS Code → Extensions (Ctrl+Shift+X)
 *       → Search "Scala Metals" → Install
 *       → Search "Scala Syntax" → Install
 *
 *  ---- RUN THE PROGRAM ----
 *
 *  6. Create a sample input file:
 *       mkdir -p ~/spark_input
 *       echo "Hello World Hello Spark World Apache Spark Hello" > ~/spark_input/input.txt
 *
 *       → File at: ~/spark_input/input.txt
 *
 *  7. Go to folder where this file is saved:
 *       cd <path-to-this-file>
 *       Example: cd ~/scala/
 *
 *  8. Compile:
 *       scalac -classpath "$SPARK_HOME/jars/*" SparkWordCount.scala
 *
 *  9. Make a JAR:
 *       jar -cvf sparkwordcount.jar *.class
 *
 *  10. Run:
 *       spark-submit --class SparkWordCount --master local[*] sparkwordcount.jar ~/spark_input/input.txt
 *
 *       → Arg 1: ~/spark_input/input.txt  (your input file)
 *
 *       Output on screen:
 *         hello  -> 3
 *         world  -> 2
 *         spark  -> 2
 *         apache -> 1
 *
 * ============================================================ */
