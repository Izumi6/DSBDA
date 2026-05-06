# DSBDA — Data Science & Big Data Analytics

Hadoop MapReduce and Apache Spark programs for Big Data practicals.

---

## 📁 Folder Structure

```
├── hadoop/
│   ├── WordCount.java       ← Word Count using Hadoop MapReduce
│   └── LogProcessor.java    ← System Log Processor using MapReduce
│
└── scala/
    └── SparkWordCount.scala ← Word Count using Apache Spark (Scala)
```

---

## 📝 Programs

### 1. WordCount.java (Hadoop MapReduce)
Counts how many times each word appears in a text file using Hadoop MapReduce on a local standalone setup.

### 2. LogProcessor.java (Hadoop MapReduce)
Processes a system log file and counts the occurrences of each log level — `INFO`, `WARN`, `ERROR`, `DEBUG`, `FATAL`.

### 3. SparkWordCount.scala (Apache Spark)
Counts word occurrences in a text file using Apache Spark's RDD API in Scala.

---

## ⚡ Quick Start

Each file contains **step-by-step terminal commands** at the bottom as comments.  
Just open the file and scroll to the bottom for instructions.

### Hadoop files (WordCount & LogProcessor):
- Hadoop already installed? Start from `hadoop version`
- Commands cover: compile → JAR → run → view output

### Spark file (SparkWordCount):
- Commands cover: install Java/Scala/Spark → compile → JAR → run

---

## 🛠 Prerequisites

| Tool | Hadoop Programs | Spark Program |
|------|:-:|:-:|
| Java JDK 8 | ✅ | ✅ |
| Hadoop 3.x | ✅ | — |
| Scala | — | ✅ |
| Apache Spark | — | ✅ |

---

## 📄 License

Free to use for educational purposes.
