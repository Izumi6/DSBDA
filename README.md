# 📘 DSBDA — Data Science & Big Data Analytics

Complete practical programs for DSBDA (Data Science & Big Data Analytics) course.  
Includes Python notebooks (Pra 1–10), Hadoop MapReduce (Java), and Apache Spark (Scala).

---

## 📂 Repository Structure

```
DSBDA/
│
├── pra1/           → Practical 1:  Data Wrangling (Titanic Dataset)
├── pra 2/          → Practical 2:  Data Wrangling (Academic Performance)
├── pra 3/          → Practical 3:  Descriptive Statistics (Loan Dataset)
├── pra 4/          → Practical 4:  Linear Regression (Boston Housing)
├── pra 5/          → Practical 5:  Logistic Regression (Classification)
├── pra 6/          → Practical 6:  Naive Bayes Classifier
├── pra 7/          → Practical 7:  Text Analytics & NLP (TF-IDF)
├── pra 8/          → Practical 8:  Data Visualization (Titanic)
├── pra 9/          → Practical 9:  Data Visualization (Box & Scatter Plots)
├── pra 10/         → Practical 10: Data Visualization (Iris Dataset)
│
├── hadoop/
│   ├── WordCount.java       → Hadoop MapReduce Word Count
│   └── LogProcessor.java    → Hadoop MapReduce Log Processor
│
├── scala/
│   └── SparkWordCount.scala → Apache Spark Word Count (Scala)
│
└── README.md
```

---

## 📝 Practical List

| # | Practical | Topic | File |
|---|-----------|-------|------|
| 1 | Data Wrangling I | Import, handling missing values, outliers, data types, normalization (Titanic) | `pra1/pr 1.ipynb` |
| 2 | Data Wrangling II | Academic dataset — variable transformation, type conversion, z-score | `pra 2/pr 2.ipynb` |
| 3 | Descriptive Statistics | Loan dataset — summary stats, grouping, mean/median/mode, std dev | `pra 3/pr 3.ipynb` |
| 4 | Linear Regression | Boston Housing — regression model, MSE, R² score, predictions | `pra 4/pr 4.ipynb` |
| 5 | Logistic Regression | Classification — confusion matrix, accuracy, precision, recall | `pra 5/pr 5.ipynb` |
| 6 | Naive Bayes | Gaussian Naive Bayes — classification report, accuracy metrics | `pra 6/pr 6.ipynb` |
| 7 | Text Analytics | NLP — tokenization, stop words, stemming, lemmatization, TF-IDF | `pra 7/pr 7.ipynb` |
| 8 | Data Visualization I | Titanic — survival plots, histograms, heatmaps | `pra 8/pr 8.ipynb` |
| 9 | Data Visualization II | Titanic — box plots, scatter plots, survival analysis | `pra 9/pr 9.ipynb` |
| 10 | Data Visualization III | Iris — species distribution, pairplots, feature analysis | `pra 10/pr 10.ipynb` |
| 11 | Hadoop WordCount | MapReduce — count word occurrences in text file (Java) | `hadoop/WordCount.java` |
| 12 | Hadoop Log Processor | MapReduce — count log levels in system log file (Java) | `hadoop/LogProcessor.java` |
| 13 | Spark WordCount | Apache Spark — count word occurrences in text file (Scala) | `scala/SparkWordCount.scala` |

---

## ⚡ Quick Start

### Python Notebooks (Practical 1–10)
1. Open any `.ipynb` file in **Jupyter Notebook** or **Google Colab**
2. Make sure required datasets (titanic.csv, iris.csv, etc.) are in the same folder
3. Run all cells

### Hadoop Programs (Practical 11–12)
1. Open `WordCount.java` or `LogProcessor.java`
2. Scroll to the bottom → **terminal commands are written in the comments**
3. Follow the steps one by one in your Ubuntu terminal
4. Hadoop should already be installed on your system

### Spark Program (Practical 13)
1. Open `SparkWordCount.scala`
2. Scroll to the bottom → **installation + run commands are in the comments**
3. Follow the steps to install Spark and run the program

---

## 🛠 Technologies Used

| Technology | Used In |
|------------|---------|
| Python (Pandas, NumPy, Scikit-learn, Matplotlib, Seaborn) | Practicals 1–10 |
| NLTK | Practical 7 |
| Java + Hadoop MapReduce | Practicals 11–12 |
| Scala + Apache Spark | Practical 13 |

---

## 📋 Datasets Required

| Dataset | Used In |
|---------|---------|
| `titanic.csv` | Practical 1, 8, 9 |
| `loan_data_set.csv` | Practical 3 |
| `BostonHousing.csv` | Practical 4 |
| `iris.csv` | Practical 10 |

> **Note:** Datasets are not included in this repo. Download them from Kaggle or your course materials and place them in the respective practical folders.

---

## 📄 License

Free to use for educational purposes.
