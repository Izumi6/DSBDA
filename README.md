# 📘 DSBDA — Data Science & Big Data Analytics

Complete practical programs for DSBDA (Data Science & Big Data Analytics) course.  
Includes Python notebooks (Practical 1–10) with datasets, Hadoop MapReduce (Java), and Apache Spark (Scala).

---

## 📂 Repository Structure

```
DSBDA/
│
├── pra1/
│   ├── pr 1.ipynb              ← Practical 1 Notebook
│   └── titanic.csv             ← Dataset
│
├── pra 2/
│   ├── pr 2.ipynb              ← Practical 2 Notebook
│   └── academic_performance.csv← Dataset
│
├── pra 3/
│   ├── pr 3.ipynb              ← Practical 3 Notebook
│   ├── loan_data_set.csv       ← Dataset
│   └── iris.csv                ← Dataset
│
├── pra 4/
│   ├── pr 4.ipynb              ← Practical 4 Notebook
│   └── BostonHousing.csv       ← Dataset
│
├── pra 5/
│   ├── pr 5.ipynb              ← Practical 5 Notebook
│   └── Social_Network_Ads.csv  ← Dataset
│
├── pra 6/
│   ├── pr 6.ipynb              ← Practical 6 Notebook
│   └── iris.csv                ← Dataset
│
├── pra 7/
│   └── pr 7.ipynb              ← Practical 7 Notebook (no dataset needed)
│
├── pra 8/
│   ├── pr 8.ipynb              ← Practical 8 Notebook
│   └── titanic.csv             ← Dataset
│
├── pra 9/
│   ├── pr 9.ipynb              ← Practical 9 Notebook
│   └── titanic.csv             ← Dataset
│
├── pra 10/
│   ├── pr 10.ipynb             ← Practical 10 Notebook
│   └── iris.csv                ← Dataset
│
├── hadoop/
│   ├── WordCount.java          ← Hadoop MapReduce Word Count
│   └── LogProcessor.java       ← Hadoop MapReduce Log Processor
│
├── scala/
│   └── SparkWordCount.scala    ← Apache Spark Word Count (Scala)
│
└── README.md
```

---

## 📝 Practical List

| # | Topic | Dataset | Folder |
|---|-------|---------|--------|
| 1 | Data Wrangling — Import, missing values, outliers, normalization | `titanic.csv` | `pra1/` |
| 2 | Data Wrangling — Variable transformation, type conversion, z-score | `academic_performance.csv` | `pra 2/` |
| 3 | Descriptive Statistics — Summary stats, grouping, mean/median/mode | `loan_data_set.csv`, `iris.csv` | `pra 3/` |
| 4 | Linear Regression — Regression model, MSE, R² score | `BostonHousing.csv` | `pra 4/` |
| 5 | Logistic Regression — Confusion matrix, accuracy, precision, recall | `Social_Network_Ads.csv` | `pra 5/` |
| 6 | Naive Bayes Classifier — Classification report, accuracy metrics | `iris.csv` | `pra 6/` |
| 7 | Text Analytics & NLP — Tokenization, stemming, lemmatization, TF-IDF | No dataset (inline text) | `pra 7/` |
| 8 | Data Visualization I — Survival plots, histograms, heatmaps | `titanic.csv` | `pra 8/` |
| 9 | Data Visualization II — Box plots, scatter plots, survival analysis | `titanic.csv` | `pra 9/` |
| 10 | Data Visualization III — Species distribution, pairplots | `iris.csv` | `pra 10/` |
| 11 | Hadoop WordCount — Count word occurrences using MapReduce (Java) | Text input (create in terminal) | `hadoop/` |
| 12 | Hadoop Log Processor — Count log levels using MapReduce (Java) | Log input (create in terminal) | `hadoop/` |
| 13 | Spark WordCount — Count words using Apache Spark (Scala) | Text input (create in terminal) | `scala/` |

---

## ⚡ How to Run

### Python Notebooks (Practical 1–10)
1. Open the `.ipynb` file in **Jupyter Notebook** or **Google Colab**
2. The dataset is already in the same folder — just run all cells
3. No extra setup needed

### Hadoop Programs (Practical 11–12)
1. Open `WordCount.java` or `LogProcessor.java`
2. **Scroll to the bottom** — terminal commands are in the comments
3. Follow the steps one by one in your Ubuntu terminal
4. Commands start from `hadoop version` (Hadoop must be pre-installed)

### Spark Program (Practical 13)
1. Open `SparkWordCount.scala`
2. **Scroll to the bottom** — install + run commands are in the comments
3. Follow the steps to install Spark and run the program

---

## 🛠 Technologies Used

| Technology | Practicals |
|------------|------------|
| Python, Pandas, NumPy | 1–10 |
| Scikit-learn | 4, 5, 6 |
| Matplotlib, Seaborn | 2, 4, 8, 9, 10 |
| NLTK | 7 |
| Java + Hadoop MapReduce | 11, 12 |
| Scala + Apache Spark | 13 |

---

## 📄 License

Free to use for educational purposes.
