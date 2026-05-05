# Problem 1: Predicting Heart Diseases via Decision Trees & Random Forests

## AI usage

I used ChatGPT to help structure the PySpark solution, including preprocessing, feature engineering, Spark ML pipelines, hyper-parameter tuning, and metric calculation.

The main prompting steps were:

- creating a step-by-step PySpark solution for Problem 1,
- debugging CSV loading and preprocessing,
- fixing the label-indexing issue caused by using `handleInvalid="keep"` on the target label,
- implementing `CrossValidator`, `TrainValidationSplit`, and `RandomForestClassifier`,
- preparing and cleaning the final report.

## Run instructions

The experiments were executed on the IRIS HPC cluster using Spark 3.5.4.

The Spark module was loaded with:

```bash
module load devel/Spark/3.5.4-foss-2023b-Java-17
```

The following commands were used:

```bash
spark-submit Problem1_cv_full.py heart_2020_cleaned.csv > cv_full_output.txt 2>&1
spark-submit Problem1_tvs_full.py heart_2020_cleaned.csv > tvs_full_output.txt 2>&1
sbatch run_rf_age.sbatch
```

The RandomForest experiment for Part (d) was submitted with `sbatch` because the interactive HPC session reached its time limit.

---

## Part (a): Loading and preprocessing the dataset

The dataset `heart_2020_cleaned.csv` was loaded into a Spark DataFrame using:

```python
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)
```

The loaded DataFrame contained:

```text
Rows: 319795
Columns: 18
```

The columns were:

```text
HeartDisease, BMI, Smoking, AlcoholDrinking, Stroke, PhysicalHealth,
MentalHealth, DiffWalking, Sex, AgeCategory, Race, Diabetic,
PhysicalActivity, GenHealth, SleepTime, Asthma, KidneyDisease, SkinCancer
```

The numeric feature columns were:

```text
BMI, PhysicalHealth, MentalHealth, SleepTime
```

All categorical feature columns were converted using `StringIndexer`, and all features were assembled into a single feature vector using `VectorAssembler`.


The resulting label mapping was:

```text
No  -> 0.0
Yes -> 1.0
```

---

## Binary evaluation

 `BinaryClassificationMetrics` does not directly provide plain accuracy, confusion matrix was computed manually from the final predictions and used TP, TN, FP, and FN to calculate precision, recall, and accuracy.

---

## Part (b): DecisionTree with 5-fold CrossValidator

### Setup

```text
Dataset rows: 319795
Dataset columns: 18

Training rows: 256073
Testing rows: 63722
```

The following hyper-parameter was used:

```text
impurity = entropy, gini
maxDepth = 6, 12, 24
maxBins = 20, 50, 100
```

This resulted in:

```text
Parameter combinations: 18
Cross-validation folds: 5
Total model trainings: 90
```

### Best model

```text
impurity = entropy
maxDepth = 6
maxBins = 20
```


### Test results

```text
TP = 256
TN = 58049
FP = 211
FN = 5206

Precision = 0.5481798715203426
Recall = 0.046869278652508235
Accuracy = 0.9149901133046672
AUC ROC = 0.781472115554674
AUC PR = 0.2680312689447007
```

### Average per-class accuracy

```text
No class accuracy  = TN / (TN + FP)
                   = 58049 / (58049 + 211)
                   = 0.9963783041572262

Yes class accuracy = TP / (TP + FN)
                   = 256 / (256 + 5206)
                   = 0.046869278652508235

Average per-class accuracy
                   = 0.5216237914048672
```

### Runtime

```text
Training runtime = 1281.9965057373047 seconds
Total runtime = 1291.990782737732 seconds
```

---

## Part (c): DecisionTree with TrainValidationSplit

### Setup

```text
Dataset rows: 319795
Dataset columns: 18

Training rows: 256073
Testing rows: 63722
```

The same hyper-parameter used:

```text
impurity = entropy, gini
maxDepth = 6, 12, 24
maxBins = 20, 50, 100
```

The TrainValidationSplit configuration was:

```text
trainRatio = 0.8
Parameter combinations = 18
Total model trainings = 18
```

### Best model

```text
impurity = entropy
maxDepth = 6
maxBins = 20
```

### Test results

```text
TP = 256
TN = 58049
FP = 211
FN = 5206

Precision = 0.5481798715203426
Recall = 0.046869278652508235
Accuracy = 0.9149901133046672
AUC ROC = 0.781472115554674
AUC PR = 0.2680312689447007
```

### Average per-class accuracy

```text
No class accuracy = 0.9963783041572262
Yes class accuracy = 0.046869278652508235
Average per-class accuracy = 0.5216237914048672
```

### Runtime

```text
Training runtime = 350.7296690940857 seconds
Total runtime = 361.78817081451416 seconds
```

### Comparison with CrossValidator

Both `CrossValidator` and `TrainValidationSplit` selected the same best hyper-parameter setting:

```text
impurity = entropy
maxDepth = 6
maxBins = 20
```

They also achieved the almost same test performance:

```text
Accuracy = 0.9149901133046672
Precision = 0.5481798715203426
Recall = 0.046869278652508235
AUC ROC = 0.781472115554674
```

However, `TrainValidationSplit` was much faster. The 5-fold `CrossValidator` trained 90 models and took about 1291.99 seconds, while `TrainValidationSplit` trained only 18 models and took about 361.79 seconds.

Therefore, `TrainValidationSplit` was approximately 3.57 times faster in this experiment while selecting the same model and achieving the same test metrics.

---

## Part (d): RandomForest with AgeCategory as target label

For this part, the target label was changed from `HeartDisease` to `AgeCategory`.

### Setup

```text
Target label: AgeCategory
Classifier: RandomForestClassifier
Evaluation: MulticlassClassificationEvaluator and MulticlassMetrics

Dataset rows: 319795
Dataset columns: 18

Training rows: 256074
Testing rows: 63721
```

Hyper-parameter  used:

```text
impurity = entropy, gini
maxDepth = 6, 12, 24
maxBins = 20, 50, 100
numTrees = 5, 10, 20
```

This resulted in:

```text
Parameter combinations: 54
Cross-validation folds: 5
Total model trainings: 270
```

### Best model

```text
impurity = gini
maxDepth = 12
maxBins = 50
numTrees = 20
```

### Test results

```text
Accuracy = 0.17626842014406555
F1 = 0.14554407459584845
Weighted precision = 0.16604726535582084
Weighted recall = 0.17626842014406555
```

### AgeCategory label mapping

```text
0 -> 65-69
1 -> 60-64
2 -> 70-74
3 -> 55-59
4 -> 50-54
5 -> 80 or older
6 -> 45-49
7 -> 75-79
8 -> 18-24
9 -> 40-44
10 -> 35-39
11 -> 30-34
12 -> 25-29
```

### Runtime

```text
Training runtime = 14439.940875768661 seconds
Total runtime = 14445.74688076973 seconds
```

### Interpretation

The RandomForest classifier selected `gini` impurity, `maxDepth = 12`, `maxBins = 50`, and `numTrees = 20` as the best hyper-parameter setting.

The final accuracy was approximately `0.1763`, and the weighted F1 score was approximately `0.1455`. This performance is much lower than the binary `HeartDisease` classification task because `AgeCategory` is a multiclass prediction problem with 13 possible age groups. Many neighboring age groups are difficult to distinguish using only the available health-related attributes.

The long runtime is expected because the experiment evaluated 54 hyper-parameter combinations with 5-fold cross-validation, resulting in 270 RandomForest model trainings. Since each RandomForest model itself trains multiple decision trees, this was the most computationally expensive part of Problem 1.
