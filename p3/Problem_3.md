# Problem 3: Recommender Systems via Matrix Factorization

## Explanation of the Code

The implementation in `Problem3.scala` shows a Spark MLlib pipeline for collaborative filtering using the Alternating Least Squares algorithm on the AudioScrobbler dataset.

1.  **Data preprocessing and alias resolution:**
    The code first loads the artist data and aliases. It uses a broadcast variable to efficiently map misspelled or redundant artist IDs to their canonical versions.

2.  **User filtering:**
    To ensure sufficient data for meaningful recommendations, the dataset is filtered to include only "active" users who have listened to at least 100 distinct artists.

3.  **Stratified data splitting:**
    Instead of a global random split, the code performs a per-user 80/20 split. This ensures that for every user in the dataset, 80% of their listening history is used for training and the remaining 20% is reserved for evaluating the model's ability to "rediscover" those artists.

4.  **Baseline Model (Most Popular):**
    A baseline is established by recommending the most frequently played artists globally. This provides a lower bound for the AUC metric to compare against the matrix factorization model.

5.  **Hyperparameter tuning with Cross-Validation:**
    The script implements a manual grid search over three dimensions: `rank` (number of latent features), `lambda` (regularization), and `alpha` (confidence weight for implicit feedback). To prevent overfitting, a **5-fold cross-validation** is applied to the training data. The training set is partitioned into 5 folds, and for each parameter set, the model is trained on 4 folds and validated on the 5th.

6.  **Performance optimization:**
    Due to the scale of the dataset, we implemented a custom `localAUC` helper and used `recommendProductsForUsers(50)` to batch the evaluation process.

7.  **Synthetic User Recommendations:**
    Finally, the code demonstrates the system's utility by adding a new user profile with a few manual artist ratings, retraining the model with the best found parameters, and outputting the top-25 personalized artist recommendations.

## Summary of Results

Due to the intensive computational requirements of the ALS algorithm and the scale of the AudioScrobbler dataset, we were able to successfully implement the full pipeline, including the data processing, baseline evaluation, and the infrastructure for hyperparameter tuning. However, the grid search with cross-validation exceeded the available execution we had before submitting.

### Baseline Evaluation

We computed the baseline recommendation quality using the "Most Popular" approach (recommending the artists with the highest total play counts across all users).

- **Average AUC (Most Popular):** 0.5236801036135709

This value serves as a reference point. A value slightly above 0.5 indicates that the most popular artists have a slightly better than random chance of being listened to by a specific user, but it lacks the personalization that matrix factorization provides.

## Computational Challenges and Argument for Partial Results

The implementation includes a manual grid search for the following hyperparameters:

- **Rank:** `[10, 25, 50]`
- **Lambda:** `[1.0, 0.1, 0.01]`
- **Alpha:** `[1.0, 10.0, 100.0]`

With 27 unique parameter combinations and a **5-fold cross-validation** strategy, the system is required to train and evaluate a total of **135 models**.

During testing on the AION cluster, we observed the following performance bottlenecks:

1. **Model Training:** Each ALS training iteration (even with only 5 iterations) takes several minutes given the dataset size.
2. **Evaluation Bottleneck:** The step `model.recommendProductsForUsers(50)` followed by the join and AUC calculation takes approximately **10 minutes** per iteration.
3. **Total Estimated Time:** 135 iterations \* 10 minutes = 1,350 minutes (**22.5 hours**).

## Instructions to Run

To run the solution, follow these steps:

1.  **Data Placement:** Ensure the AudioScrobbler dataset files (`artist_alias.txt`, `artist_data.txt`, `user_artist_data.txt`) are placed in the `./data/` directory relative to the project root.
2.  **Build:** Use `sbt` to package the application:
    ```bash
    sbt clean package
    ```
3.  **Execute:** Run the provided `run.sh` script or use `spark-submit`:

    ```bash
    ./run.sh
    ```

    \_Note: A slrum script is provided for envirmonment setup and execution on AION.
