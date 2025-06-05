import pandas as pd
import pickle
import logging
import json
from datetime import datetime
from pathlib import Path
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GC Predictor")

def load_data(train_path: str, test_path: str):
    logger.info("📦 Loading train and test datasets from Parquet...")
    train_df = pd.read_parquet(train_path)
    test_df = pd.read_parquet(test_path)
    return train_df, test_df

def train_model(X_train, y_train):
    logger.info("🧠 Training RandomForest model...")
    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("rf", RandomForestClassifier(n_estimators=100, random_state=42))
    ])
    pipeline.fit(X_train, y_train)
    return pipeline

def evaluate_model(model, X_test, y_test):
    logger.info("📊 Evaluating model performance...")
    y_pred = model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    logger.info(f"✅ Accuracy: {acc:.4f}")
    logger.info("🧾 Classification Report:")
    logger.info("\n" + classification_report(y_test, y_pred))
    return acc

def save_model(model, output_path: str):
    logger.info(f"💾 Saving model to {output_path}...")
    with open(output_path, "wb") as f:
        pickle.dump(model, f)
    logger.info("🎉 Model saved successfully.")

def save_metrics(accuracy: float, output_path: str):
    logger.info(f"📈 Saving accuracy metrics to {output_path}...")
    metrics = {
        "accuracy": accuracy,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(metrics, f)
    logger.info("📝 Metrics saved.")

def main():
    train_path = "s3a://processed/features_train.parquet"
    test_path = "s3a://processed/features_test.parquet"
    model_output = "ds_model/gc_rf_model.pkl"
    metrics_output = "/opt/airflow/output/metrics.json"  # For Airflow to check

    train_df, test_df = load_data(train_path, test_path)

    features = ["length", "a_count", "t_count", "c_count", "g_count", "gc_content"]
    target = "label"

    X_train, y_train = train_df[features], train_df[target]
    X_test, y_test = test_df[features], test_df[target]

    model = train_model(X_train, y_train)
    accuracy = evaluate_model(model, X_test, y_test)
    save_model(model, model_output)
    save_metrics(accuracy, metrics_output)

    logger.info("✅ Model pipeline complete.")

if __name__ == "__main__":
    main()