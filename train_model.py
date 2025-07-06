import pandas as pd
import os
import joblib
from sklearn.ensemble import RandomForestRegressor

def train():
    # Load all features
    folder = "feature_store"
    dfs = []
    for file in os.listdir(folder):
        if file.endswith(".parquet"):
            dfs.append(pd.read_parquet(os.path.join(folder, file)))

    if not dfs:
        raise ValueError("No feature files found!")

    df = pd.concat(dfs)
    df = df.sort_index()

    # Drop rows with missing target
    df = df.dropna(subset=["aqi"])

    # Define features and target
    feature_cols = [col for col in df.columns if col not in ["aqi"]]
    X = df[feature_cols]
    y = df["aqi"]

    # Train model
    model = RandomForestRegressor()
    model.fit(X, y)

    # ✅ Make sure model folder exists
    os.makedirs("model", exist_ok=True)

    # Save model
    joblib.dump(model, "model/latest_model.joblib")
    print("✅ Model trained and saved.")

if __name__ == "__main__":
    train()
