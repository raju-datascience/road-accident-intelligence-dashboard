import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer


# --------------------------------------------------
# 1️⃣ LOAD DATA
# --------------------------------------------------

def load_data(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    print("✅ Data loaded successfully")
    print("Shape:", df.shape)
    return df


# --------------------------------------------------
# 2️⃣ DROP HIGH-MISSING COLUMNS
# --------------------------------------------------

def drop_high_missing_columns(df: pd.DataFrame, threshold: float = 0.40):

    df = df.copy()

    missing_percent = df.isnull().mean()

    cols_to_drop = missing_percent[missing_percent > threshold].index.tolist()

    if cols_to_drop:
        print("\n⚠ Dropping columns with >", threshold * 100, "% missing:")
        for col in cols_to_drop:
            print(" -", col)

        df = df.drop(columns=cols_to_drop)

    return df


# --------------------------------------------------
# 3️⃣ CLEAN MISSING VALUES (Robust Version)
# --------------------------------------------------

def clean_missing_values(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    # Structural zero columns
    structural_zero_cols = [
        "cycle_time_s",
        "violations_count"
    ]

    for col in structural_zero_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    # Separate numeric and categorical
    numeric_cols = df.select_dtypes(include=np.number).columns.tolist()
    categorical_cols = df.select_dtypes(include="object").columns.tolist()

    # Remove target from numeric handling
    if "accident_occurred" in numeric_cols:
        numeric_cols.remove("accident_occurred")

    # -----------------------------
    # Numeric columns → median
    # -----------------------------
    if numeric_cols:
        median_imputer = SimpleImputer(strategy="median")
        df[numeric_cols] = median_imputer.fit_transform(df[numeric_cols])

    # -----------------------------
    # Categorical columns → "Unknown"
    # -----------------------------
    for col in categorical_cols:
        df[col] = df[col].fillna("Unknown")

    # Final check
    remaining_missing = df.isnull().sum().sum()

    print("\nRemaining missing values after cleaning:", remaining_missing)

    return df


# --------------------------------------------------
# 4️⃣ FEATURE ENGINEERING (Dashboard Version)
# --------------------------------------------------

def engineer_features_for_dashboard(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()

    # Convert timestamp
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    df["hour"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.day_name()
    df["month"] = df["timestamp"].dt.month
    df["day_of_week_num"] = df["timestamp"].dt.dayofweek

    df["is_weekend"] = df["day_of_week_num"].isin([5, 6]).astype(int)

    # -------------------------
    # Speed Buckets
    # -------------------------
    if "avg_speed_kmph" in df.columns:
        df["speed_bucket"] = pd.cut(
            df["avg_speed_kmph"],
            bins=[0, 30, 60, 200],
            labels=["Low Speed", "Medium Speed", "High Speed"]
        )

    # -------------------------
    # Traffic Buckets
    # -------------------------
    if "vehicle_count_per_hr" in df.columns:
        df["traffic_bucket"] = pd.cut(
            df["vehicle_count_per_hr"],
            bins=[0, 50, 100, 1000],
            labels=["Low Traffic", "Medium Traffic", "High Traffic"]
        )

    # -------------------------
    # Blackspot Buckets
    # -------------------------
    if "blackspot_score" in df.columns:
        df["blackspot_bucket"] = pd.cut(
            df["blackspot_score"],
            bins=[0, 3, 7, 10],
            labels=["Low Risk Zone", "Medium Risk Zone", "High Risk Zone"]
        )

    print("✅ Feature engineering completed")

    return df


# --------------------------------------------------
# 5️⃣ ACCIDENT RATE HELPER
# --------------------------------------------------

def calculate_accident_rate(df: pd.DataFrame, group_col: str):

    result = (
        df.groupby(group_col)
          .agg(
              total_records=("accident_occurred", "count"),
              total_accidents=("accident_occurred", "sum")
          )
          .reset_index()
    )

    result["accident_rate"] = (
        result["total_accidents"] / result["total_records"]
    )

    return result.sort_values("accident_rate", ascending=False)


# --------------------------------------------------
# 6️⃣ EXPORT DATA
# --------------------------------------------------

def export_clean_data(df: pd.DataFrame, output_path: str):

    df.to_csv(output_path, index=False)
    print(f"✅ Clean analytics dataset saved to {output_path}")


# --------------------------------------------------
# 7️⃣ MAIN EXECUTION
# --------------------------------------------------

if __name__ == "__main__":

    DATA_PATH = "data/raw/traffic_accidents.csv"
    OUTPUT_PATH = "data/processed/traffic_analytics_ready.csv"

    df = load_data(DATA_PATH)

    df = drop_high_missing_columns(df, threshold=0.40)

    df_clean = clean_missing_values(df)

    df_analytics = engineer_features_for_dashboard(df_clean)

    print("\nSample accident rate by weather:")
    if "weather" in df_analytics.columns:
        print(calculate_accident_rate(df_analytics, "weather"))

    export_clean_data(df_analytics, OUTPUT_PATH)

    print("\n🚀 Analytics dataset ready for MySQL & Power BI")
