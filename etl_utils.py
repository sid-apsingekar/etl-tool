# etl_utils.py
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import re

# ----------------- EXTRACT -----------------
def extract_from_upload(uploaded_file):
    if uploaded_file is None: return None
    try:
        if uploaded_file.name.endswith(".csv"):
            return pd.read_csv(uploaded_file)
        else:
            return pd.read_excel(uploaded_file)
    except Exception as e:
        raise ValueError(f"Failed to read file: {e}")

# ----------------- DATASET OVERVIEW -----------------
def dataset_overview(df):
    overview = {}
    overview["shape"] = df.shape
    overview["dtypes"] = df.dtypes.astype(str).to_dict()
    overview["head"] = df.head(10)
    overview["describe"] = df.describe(include='all').transpose()
    missing = df.isna().sum()
    overview["missing"] = pd.DataFrame({
        "missing_count": missing,
        "missing_pct": (missing / len(df) * 100).round(2)
    }).sort_values("missing_pct", ascending=False)
    return overview

# ----------------- TRANSFORM -----------------
def transform_dataframe(df, options):
    df2 = df.copy()
    
    # Drop duplicates
    if options.get("drop_duplicates"):
        df2 = df2.drop_duplicates()
    
    # Drop columns
    if options.get("drop_columns"):
        df2 = df2.drop(columns=options["drop_columns"], errors='ignore')

    # Fill missing
    fm = options.get("fill_missing", {})
    strategy = fm.get("strategy")
    value = fm.get("value")
    if strategy == "mean":
        for c in df2.select_dtypes(include=['number']).columns:
            df2[c] = df2[c].fillna(df2[c].mean())
    elif strategy == "median":
        for c in df2.select_dtypes(include=['number']).columns:
            df2[c] = df2[c].fillna(df2[c].median())
    elif strategy == "mode":
        for c in df2.columns:
            if not df2[c].mode().empty:
                df2[c] = df2[c].fillna(df2[c].mode()[0])
    elif strategy == "value":
        df2 = df2.fillna(value)

    # Rename columns
    if options.get("rename_map"):
        df2 = df2.rename(columns=options["rename_map"])

    # One-hot encode
    if options.get("encode_columns"):
        df2 = pd.get_dummies(df2, columns=options["encode_columns"], dummy_na=False)

    # Scale numeric columns
    if options.get("scale_numeric"):
        scaler = StandardScaler()
        numeric_cols = [c for c in options["scale_numeric"] if c in df2.columns]
        if numeric_cols:
            df2[numeric_cols] = scaler.fit_transform(df2[numeric_cols].astype(float))

    return df2

# ----------------- LOAD -----------------
def load_to_mysql(df, table_name, db_url, if_exists='replace'):
    try:
        engine = create_engine(db_url)
        
        # Sanitize column names to be SQL-friendly
        df_safe = df.copy()
        df_safe.columns = [re.sub(r'\W+', '_', str(c)).lower() for c in df_safe.columns]
        
        df_safe.to_sql(table_name, con=engine, index=False, if_exists=if_exists)
        return True, f"✅ Data loaded into MySQL table `{table_name}` successfully!"
    except SQLAlchemyError as e:
        return False, str(e)

# ----------------- TEST CONNECTION -----------------
def test_db_connection(db_url):
    try:
        engine = create_engine(db_url)
        conn = engine.connect()
        conn.close()
        return True, "Connection successful!"
    except SQLAlchemyError as e:
        return False, str(e)
