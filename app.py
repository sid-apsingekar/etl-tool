# app.py
import streamlit as st
import pandas as pd
from urllib.parse import quote_plus
from etl_utils import extract_from_upload, dataset_overview, transform_dataframe, load_to_mysql, test_db_connection

st.set_page_config(page_title="Simple ETL Tool", layout="wide")

st.set_page_config(page_title="Simple ETL Tool", layout="wide")

# ----------------- SIDEBAR -----------------
with st.sidebar:
    st.markdown("## Developers")
    st.markdown("- Sidharth Apsingekar (A04)")
    st.markdown("- Swati Borade (A15)")


# ----------------- SESSION STATE -----------------
for key in ["df", "transformed", "mode"]:
    if key not in st.session_state:
        st.session_state[key] = None

st.title("ETL Tool — E (Extract) | T (Transform) | L (Load)")

# ----------------- E T L BUTTONS -----------------
col1, col2, col3 = st.columns([1, 1, 1])
with col1:
    if st.button("E - Extract", key="btn_extract"): st.session_state["mode"] = "extract"
with col2:
    if st.button("T - Transform", key="btn_transform"): st.session_state["mode"] = "transform"
with col3:
    if st.button("L - Load", key="btn_load"): st.session_state["mode"] = "load"

# ----------------- EXTRACT -----------------
if st.session_state["mode"] == "extract":
    st.header("Extract — Upload & preview dataset")
    uploaded = st.file_uploader("Choose a CSV or Excel file", type=['csv','xlsx','xls'], key="file_uploader")
    if uploaded:
        df = extract_from_upload(uploaded)
        st.session_state["df"] = df
        st.success(f"Loaded dataset: {df.shape[0]} rows x {df.shape[1]} cols")

    if st.session_state["df"] is not None:
        df = st.session_state["df"]
        ov = dataset_overview(df)
        st.subheader("Top rows")
        st.dataframe(ov["head"])
        st.subheader("Shape & dtypes")
        st.write("Rows, Columns:", ov["shape"])
        st.write(pd.DataFrame.from_dict(ov["dtypes"], orient='index', columns=['dtype']))
        st.subheader("Missing values")
        st.dataframe(ov["missing"])
        st.subheader("Summary statistics")
        st.dataframe(ov["describe"])

# ----------------- TRANSFORM -----------------
elif st.session_state["mode"] == "transform":
    st.header("Transform — choose operations")
    if st.session_state["df"] is None:
        st.info("No dataset in memory — go to **E - Extract** first.")
    else:
        df = st.session_state["df"]
        with st.expander("Preview original data", expanded=True):
            st.dataframe(df.head(50))

        st.subheader("Transform options")
        drop_dup = st.checkbox("Drop duplicates", value=False, key="drop_dup")
        drop_cols = st.multiselect("Drop columns", options=list(df.columns), default=[], key="drop_cols")
        fill_method = st.selectbox("Fill missing values", ["none", "mean", "median", "mode", "value"], key="fill_method")
        fill_value = st.text_input("Fill with (literal value)", "", key="fill_value") if fill_method=="value" else None
        encode_cols = st.multiselect("One-hot encode columns", list(df.select_dtypes(include=['object','category']).columns), key="encode_cols")
        scale_cols = st.multiselect("Scale numeric columns (StandardScaler)", list(df.select_dtypes(include=['number']).columns), key="scale_cols")

        rename_map = {}
        if st.checkbox("Rename columns manually", key="rename_checkbox"):
            col_to_rename = st.selectbox("Column to rename", list(df.columns), key="col_to_rename")
            new_name = st.text_input("New name for column", value=col_to_rename, key="new_name")
            if st.button("Apply rename", key="apply_rename"):
                rename_map = {col_to_rename: new_name}

        options = {
            "drop_duplicates": drop_dup,
            "drop_columns": drop_cols,
            "fill_missing": {"strategy": fill_method if fill_method!="none" else None, "value": fill_value},
            "encode_columns": encode_cols,
            "scale_numeric": scale_cols,
            "rename_map": rename_map
        }

        if st.button("Run transform", key="run_transform"):
            transformed = transform_dataframe(df, options)
            st.session_state["transformed"] = transformed
            st.success("Transformation applied — preview below")

        if st.session_state.get("transformed") is not None:
            st.subheader("Transformed preview")
            st.dataframe(st.session_state["transformed"].head(50))
            csv = st.session_state["transformed"].to_csv(index=False).encode('utf-8')
            st.download_button("Download transformed CSV", data=csv, file_name="transformed.csv", mime="text/csv", key="download_csv")

# ----------------- LOAD -----------------
elif st.session_state["mode"] == "load":
    st.header("Load — push to MySQL")

    # Choose which DataFrame to load
    df_to_load = st.session_state.get("transformed") if st.session_state.get("transformed") is not None else st.session_state.get("df")

    if df_to_load is None:
        st.info("No dataset ready to load — upload/transform first.")
    else:
        st.write("Rows:", df_to_load.shape[0], "Cols:", df_to_load.shape[1])
        table_name = st.text_input("Target MySQL table name", "etl_table", key="table_name")
        use_secrets = st.checkbox("Use Streamlit secrets (.streamlit/secrets.toml)", True, key="use_secrets")

        db_url = None
        if use_secrets:
            mysql_conf = st.secrets.get("mysql", {})
            user = mysql_conf.get("user")
            password = mysql_conf.get("password", "")
            host = mysql_conf.get("host", "localhost")
            port = mysql_conf.get("port", "3306")
            dbname = mysql_conf.get("db")
            if user and dbname:
                db_url = f"mysql+pymysql://{user}:{quote_plus(password)}@{host}:{port}/{dbname}"
            else:
                st.error("Please set both 'user' and 'db' in .streamlit/secrets.toml")
        else:
            db_url = st.text_input("Enter SQLAlchemy DB URL (mysql+pymysql://user:pass@host:3306/dbname)", key="db_url_input")

        if st.button("Test connection", key="test_connection"):
            if not db_url: st.error("No DB URL provided.")
            else:
                ok, msg = test_db_connection(db_url)
                st.success(msg) if ok else st.error(msg)

        if st.button("Load to MySQL", key="load_mysql"):
            if not db_url:
                st.error("No DB URL provided.")
            else:
                with st.spinner("Loading to MySQL..."):
                    # Replace table each time (new dataset)
                    success, msg = load_to_mysql(df_to_load, table_name, db_url, if_exists='replace')
                    st.success(msg) if success else st.error(f"Load failed: {msg}")

st.markdown("---")
st.caption("ETL flow: upload (E) → transform (T) → load (L). Store DB creds in .streamlit/secrets.toml for safety.")
