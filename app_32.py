# v32 - AWS Lambda + API Gateway Integration
# Connects to AWS backend for ultra-fast processing (5-15 seconds vs 178 seconds)

# v31f - Process Timeframes Separately" Option add.  with one high and one low range, current process take 200+ seconds.  
# this mod should bring it down to under 90 seconds.

# v31e - Processing time optimization to remove redundant data points.  Zone Sorting Fix #2

# v31d - Asset ID in Filename; for Custom Ranges path, Changed sort order from ['Group', 'Output', 'Arrival'] to ['Range', 'Group', 'Output', 'Arrival']

# v31c - "Most Current" mode: Now uses dt.datetime.now() as the report time (current timestamp)
# This ensures that report_time always has a valid datetime value when processing, preventing the NaN conversion error.

# v31b - Updated file uploads (7 files), added asset selector, modified custom ranges to single tab output
# Previous: v30b - Added HOD/LOD report mode with multi-day processing

import streamlit as st
import pandas as pd
import datetime as dt
import io
import requests
import boto3
from typing import Optional
from pandas import ExcelWriter

# Configure pandas
pd.set_option("styler.render.max_elements", 2000000)

# AWS Configuration
AWS_REGION = "us-east-2"
API_ENDPOINT = "https://5c9t51huga.execute-api.us-east-2.amazonaws.com/prod/query"
S3_BUCKET = "traveler-app-uploads"

# Optional: AWS credentials for S3 uploads
# You can configure these via environment variables or Streamlit secrets
# For local testing: aws configure
# For Streamlit Cloud: Add to .streamlit/secrets.toml

# === S3 Upload Helper ===
def upload_to_s3(file, asset_id, timeframe, feed_type):
    """Upload file to S3 which triggers automatic ETL processing"""
    try:
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        # Generate S3 key: {asset_id}/{timeframe}/{feed_type}/{filename}
        filename = file.name
        s3_key = f"{asset_id}/{timeframe}/{feed_type}/{filename}"
        
        # Upload file
        file.seek(0)  # Reset file pointer
        s3_client.upload_fileobj(file, S3_BUCKET, s3_key)
        
        return True, s3_key
    except Exception as e:
        return False, str(e)

# === API Query Helper ===
def query_aws_api(asset_id, timeframes, report_date, scope_days, custom_ranges, measurements):
    """Query AWS Lambda via API Gateway"""
    try:
        payload = {
            "asset_id": asset_id,
            "timeframes": timeframes,
            "report_date": report_date,
            "scope_days": scope_days,
            "custom_ranges": custom_ranges,
            "measurements": measurements
        }
        
        response = requests.post(API_ENDPOINT, json=payload, timeout=30)
        
        if response.status_code == 200:
            return True, response.json()
        else:
            return False, f"API returned status {response.status_code}: {response.text}"
    except Exception as e:
        return False, str(e)

# === Unified Export Helper ===
def render_unified_export(traveler_reports, report_time, asset_id=""):
    if not traveler_reports:
        return

    st.markdown("---")
    st.markdown("### 📥 Unified Excel Download")

    asset_prefix = f"{asset_id.lower()}_" if asset_id else ""
    report_datetime_str = report_time.strftime("%d-%b-%y_%H-%M")

    def _coerce_arrival_datetime(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        if "Arrival_datetime" in df.columns:
            df["Arrival"] = pd.to_datetime(df["Arrival_datetime"], errors="coerce")
        elif "Arrival" in df.columns:
            df["Arrival"] = pd.to_datetime(df["Arrival"], errors="coerce", infer_datetime_format=True)
        return df

    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="xlsxwriter", datetime_format="mm/dd/yyyy hh:mm") as writer:
        workbook = writer.book
        header_fmt = workbook.add_format({
            "bold": True, "text_wrap": True, "valign": "top",
            "fg_color": "#D7E4BC", "border": 1
        })
        date_fmt = workbook.add_format({"num_format": "mm/dd/yyyy hh:mm"})

        for group_name, group_data in traveler_reports.items():
            if not isinstance(group_data, pd.DataFrame) or group_data.empty:
                continue

            sheet_name = group_name.replace(" ", "_").replace("-", "_")[:31]
            export_data = group_data.drop(columns=["Group"], errors="ignore").copy()
            export_data = _coerce_arrival_datetime(export_data)

            export_data.to_excel(writer, sheet_name=sheet_name, index=False)
            ws = writer.sheets[sheet_name]

            for c, name in enumerate(export_data.columns):
                ws.write(0, c, name, header_fmt)

            if "Arrival" in export_data.columns:
                a_idx = export_data.columns.get_loc("Arrival")
                ws.set_column(a_idx, a_idx, 18, date_fmt)

    excel_buffer.seek(0)
    total_entries = sum(len(df) for df in traveler_reports.values() if isinstance(df, pd.DataFrame))
    num_groups = sum(1 for v in traveler_reports.values() if isinstance(v, pd.DataFrame) and not v.empty)

    st.download_button(
        "📥 Download Excel Report",
        data=excel_buffer.getvalue(),
        file_name=f"{asset_prefix}traveler_report_{report_datetime_str}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        help=f"Excel file contains {num_groups} sheets with {total_entries} total entries"
    )

# === Main App ===
st.set_page_config(layout="wide")
st.header("🚀 Traveler App - AWS Accelerated v01 (legacy v32")

# Display mode selection
st.sidebar.markdown("### 🔧 Processing Mode")
use_aws = st.sidebar.checkbox("⚡ Use AWS Lambda (Fast)", value=True, 
    help="Process using AWS Lambda + DynamoDB for 10-35x faster performance")

if not use_aws:
    st.sidebar.warning("Local processing mode requires original app files")
    st.warning("⚠️ AWS mode is disabled. Original local processing not available in this version.")
    st.info("Enable 'Use AWS Lambda (Fast)' in the sidebar to use cloud processing.")
    st.stop()

# Asset ID Selector
st.markdown("### Asset Selection")
asset_id = st.selectbox(
    "Select Asset ID",
    options=["NQ", "ES", "YM", "RTY"],
    index=0,
    help="Select the asset/instrument for this analysis"
)
st.info(f"Selected Asset: **{asset_id}**")

# File uploads section
st.markdown("---")
st.markdown("### 📤 Upload Data Files to AWS")

upload_mode = st.radio("Upload Mode", ["Upload New Files", "Use Existing Data"])

if upload_mode == "Upload New Files":
    st.markdown("Upload your CSV files - they'll be automatically processed by AWS Lambda")
    
    # Row 1: Small feeds
    col1, col2, col3 = st.columns(3)
    with col1:
        small_3m_file = st.file_uploader("3m small", type="csv", key="small_3m")
    with col2:
        small_5m_file = st.file_uploader("5m small", type="csv", key="small_5m")
    with col3:
        small_15m_file = st.file_uploader("15m small", type="csv", key="small_15m")
    
    # Row 2: Big feeds
    col4, col5, col6 = st.columns(3)
    with col4:
        big_3m_file = st.file_uploader("3m big", type="csv", key="big_3m")
    with col5:
        big_5m_file = st.file_uploader("5m big", type="csv", key="big_5m")
    with col6:
        big_15m_file = st.file_uploader("15m big", type="csv", key="big_15m")
    
    # Upload button
    if st.button("📤 Upload Files to AWS S3"):
        uploaded_count = 0
        upload_errors = []
        
        with st.spinner("Uploading files to AWS S3..."):
            files_to_upload = [
                (small_3m_file, "3m", "small"),
                (small_5m_file, "5m", "small"),
                (small_15m_file, "15m", "small"),
                (big_3m_file, "3m", "big"),
                (big_5m_file, "5m", "big"),
                (big_15m_file, "15m", "big"),
            ]
            
            for file, timeframe, feed_type in files_to_upload:
                if file is not None:
                    success, result = upload_to_s3(file, asset_id, timeframe, feed_type)
                    if success:
                        uploaded_count += 1
                        st.success(f"✅ Uploaded: {result}")
                    else:
                        upload_errors.append(f"❌ Failed to upload {file.name}: {result}")
        
        if uploaded_count > 0:
            st.success(f"✅ Successfully uploaded {uploaded_count} files!")
            st.info("⏳ AWS Lambda is now processing your files in the background. Wait 10-30 seconds, then run your query.")
        
        if upload_errors:
            for error in upload_errors:
                st.error(error)

# Measurement file
st.markdown("### 📊 Measurement File")
measurement_file = st.file_uploader("Upload measurement file", type=["xlsx", "xls"])

# Report Time
st.markdown("---")
st.markdown("### ⏰ Report Settings")
report_mode = st.radio("Select Report Time & Date", ["Most Current", "Choose a time"])
if report_mode == "Choose a time":
    selected_date = st.date_input("Select Report Date", value=dt.date.today())
    selected_time = st.time_input("Select Report Time", value=dt.time(18, 0))
    report_time = dt.datetime.combine(selected_date, selected_time)
else:
    report_time = dt.datetime.now()

report_date_str = report_time.strftime('%Y-%m-%d')

# Custom Ranges Configuration
st.markdown("---")
st.markdown("### 🎯 Custom Ranges Configuration")

col1, col2 = st.columns(2)

with col1:
    st.markdown("**High Ranges**")
    use_high1 = st.checkbox("Enable High Range 1", value=False)
    high1 = st.number_input("High Range 1 Center", value=0.0, step=0.1,
        help="Range will be [value-24, value]") if use_high1 else 0.0
    
    use_high2 = st.checkbox("Enable High Range 2", value=False)
    high2 = st.number_input("High Range 2 Center", value=0.0, step=0.1,
        help="Range will be [value-24, value]") if use_high2 else 0.0

with col2:
    st.markdown("**Low Ranges**")
    use_low1 = st.checkbox("Enable Low Range 1", value=False)
    low1 = st.number_input("Low Range 1 Center", value=0.0, step=0.1,
        help="Range will be [value, value+24]") if use_low1 else 0.0
    
    use_low2 = st.checkbox("Enable Low Range 2", value=False)
    low2 = st.number_input("Low Range 2 Center", value=0.0, step=0.1,
        help="Range will be [value, value+24]") if use_low2 else 0.0

# Scope and timeframes
scope_days = st.number_input("Scope (days)", value=20, min_value=1, max_value=365,
    help="Look back period for data analysis")

timeframes = st.multiselect(
    "Timeframes to Include",
    options=["3m", "5m", "15m"],
    default=["3m", "5m", "15m"],
    help="Select which timeframes to include in the analysis"
)

# Query button
st.markdown("---")
if st.button("🚀 Run AWS Query", type="primary"):
    if not measurement_file:
        st.error("Please upload a measurement file")
        st.stop()
    
    # Load measurements
    try:
        measurements_df = pd.read_excel(measurement_file, sheet_name=0)
        
        # Convert to list of dicts for API
        measurements = []
        for _, row in measurements_df.iterrows():
            m_val_col = next((c for c in ['M value', 'M Value', 'M_Value', 'm_value'] if c in row.index), None)
            m_name_col = next((c for c in ['M Name', 'M name', 'M_name', 'm_name'] if c in row.index), None)
            
            if m_val_col:
                measurements.append({
                    "M_value": row[m_val_col],
                    "M_name": row[m_name_col] if m_name_col else f"M{row[m_val_col]}"
                })
        
        st.info(f"Loaded {len(measurements)} measurements")
    except Exception as e:
        st.error(f"Error loading measurements: {e}")
        st.stop()
    
    # Build custom ranges
    custom_ranges = {}
    if use_high1 and high1 > 0:
        custom_ranges['High 1'] = {'enabled': True, 'value': high1}
    if use_high2 and high2 > 0:
        custom_ranges['High 2'] = {'enabled': True, 'value': high2}
    if use_low1 and low1 > 0:
        custom_ranges['Low 1'] = {'enabled': True, 'value': low1}
    if use_low2 and low2 > 0:
        custom_ranges['Low 2'] = {'enabled': True, 'value': low2}
    
    if not custom_ranges:
        st.error("Please enable at least one custom range")
        st.stop()
    
    # Query AWS API
    with st.spinner("🔄 Querying AWS Lambda..."):
        import time
        start_time = time.time()
        
        success, result = query_aws_api(
            asset_id=asset_id,
            timeframes=timeframes,
            report_date=report_date_str,
            scope_days=scope_days,
            custom_ranges=custom_ranges,
            measurements=measurements
        )
        
        processing_time = time.time() - start_time
    
    if success:
        data = result.get('data', [])
        count = result.get('count', 0)
        hlc_processed = result.get('hlc_records_processed', 0)
        
        st.success(f"✅ Query complete in {processing_time:.2f} seconds!")
        
        # Display metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Processing Time", f"{processing_time:.1f}s")
        with col2:
            st.metric("Traveler Entries", count)
        with col3:
            st.metric("HLC Records Processed", hlc_processed)
        
        if count > 0:
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Display data
            st.markdown("---")
            st.markdown("### 📊 Results")
            st.dataframe(df, use_container_width=True)
            
            # Prepare for export
            traveler_reports = {"Grp_All": df}
            render_unified_export(traveler_reports, report_time, asset_id)
            
            # Performance comparison
            st.markdown("---")
            st.markdown("### ⚡ Performance Comparison")
            local_time = 178  # Previous local processing time
            speedup = local_time / processing_time if processing_time > 0 else 0
            
            st.info(f"**Local Processing:** ~{local_time}s  |  **AWS Lambda:** {processing_time:.1f}s  |  **Speedup:** {speedup:.1f}x faster!")
        else:
            st.warning("No traveler entries found matching the criteria")
    else:
        st.error(f"Query failed: {result}")

# Sidebar info
st.sidebar.markdown("---")
st.sidebar.markdown("### ℹ️ AWS Integration Info")
st.sidebar.info(f"""
**API Endpoint:** 
`{API_ENDPOINT[:40]}...`

**S3 Bucket:** 
`{S3_BUCKET}`

**Region:** 
`{AWS_REGION}`

**Status:** 
{'✅ Connected' if API_ENDPOINT != 'https://5c9t51huga.execute-api.us-east-2.amazonaws.com/prod/query' else '⚠️ Configure API endpoint'}
""")

st.sidebar.markdown("---")
st.sidebar.markdown("### 🚀 Benefits")
st.sidebar.success("""
✅ 10-35x faster processing
✅ Auto-scaling infrastructure
✅ Pay only for what you use
✅ ~$0-5/month cost
✅ Serverless - no maintenance
""")
