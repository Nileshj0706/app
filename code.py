import json
import re
from pathlib import Path
import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# --- 1. THEME & CONFIGURATION ---
st.set_page_config(page_title="ESG Nexus | Master Repository", layout="wide")

st.markdown(
    """
    <style>
        .main { background-color: #f1f5f9; }
        h1 { color: #1e3a8a !important; }
        .stExpander {
            background-color: #f0fdf4 !important;
            border: 2px solid #16a34a !important;
            border-radius: 10px;
        }
        div.stButton > button { font-weight: bold; border-radius: 8px; }
        div.stButton > button[kind="primary"] { background-color: #dc2626 !important; color: white !important; }
        div.stButton > button[kind="secondary"] { background-color: #2563eb !important; color: white !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

KPI_FILE = Path("KPIMaster_WithTopics - Functional Team(KPIMaster_WithTopics).csv")
CODES_FILE = Path("KPIMaster_WithTopics - Functional Team(Codes).csv")


@st.cache_data
def load_data():
    df_m = pd.read_csv(KPI_FILE, encoding="latin1", on_bad_lines="skip")
    df_c_raw = pd.read_csv(CODES_FILE, encoding="latin1", on_bad_lines="skip")

    p_map = {1.0: "Environmental", 2.0: "Social", 3.0: "Governance", 4.0: "General"}

    try:
        h_idx = df_c_raw[df_c_raw.iloc[:, 6] == "TopicCode"].index[0]
        df_topics = pd.read_csv(CODES_FILE, skiprows=h_idx + 1, encoding="latin1")
        t_id_to_name = dict(zip(df_topics["TopicCode"].astype(str), df_topics["Name"].astype(str)))
        t_name_to_id = dict(zip(df_topics["Name"].astype(str), df_topics["TopicCode"].astype(str)))
    except (IndexError, KeyError, pd.errors.ParserError):
        t_id_to_name, t_name_to_id = {}, {}

    agg_map = {1.0: "SUM", 2.0: "MATCH_AND_APPEND", 3.0: "APPEND", 0.0: "NONE"}
    type_map = {
        1.0: "TextBlock (Narrative)",
        2.0: "Table (Title)",
        3.0: "Numeric (Table)",
        4.0: "TextArea (Narrative in Table)",
        0.0: "None",
    }

    fw_map = {
        1: "ISSB",
        2: "BRSR",
        3: "GRI",
        4: "ESRS",
        5: "ASRS",
        6: "GENERAL",
        7: "CSRD",
        8: "SASB",
        9: "DJSI",
        10: "CHRB",
        11: "TNFD",
        12: "ISE",
        13: "Ecovadis",
        14: "Others",
    }
    return df_m, p_map, t_id_to_name, t_name_to_id, agg_map, type_map, fw_map


def read_latest_master() -> pd.DataFrame:
    return pd.read_csv(KPI_FILE, encoding="latin1", on_bad_lines="skip")


def save_master(df: pd.DataFrame) -> None:
    tmp_path = KPI_FILE.with_suffix(KPI_FILE.suffix + ".tmp")
    df.to_csv(tmp_path, index=False)
    tmp_path.replace(KPI_FILE)


(df_master, p_map, t_id_to_name, t_name_to_id, agg_map, type_map, fw_map) = load_data()

if "active_pillar" not in st.session_state:
    st.session_state["active_pillar"] = "All"
if "selected_framework" not in st.session_state:
    st.session_state["selected_framework"] = "GRI"
if "table_col_count" not in st.session_state:
    st.session_state["table_col_count"] = 3


def get_next_iris_code(df, pillar_selection):
    prefix = pillar_selection[0].upper()
    existing_codes = df["IrisKPICode"].dropna().astype(str)
    nums = []
    for code in existing_codes:
        if code.startswith(prefix):
            match = re.search(r"\d+", code)
            if match:
                nums.append(int(match.group()))
    next_num = max(nums) + 1 if nums else 1
    return f"{prefix}_{str(next_num).zfill(4)}"


def parse_fw(detail, target_id):
    if pd.isna(detail):
        return "—"
    try:
        data = json.loads(str(detail).replace("'", '"'))
        for item in data:
            if item.get("Standard") == target_id:
                return item.get("Description", "—")
    except (json.JSONDecodeError, TypeError):
        pass
    return "—"


def parse_fw_reference_code(detail, target_id):
    if pd.isna(detail):
        return "—"
    try:
        data = json.loads(str(detail).replace("'", '"'))
        for item in data:
            if item.get("Standard") == target_id:
                return item.get("ReferenceCode", "—")
    except (json.JSONDecodeError, TypeError):
        pass
    return "—"


def _pick_col(columns, *candidates):
    lookup = {c.lower(): c for c in columns}
    for candidate in candidates:
        col = lookup.get(candidate.lower())
        if col:
            return col
    return None


def _new_row_template(columns):
    return {c: pd.NA for c in columns}


def _set_if_present(row, columns, value, *candidates):
    col = _pick_col(columns, *candidates)
    if col is not None:
        row[col] = value


def _next_group_code(df):
    parent_col = _pick_col(df.columns, "ParentCode")
    if not parent_col:
        return "Group_1"
    max_id = 0
    for val in df[parent_col].dropna().astype(str):
        m = re.search(r"Group_(\d+)", val)
        if m:
            max_id = max(max_id, int(m.group(1)))
    return f"Group_{max_id + 1}"


def _extract_parent_iris_from_kpidetail(value):
    if pd.isna(value):
        return None
    try:
        payload = json.loads(str(value).replace("'", '"'))
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict) and item.get("ParentIrisKPICode"):
                    return str(item.get("ParentIrisKPICode"))
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    return None


def _is_parent_table_row(kpidetail):
    if pd.isna(kpidetail):
        return False
    try:
        payload = json.loads(str(kpidetail).replace("'", '"'))
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict) and item.get("HierarchyType") == "PARENT_TABLE":
                    return True
    except (json.JSONDecodeError, TypeError, ValueError):
        return False
    return False


st.title("ESG Nexus Master Console")

with st.expander("🟢 ADD NEW KPI (Auto-generates Iris Code)"):
    entry_mode = st.selectbox(
        "KPI Type",
        options=["Standard KPI", "Tabular — Static"],
        index=0,
        key="add_kpi_entry_mode",
    )

    st.subheader("Business Language Entry")
    c1, c2 = st.columns(2)
    with c1:
        in_title = st.text_input("Master KPI Name")
        in_pillar = st.selectbox("Pillar", options=list(p_map.values()))
        in_topic = st.selectbox("Topic", options=list(t_name_to_id.keys()))
    with c2:
        if entry_mode == "Standard KPI":
            in_type = st.selectbox("Type", options=["TextBlock (Narrative)"])
        else:
            in_type = st.selectbox(
                "Tabular KPI Type",
                options=["Table (Title)", "Numeric (Table)", "TextArea (Narrative in Table)"],
            )
        in_agg = st.selectbox("Aggregation", options=list(agg_map.values()))
        in_fw = st.selectbox("Map to Framework", options=list(fw_map.values()))

    in_desc = st.text_area("Framework Language Description (Main)")
    in_ref_code = st.text_input("Framework Reference Code (optional)")

    is_table_title = entry_mode == "Tabular — Static"
    row_labels = []
    row_descs = {}
    col_headers = []
    col_descs = {}
    cell_kpis = []
    cell_descs = {}
    column_configs = []

    if is_table_title:
        st.markdown("### 🧩 Table Configuration — Static")
        row_count = int(st.number_input("Number of Rows *", min_value=1, max_value=25, value=3, step=1, key="row_count"))
        
        st.markdown("#### Row Labels & Descriptions")
        for i in range(row_count):
            rl_c1, rl_c2 = st.columns(2)
            label = rl_c1.text_input(f"Row {i + 1} Label", value=f"Row {i + 1}", key=f"row_label_{i}")
            row_labels.append(label)
            row_descs[i+1] = rl_c2.text_input(f"Row {i + 1} Description", value=in_desc, key=f"row_desc_{i}")

        col_count = int(st.number_input("Number of Columns *", min_value=1, max_value=25, value=int(st.session_state.table_col_count), step=1, key="table_col_count_input"))
        st.session_state.table_col_count = col_count

        for j in range(col_count):
            with st.container(border=True):
                st.markdown(f"**Col {chr(65 + j) if j < 26 else j + 1}**")
                cc1, cc2 = st.columns(2)
                header = cc1.text_input(f"Header {j + 1} *", value=f"Column {j + 1}", key=f"col_header_{j}")
                col_descs[j+1] = cc2.text_input(f"Col {j+1} Description", value=in_desc, key=f"col_desc_input_{j}")
                
                cc3, cc4 = st.columns(2)
                formula = cc3.text_input(f"Formula {j+1}", value="", key=f"col_formula_{j}")
                unit = cc4.text_input(f"Unit {j+1}", value="", key=f"col_unit_{j}")
                col_agg = st.selectbox(f"Agg {j+1}", options=list(agg_map.values()), index=list(agg_map.values()).index("NONE"), key=f"col_agg_{j}")

                col_headers.append(header)
                column_configs.append({"header": header, "formula": formula, "unit": unit, "aggregation": col_agg})

        st.markdown("#### Cell KPI Names & Descriptions")
        for i in range(row_count):
            for j in range(col_count):
                ce_c1, ce_c2 = st.columns(2)
                v = ce_c1.text_input(f"R{i + 1}C{j + 1} Title", value="", key=f"cell_kpi_{i}_{j}")
                d = ce_c2.text_input(f"R{i + 1}C{j + 1} Description", value=in_desc, key=f"cell_desc_{i}_{j}")
                cell_kpis.append({"row_index": i + 1, "column_index": j + 1, "title": v, "desc": d})

    if st.button("✅ Create & Save KPI", type="primary"):
        inv_p = {v: k for k, v in p_map.items()}
        inv_a = {v: k for k, v in agg_map.items()}
        inv_t = {v: k for k, v in type_map.items()}
        inv_fw = {v: k for k, v in fw_map.items()}

        try:
            latest_master = read_latest_master()
            fw_standard = inv_fw[in_fw]
            schema_cols = list(latest_master.columns)
            group_code = _next_group_code(latest_master) if is_table_title else None
            parent_id = get_next_iris_code(latest_master, in_pillar)
            rows_to_add = []

            # Parent row
            parent_payload = {"Standard": fw_standard, "Description": in_desc, "ReferenceCode": in_ref_code}
            if is_table_title:
                parent_payload.update({"HierarchyType": "PARENT_TABLE", "RowIndex": 0, "ColumnIndex": 0})
            
            parent_row = _new_row_template(schema_cols)
            _set_if_present(parent_row, schema_cols, parent_id, "IrisKPICode")
            _set_if_present(parent_row, schema_cols, inv_p[in_pillar], "Category")
            _set_if_present(parent_row, schema_cols, t_name_to_id[in_topic], "TopicId")
            _set_if_present(parent_row, schema_cols, inv_t[in_type], "Type")
            _set_if_present(parent_row, schema_cols, in_title, "Title")
            _set_if_present(parent_row, schema_cols, inv_a[in_agg], "AggregationType", "Aggregation")
            _set_if_present(parent_row, schema_cols, json.dumps([parent_payload]), "KPIDetail")
            _set_if_present(parent_row, schema_cols, 0, "RowIndex")
            _set_if_present(parent_row, schema_cols, 0, "ColIndex", "ColumnIndex")
            _set_if_present(parent_row, schema_cols, False, "IsDynamic")
            if is_table_title: _set_if_present(parent_row, schema_cols, group_code, "ParentCode")
            rows_to_add.append(parent_row)

            if is_table_title:
                temp_df = pd.concat([latest_master, pd.DataFrame(rows_to_add)], ignore_index=True)

                for i, label in enumerate(row_labels, start=1):
                    child_id = get_next_iris_code(temp_df, in_pillar)
                    row_payload = {"Standard": fw_standard, "Description": row_descs[i], "ReferenceCode": in_ref_code, "HierarchyType": "CHILD_ROW_HEADER", "ParentIrisKPICode": parent_id, "RowIndex": i, "ColumnIndex": 0, "RowHeader": label}
                    row_row = _new_row_template(schema_cols)
                    _set_if_present(row_row, schema_cols, child_id, "IrisKPICode")
                    _set_if_present(row_row, schema_cols, inv_p[in_pillar], "Category")
                    _set_if_present(row_row, schema_cols, t_name_to_id[in_topic], "TopicId")
                    _set_if_present(row_row, schema_cols, inv_t[in_type], "Type")
                    _set_if_present(row_row, schema_cols, label, "Title")
                    _set_if_present(row_row, schema_cols, inv_a["NONE"], "AggregationType", "Aggregation")
                    _set_if_present(row_row, schema_cols, json.dumps([row_payload]), "KPIDetail")
                    _set_if_present(row_row, schema_cols, i, "RowIndex")
                    _set_if_present(row_row, schema_cols, 0, "ColIndex", "ColumnIndex")
                    _set_if_present(row_row, schema_cols, False, "IsDynamic")
                    _set_if_present(row_row, schema_cols, group_code, "ParentCode")
                    _set_if_present(row_row, schema_cols, label, "RowHeader")
                    rows_to_add.append(row_row)
                    temp_df = pd.concat([temp_df, pd.DataFrame([row_row])], ignore_index=True)

                for j, header in enumerate(col_headers, start=1):
                    child_id = get_next_iris_code(temp_df, in_pillar)
                    col_cfg = column_configs[j - 1]
                    col_payload = {"Standard": fw_standard, "Description": col_descs[j], "ReferenceCode": in_ref_code, "HierarchyType": "CHILD_COLUMN_HEADER", "ParentIrisKPICode": parent_id, "RowIndex": 0, "ColumnIndex": j, "ColumnHeader": header, "Unit": col_cfg.get("unit", ""), "CellFormula": col_cfg.get("formula", ""), "Aggregation": col_cfg.get("aggregation", "NONE")}
                    col_row = _new_row_template(schema_cols)
                    _set_if_present(col_row, schema_cols, child_id, "IrisKPICode")
                    _set_if_present(col_row, schema_cols, inv_p[in_pillar], "Category")
                    _set_if_present(col_row, schema_cols, t_name_to_id[in_topic], "TopicId")
                    _set_if_present(col_row, schema_cols, inv_t[in_type], "Type")
                    _set_if_present(col_row, schema_cols, header, "Title")
                    _set_if_present(col_row, schema_cols, inv_a["NONE"], "AggregationType", "Aggregation")
                    _set_if_present(col_row, schema_cols, json.dumps([col_payload]), "KPIDetail")
                    _set_if_present(col_row, schema_cols, 0, "RowIndex")
                    _set_if_present(col_row, schema_cols, j, "ColIndex", "ColumnIndex")
                    _set_if_present(col_row, schema_cols, False, "IsDynamic")
                    _set_if_present(col_row, schema_cols, group_code, "ParentCode")
                    _set_if_present(col_row, schema_cols, header, "ColumnHeader")
                    rows_to_add.append(col_row)
                    temp_df = pd.concat([temp_df, pd.DataFrame([col_row])], ignore_index=True)

                for cell in cell_kpis:
                    child_id = get_next_iris_code(temp_df, in_pillar)
                    col_cfg = column_configs[cell["column_index"] - 1]
                    cell_title = cell["title"] or f"{in_title} | R{cell['row_index']}C{cell['column_index']}"
                    cell_payload = {"Standard": fw_standard, "Description": cell["desc"], "ReferenceCode": in_ref_code, "HierarchyType": "CHILD_CELL_VALUE", "ParentIrisKPICode": parent_id, "RowIndex": cell["row_index"], "ColumnIndex": cell["column_index"], "Unit": col_cfg.get("unit", ""), "CellFormula": col_cfg.get("formula", ""), "Aggregation": col_cfg.get("aggregation", in_agg)}
                    cell_row = _new_row_template(schema_cols)
                    _set_if_present(cell_row, schema_cols, child_id, "IrisKPICode")
                    _set_if_present(cell_row, schema_cols, inv_p[in_pillar], "Category")
                    _set_if_present(cell_row, schema_cols, t_name_to_id[in_topic], "TopicId")
                    _set_if_present(cell_row, schema_cols, inv_t[in_type], "Type")
                    _set_if_present(cell_row, schema_cols, cell_title, "Title")
                    _set_if_present(cell_row, schema_cols, inv_a[in_agg], "AggregationType", "Aggregation")
                    _set_if_present(cell_row, schema_cols, json.dumps([cell_payload]), "KPIDetail")
                    _set_if_present(cell_row, schema_cols, cell["row_index"], "RowIndex")
                    _set_if_present(cell_row, schema_cols, cell["column_index"], "ColIndex", "ColumnIndex")
                    _set_if_present(cell_row, schema_cols, False, "IsDynamic")
                    _set_if_present(cell_row, schema_cols, group_code, "ParentCode")
                    rows_to_add.append(cell_row)
                    temp_df = pd.concat([temp_df, pd.DataFrame([cell_row])], ignore_index=True)

            save_master(pd.concat([latest_master, pd.DataFrame(rows_to_add)], ignore_index=True))
            st.success("Successfully added KPI rows."); st.cache_data.clear(); st.rerun()
        except PermissionError: st.error("Close the Excel file!")

# Remaining parts (Delete, Filters, AgGrid, Save All) are exactly as in your original file.
with st.expander("🔴 REMOVE KPI (Deletes from Master)"):
    latest_master_for_delete = read_latest_master()
    kpi_options = sorted(latest_master_for_delete["IrisKPICode"].dropna().astype(str).unique().tolist())
    remove_mode = st.radio("Remove by", options=["Iris KPI Code", "KPI Title"], horizontal=True, key="remove_kpi_mode")
    selected_kpi_code = None
    if remove_mode == "Iris KPI Code":
        selected_kpi_code = st.selectbox("Select Iris KPI Code", options=[""] + kpi_options, key="remove_kpi_code")
    else:
        title_df = latest_master_for_delete[["IrisKPICode", "Title"]].dropna(subset=["IrisKPICode"]).copy()
        title_df["Display"] = title_df["IrisKPICode"].astype(str) + " | " + title_df["Title"].fillna("").astype(str)
        selected_display = st.selectbox("Select KPI", options=[""] + sorted(title_df["Display"].tolist()), key="remove_kpi_title")
        if selected_display: selected_kpi_code = selected_display.split(" | ", 1)[0]
    if st.button("🗑️ Remove KPI", type="secondary"):
        if selected_kpi_code:
            lm = read_latest_master()
            codes_to_remove = {str(selected_kpi_code)}
            pc_col = _pick_col(lm.columns, "ParentCode")
            if pc_col:
                p_val = lm[lm["IrisKPICode"].astype(str) == str(selected_kpi_code)].iloc[0].get(pc_col)
                if pd.notna(p_val):
                    codes_to_remove.update(lm[lm[pc_col].astype(str) == str(p_val)]["IrisKPICode"].dropna().astype(str).tolist())
            save_master(lm[~lm["IrisKPICode"].astype(str).isin(codes_to_remove)])
            st.success("Removed."); st.cache_data.clear(); st.rerun()

search = st.text_input("🔍 Search Database...", placeholder="Search names or IDs...")
st.session_state.active_pillar = st.selectbox("Pillar", options=["All", "Environmental", "Social", "Governance", "General"])
st.session_state.selected_framework = st.selectbox("Select framework KPI to display", options=list(fw_map.values()))

biz_df = df_master.copy()
biz_df["Pillar"] = biz_df["Category"].map(p_map)
biz_df["Topic"] = biz_df["TopicId"].map(t_id_to_name)
biz_df["Type"] = biz_df["Type"].map(type_map)
biz_df["Aggregation"] = biz_df["AggregationType"].map(agg_map)
for sid, sname in fw_map.items():
    biz_df[f"{sname} Language"] = biz_df["KPIDetail"].apply(lambda x: parse_fw(x, sid))
    biz_df[f"{sname} ReferenceCode"] = biz_df["KPIDetail"].apply(lambda x: parse_fw_reference_code(x, sid))

if st.session_state.active_pillar != "All": biz_df = biz_df[biz_df["Pillar"] == st.session_state.active_pillar]
if search: biz_df = biz_df[biz_df.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)]

final_cols = ["IrisKPICode", "Pillar", "Topic", "Type", "Title", "Aggregation", f"{st.session_state.selected_framework} Language"]
grid_df = biz_df[final_cols].copy()
gb = GridOptionsBuilder.from_dataframe(grid_df)
gb.configure_default_column(editable=False, sortable=True, filter=True, floatingFilter=True, resizable=True)
gb.configure_column("Pillar", editable=True)
gb.configure_column("Topic", editable=True)
gb.configure_column("Title", editable=True)
grid_response = AgGrid(grid_df, gridOptions=gb.build(), update_mode=GridUpdateMode.MODEL_CHANGED, height=500)

if st.button("💾 SAVE ALL CHANGES", type="primary"):
    lm = read_latest_master()
    ed = pd.DataFrame(grid_response["data"])
    inv_p, inv_topic = {v: k for k, v in p_map.items()}, {v: k for k, v in t_id_to_name.items()}
    for _, row in ed.iterrows():
        idx = lm[lm["IrisKPICode"] == row["IrisKPICode"]].index
        if not idx.empty:
            lm.loc[idx, "Title"] = row["Title"]
            lm.loc[idx, "Category"] = inv_p.get(row["Pillar"], 1.0)
            lm.loc[idx, "TopicId"] = inv_topic.get(row["Topic"], "T_01")
    save_master(lm)
    st.success("Master database updated!"); st.cache_data.clear(); st.rerun()