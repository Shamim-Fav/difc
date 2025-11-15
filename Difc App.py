import streamlit as st
import pandas as pd
import requests
import json
from time import sleep
from io import BytesIO

# -----------------------------
# CONFIG
# -----------------------------
URL = "https://www.difc.com/api/handleRequest"
HEADERS = {
    "Content-Type": "text/plain;charset=UTF-8",
    "Accept": "*/*",
    "User-Agent": "Mozilla/5.0",
    "Origin": "https://www.difc.com",
    "Referer": "https://www.difc.com/business/public-register"
}
PAGE_SIZE = 200
STEP1_FILE = "Step1_DIFC_Companies.xlsx"
STEP2_FILE = "Step2_DIFC_Details.xlsx"

# Example company types (you can extend this list)
COMPANY_TYPES = ["All", "Financial - related", "Wealth & Asset Management", "Non - financial"]

# -----------------------------
# FUNCTIONS
# -----------------------------
def fetch_companies(offset=0):
    payload = {
        "name": "",
        "licenseType": "",
        "licenseNo": "",
        "status": "",
        "offset": offset,
        "slug": "/CRM/public-register",
        "method": "POST"
    }
    try:
        response = requests.post(URL, headers=HEADERS, data=json.dumps(payload))
        response.raise_for_status()
        return response.json()
    except:
        return None

def get_activities(company):
    activities = []
    license_records = company.get("License_Activities__r", {}).get("records", [])
    for rec in license_records:  
        name = rec.get("Activity__r", {}).get("Name", "")
        if name:
            activities.append(name)
    return "; ".join(activities)

def flatten_company(company):
    flat = company.copy()
    flat["License_Activities"] = get_activities(company)
    flat["DIFC_URL"] = "https://www.difc.com/business/public-register/public-register-details?companyId=" + company.get("Id", "")
    return flat

def step1_fetch(num_raw, selected_type, progress_bar, status_text):
    offset = 0
    all_raw = []

    status_text.text("🔎 Step 1 — Fetching raw data ...")
    while len(all_raw) < num_raw:
        status_text.text(f"Fetching records starting from offset {offset} ...")
        data = fetch_companies(offset)
        if not data:
            st.warning(f"No data returned for offset {offset}.")
            break
        companies = data.get("Data", {}).get("companyList", [])
        if not companies:
            st.warning("No companies returned. Stopping fetch.")
            break
        all_raw.extend(companies)
        progress_bar.progress(min(len(all_raw)/num_raw, 1.0))
        if len(all_raw) >= num_raw:
            break
        offset += PAGE_SIZE
        sleep(0.8)
    all_raw = all_raw[:num_raw]

    if selected_type == "All":
        filtered = [flatten_company(c) for c in all_raw]
    else:
        filtered = [flatten_company(c) for c in all_raw if c.get("Company_Type__c") == selected_type]

    # Save Step 1 to BytesIO
    step1_buffer = BytesIO()
    with pd.ExcelWriter(step1_buffer, engine="openpyxl") as writer:
        pd.DataFrame(all_raw).to_excel(writer, sheet_name="RawData", index=False)
        pd.DataFrame(filtered).to_excel(writer, sheet_name="FilteredData", index=False)
    step1_buffer.seek(0)
    
    return filtered, step1_buffer

def fetch_company_details(record_id):
    payload = {
        "slug": f"/CRM/public-register?recordId={record_id}",
        "method": "GET"
    }
    try:
        r = requests.post(URL, headers=HEADERS, data=json.dumps(payload))
        r.raise_for_status()
        return r.json()
    except:
        return None

def extract_filtered(item, record_id):
    name = item.get("EntityName",[{}])[0].get("Name","") if item.get("EntityName") else item.get("TradingName",[{}])[0].get("TradeName","") if item.get("TradingName") else ""
    reg_no = item.get("RegisteredNumber","")
    entity_type = item.get("TypeOfEntity","")
    status = item.get("EntityStatus","")
    website = item.get("MarketingFields",{}).get("Website","")
    coords = item.get("MarketingFields",{}).get("BuildingCoordinates",[])
    location = json.dumps(coords) if coords else "DIFC, Dubai"
    directors = item.get("Director",[])
    contacts = [d.get("DirectorName","") for d in directors[:4]]
    contacts += [""]*(4-len(contacts))
    url = f"https://www.difc.com/business/public-register/public-register-details?companyId={record_id}"
    return {
        "ID": record_id,
        "Name": name,
        "RegisteredNumber": reg_no,
        "Type": entity_type,
        "Status": status,
        "Location": location,
        "Website": website,
        "Contact 1": contacts[0],
        "Contact 2": contacts[1],
        "Contact 3": contacts[2],
        "Contact 4": contacts[3],
        "URL": url
    }

def flatten(value):
    if isinstance(value,(dict,list)):
        return json.dumps(value,ensure_ascii=False)
    return value

def extract_raw(json_data, record_id):
    try:
        item = json_data["Data"]["DIFCData"]["PublicRegistry"][0]
    except:
        return {"ID": record_id,"Error":"No PublicRegistry data"}
    raw = {"ID": record_id}
    for k,v in item.items():
        raw[k] = flatten(v)
    return raw

def step2_fetch(filtered_companies, progress_bar, status_text):
    raw_rows = []
    filtered_rows = []

    status_text.text("🔎 Step 2 — Fetching detailed info ...")
    for idx, comp in enumerate(filtered_companies):
        record_id = comp.get("Id")
        if not record_id:
            continue
        status_text.text(f"Fetching details: {record_id}")
        data = fetch_company_details(record_id)
        if not data:
            continue
        try:
            item = data["Data"]["DIFCData"]["PublicRegistry"][0]
        except:
            continue
        raw_rows.append(extract_raw(data, record_id))
        filtered_rows.append(extract_filtered(item, record_id))
        progress_bar.progress((idx+1)/len(filtered_companies))
        sleep(0.8)

    step2_buffer = BytesIO()
    with pd.ExcelWriter(step2_buffer, engine="openpyxl") as writer:
        pd.DataFrame(raw_rows).to_excel(writer, sheet_name="RawData", index=False)
        pd.DataFrame(filtered_rows).to_excel(writer, sheet_name="FilteredData", index=False)
    step2_buffer.seek(0)
    
    return step2_buffer

# -----------------------------
# STREAMLIT GUI
# -----------------------------
st.title("DIFC Company Scraper")

num_raw = st.number_input("Enter number of raw records to fetch", min_value=10, max_value=5000, value=200, step=10)
selected_type = st.selectbox("Select Company Type", COMPANY_TYPES)

if st.button("Start Scraping"):
    progress_bar = st.progress(0)
    status_text = st.empty()

    # Step 1
    filtered_companies, step1_buffer = step1_fetch(num_raw, selected_type, progress_bar, status_text)
    st.success(f"Step 1 Done! {len(filtered_companies)} companies filtered by '{selected_type}'")

    st.download_button(
        label="Download Step 1 Excel",
        data=step1_buffer,
        file_name=STEP1_FILE,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    if filtered_companies:
        progress_bar.progress(0)
        step2_buffer = step2_fetch(filtered_companies, progress_bar, status_text)
        st.success("Step 2 Done! Detailed info saved.")

        st.download_button(
            label="Download Step 2 Excel",
            data=step2_buffer,
            file_name=STEP2_FILE,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
