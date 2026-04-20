import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# ================= CONFIG =================
TOKEN = "Token 93f8c79d2323f40b0f475303676aca198a5f73ea"
ASSET_UID = "atEkwi788MscNQvXdUjebc"
BASE_URL = "https://eu.kobotoolbox.org"

# ⚠️ FIX PATH (pakai r agar tidak error)
OUTPUT_FILE = r"kobo_data.xlsx"

headers = {
    "Authorization": TOKEN
}

# ================= GET ALL DATA =================
def get_all_data():
    all_data = []
    offset = 0
    limit = 100

    while True:
        url = f"{BASE_URL}/api/v2/assets/{ASSET_UID}/data/"
        params = {
            "limit": limit,
            "start": offset
        }

        response = requests.get(url, headers=headers, params=params)
        data = response.json()

        results = data.get("results", [])
        if not results:
            break

        all_data.extend(results)
        offset += limit

        print(f"Loaded {len(all_data)} records...")

    return all_data


# ================= FLATTEN DATA =================
def flatten_json(y, prefix=""):
    out = {}

    for k, v in y.items():
        key = f"{prefix}{k}"

        if isinstance(v, dict):
            out.update(flatten_json(v, key + "_"))

        elif isinstance(v, list):
            out[key] = str(v)

        else:
            out[key] = v

    return out


# ================= EXTRACT REPEAT =================
def extract_repeat(data, repeat_name):
    rows = []

    for d in data:
        parent_id = d.get("_id")

        if repeat_name in d and isinstance(d[repeat_name], list):
            for item in d[repeat_name]:
                
                # 🔥 FILTER ERROR
                if item is None:
                    continue
                
                if not isinstance(item, dict):
                    continue

                row = item.copy()
                row["_parent_id"] = parent_id
                rows.append(row)

    return pd.DataFrame(rows)


# ================= MAIN =================
data = get_all_data()

# ===== MAIN TABLE =====
flat_data = [flatten_json(d) for d in data]
df_main = pd.DataFrame(flat_data)

print("Main table created")

# ===== DETECT REPEAT COLUMNS =====
repeat_columns = []

for key in data[0].keys():
    if isinstance(data[0][key], list):
        repeat_columns.append(key)

print("Repeat columns:", repeat_columns)

# ===== SAVE TO EXCEL =====
with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    df_main.to_excel(writer, sheet_name="DATA", index=False)

    for repeat in repeat_columns:
        df_repeat = extract_repeat(data, repeat)

        if not df_repeat.empty:
            df_repeat.to_excel(writer, sheet_name=repeat[:30], index=False)

print(f"Data exported to {OUTPUT_FILE}")

# ================= GOOGLE SHEETS =================
print("Uploading to Google Sheets...")

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_name(
    "credentials.json", scope
)

client = gspread.authorize(creds)

# ⚠️ pastikan nama sama persis
sheet = client.open("Kobo Monitoring").sheet1

sheet.clear()

# 🔥 FIX ERROR NaN
df_main = df_main.fillna("")

sheet.update(
    [df_main.columns.values.tolist()] +
    df_main.values.tolist()
)

print("✅ Data berhasil dikirim ke Google Sheets")