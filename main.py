import pandas as pd

# ================= LOAD =================
user_referrals = pd.read_csv("data/user_referrals.csv")
user_referral_logs = pd.read_csv("data/user_referral_logs.csv")
user_logs = pd.read_csv("data/user_logs.csv")
statuses = pd.read_csv("data/user_referral_statuses.csv")
rewards = pd.read_csv("data/referral_rewards.csv")
transactions = pd.read_csv("data/paid_transactions.csv")
leads = pd.read_csv("data/lead_log.csv")

# ================= PROFILING =================
tables = {
    "user_referrals": user_referrals,
    "user_referral_logs": user_referral_logs,
    "user_logs": user_logs,
    "statuses": statuses,
    "rewards": rewards,
    "transactions": transactions,
    "leads": leads,
}

profile_rows = []
for name, df in tables.items():
    for col in df.columns:
        profile_rows.append({
            "table": name,
            "column": col,
            "null_count": int(df[col].isna().sum()),
            "distinct_count": int(df[col].nunique())
        })

pd.DataFrame(profile_rows).to_csv("profiling.csv", index=False)

# ================= TYPE FIX =================
def to_dt(df, cols):
    for c in cols:
        df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

user_referrals = to_dt(user_referrals, ["referral_at", "updated_at"])
user_referral_logs = to_dt(user_referral_logs, ["created_at"])
transactions = to_dt(transactions, ["transaction_at"])
user_logs = to_dt(user_logs, ["membership_expired_date"])

rewards["reward_value"] = pd.to_numeric(rewards["reward_value"], errors="coerce")

transactions["transaction_status"] = transactions["transaction_status"].astype(str).str.upper()
transactions["transaction_type"] = transactions["transaction_type"].astype(str).str.upper()

user_logs["is_deleted"] = user_logs["is_deleted"].astype(str).str.lower() == "true"
user_referral_logs["is_reward_granted"] = user_referral_logs["is_reward_granted"].astype(str).str.lower() == "true"

# ================= RENAME TO AVOID COLLISION =================
statuses = statuses.rename(columns={"id": "status_id", "created_at": "status_created_at"})
rewards = rewards.rename(columns={"id": "reward_id", "created_at": "reward_created_at"})
user_referral_logs = user_referral_logs.rename(columns={"id": "referral_details_id"})
leads = leads.rename(columns={"created_at": "lead_created_at"})

# ================= JOIN =================
df = (
    user_referrals
    .merge(user_referral_logs, left_on="referral_id", right_on="user_referral_id", how="left")
    .merge(statuses, left_on="user_referral_status_id", right_on="status_id", how="left")
    .merge(rewards, left_on="referral_reward_id", right_on="reward_id", how="left")
    .merge(transactions, on="transaction_id", how="left")
    .merge(user_logs, left_on="referrer_id", right_on="user_id", how="left", suffixes=("", "_referrer"))
    .merge(leads, left_on="referee_id", right_on="lead_id", how="left")
)

# ================= SOURCE CATEGORY =================
def map_source(row):
    if row["referral_source"] == "User Sign Up":
        return "Online"
    elif row["referral_source"] == "Draft Transaction":
        return "Offline"
    elif row["referral_source"] == "Lead":
        return row.get("source_category")
    return None

df["referral_source_category"] = df.apply(map_source, axis=1)

# ================= BUSINESS LOGIC =================
def is_valid(row):
    try:
        # VALID CASE 1
        if (
            pd.notna(row["reward_value"]) and row["reward_value"] > 0
            and row["description"] == "Berhasil"
            and pd.notna(row["transaction_id"])
            and row["transaction_status"] == "PAID"
            and row["transaction_type"] == "NEW"
            and row["transaction_at"] > row["referral_at"]
            and row["transaction_at"].month == row["referral_at"].month
            and row["membership_expired_date"] > row["referral_at"]
            and row["is_deleted"] is False
            and row["is_reward_granted"] is True
        ):
            return True

        # VALID CASE 2
        if (
            row["description"] in ["Menunggu", "Tidak Berhasil"]
            and pd.isna(row["reward_value"])
        ):
            return True

        return False
    except:
        return False

df["is_business_logic_valid"] = df.apply(is_valid, axis=1)

# ================= OUTPUT =================
output_cols = [
    "referral_details_id",
    "referral_id",
    "referral_source",
    "referral_source_category",
    "referral_at",
    "referrer_id",
    "name",
    "phone_number",
    "homeclub",
    "referee_id",
    "referee_name",
    "referee_phone",
    "description",
    "transaction_id",
    "transaction_status",
    "transaction_at",
    "transaction_location",
    "transaction_type",
    "updated_at",
    "created_at",
    "is_business_logic_valid",
]

final = df[output_cols].dropna()

final.to_csv("output_report.csv", index=False)

print("DONE, rows:", len(final))
