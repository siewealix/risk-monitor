from pathlib import Path
import sqlite3
import pandas as pd

DB_PATH = Path("data/risk_monitor_dataset.sqlite")


def load_table(conn, table_name):
    return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)


def load_all_tables(conn):
    table_names = ["users", "subscriptions", "memberships", "payments", "complaints"]
    return {table_name: load_table(conn, table_name) for table_name in table_names}


def normalize_text(value):
    if pd.isna(value):
        return pd.NA
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return pd.NA
    return text


def normalize_lower_text(value):
    text = normalize_text(value)
    if pd.isna(text):
        return pd.NA
    return text.lower()


def canonicalize_label(value):
    text = normalize_lower_text(value)
    if pd.isna(text):
        return pd.NA
    text = text.replace(" - ", "_")
    text = text.replace("-", "_")
    text = text.replace(" ", "_")
    while "__" in text:
        text = text.replace("__", "_")
    return text


def normalize_country(value):
    text = normalize_text(value)
    if pd.isna(text):
        return pd.NA
    text = text.upper()

    mapping = {
        "FRANCE": "FR",
        "FRA": "FR",
        "FR": "FR",
        "ESPAGNE": "ES",
        "SPAIN": "ES",
        "ESP": "ES",
        "ES": "ES",
        "GERMANY": "DE",
        "DEUTSCHLAND": "DE",
        "DEU": "DE",
        "DE": "DE",
        "ITALY": "IT",
        "ITA": "IT",
        "IT": "IT",
        "UNITED KINGDOM": "GB",
        "UK": "GB",
        "GBR": "GB",
        "GB": "GB",
        "UNITED STATES": "US",
        "USA": "US",
        "US": "US",
        "CAMEROON": "CM",
        "CMR": "CM",
        "CM": "CM",
    }

    return mapping.get(text, text)


def normalize_phone_prefix(value):
    text = normalize_text(value)
    if pd.isna(text):
        return pd.NA
    text = text.replace(" ", "")
    if text.isdigit():
        return f"+{text}"
    return text


def normalize_currency(value):
    text = normalize_text(value)
    if pd.isna(text):
        return pd.NA
    text = text.upper()

    mapping = {
        "€": "EUR",
        "EURO": "EUR",
        "EUROS": "EUR",
        "EUR": "EUR",
        "$": "USD",
        "US$": "USD",
        "USD": "USD",
        "GBP": "GBP",
    }

    return mapping.get(text, text)


def normalize_payment_status(value):
    text = canonicalize_label(value)
    if pd.isna(text):
        return pd.NA

    mapping = {
        "succeeded": "succeeded",
        "success": "succeeded",
        "suceeded": "succeeded",
        "succeed": "succeeded",
        "failed": "failed",
        "fail": "failed",
        "failure": "failed",
        "pending": "pending",
        "refunded": "refunded",
        "refund": "refunded",
        "disputed": "disputed",
        "chargeback": "disputed",
    }

    return mapping.get(text, text)


def normalize_complaint_status(value):
    text = canonicalize_label(value)
    if pd.isna(text):
        return pd.NA

    mapping = {
        "open": "open",
        "opened": "open",
        "in_progress": "in_progress",
        "inprogress": "in_progress",
        "escalated": "escalated",
        "resolved": "resolved",
        "closed": "closed",
    }

    return mapping.get(text, text)


def normalize_complaint_type(value):
    text = canonicalize_label(value)
    if pd.isna(text):
        return pd.NA

    mapping = {
        "access_denied": "access_denied",
        "accès_refusé": "access_denied",
        "acces_refuse": "access_denied",
        "billing_issue": "billing_issue",
        "billingissue": "billing_issue",
        "subscription_inactive": "subscription_inactive",
        "subscriptioninactive": "subscription_inactive",
    }

    return mapping.get(text, text)


def parse_mixed_datetime(value):
    if pd.isna(value):
        return pd.NaT

    text = str(value).strip()

    if text == "" or text.lower() == "nan":
        return pd.NaT

    if text.isdigit():
        if len(text) == 10:
            parsed = pd.to_datetime(int(text), unit="s", utc=True, errors="coerce")
        elif len(text) == 13:
            parsed = pd.to_datetime(int(text), unit="ms", utc=True, errors="coerce")
        else:
            parsed = pd.to_datetime(text, utc=True, errors="coerce", dayfirst=True)
    else:
        parsed = pd.to_datetime(text, utc=True, errors="coerce", dayfirst=True)

    if pd.isna(parsed):
        return pd.NaT

    return parsed.tz_localize(None)


def parse_mixed_datetime_series(series):
    parsed = series.apply(parse_mixed_datetime)
    return pd.to_datetime(parsed, errors="coerce")


def clean_users(df):
    df = df.copy()
    df["email_clean"] = df["email"].apply(normalize_lower_text)
    df["country_clean"] = df["country"].apply(normalize_country)
    df["phone_prefix_clean"] = df["phone_prefix"].apply(normalize_phone_prefix)
    df["signup_date_clean"] = parse_mixed_datetime_series(df["signup_date"])
    df["last_seen_clean"] = parse_mixed_datetime_series(df["last_seen"])
    df["status_code"] = pd.to_numeric(df["status"], errors="coerce")
    return df


def clean_subscriptions(df):
    df = df.copy()
    df["brand_clean"] = df["brand"].apply(normalize_lower_text)
    df["currency_clean"] = df["currency"].apply(normalize_currency)
    df["created_at_clean"] = parse_mixed_datetime_series(df["created_at"])
    df["status_code"] = pd.to_numeric(df["status"], errors="coerce")
    return df


def clean_memberships(df):
    df = df.copy()
    df["joined_at_clean"] = parse_mixed_datetime_series(df["joined_at"])
    df["left_at_clean"] = parse_mixed_datetime_series(df["left_at"])
    df["reason_clean"] = df["reason"].apply(canonicalize_label)
    df["status_code"] = pd.to_numeric(df["status"], errors="coerce")
    return df


def clean_payments(df):
    df = df.copy()
    df["status_clean"] = df["status"].apply(normalize_payment_status)
    df["currency_clean"] = df["currency"].apply(normalize_currency)
    df["created_at_clean"] = parse_mixed_datetime_series(df["created_at"])
    df["captured_at_clean"] = parse_mixed_datetime_series(df["captured_at"])
    df["stripe_error_code_clean"] = df["stripe_error_code"].apply(canonicalize_label)
    return df


def clean_complaints(df):
    df = df.copy()
    df["type_clean"] = df["type"].apply(normalize_complaint_type)
    df["status_clean"] = df["status"].apply(normalize_complaint_status)
    df["resolution_clean"] = df["resolution"].apply(canonicalize_label)
    df["created_at_clean"] = parse_mixed_datetime_series(df["created_at"])
    df["resolved_at_clean"] = parse_mixed_datetime_series(df["resolved_at"])
    return df


def clean_all_tables(raw_tables):
    return {
        "users": clean_users(raw_tables["users"]),
        "subscriptions": clean_subscriptions(raw_tables["subscriptions"]),
        "memberships": clean_memberships(raw_tables["memberships"]),
        "payments": clean_payments(raw_tables["payments"]),
        "complaints": clean_complaints(raw_tables["complaints"]),
    }


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Base introuvable : {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)

    try:
        raw_tables = load_all_tables(conn)
    finally:
        conn.close()

    cleaned_tables = clean_all_tables(raw_tables)

    print("\nCleaning module executed successfully.")

    print("\n=== PAYMENTS STATUS BEFORE CLEANING ===")
    print(raw_tables["payments"]["status"].astype(str).value_counts(dropna=False).head(20).to_string())

    print("\n=== PAYMENTS STATUS AFTER CLEANING ===")
    print(cleaned_tables["payments"]["status_clean"].astype(str).value_counts(dropna=False).head(20).to_string())

    print("\n=== COMPLAINTS STATUS BEFORE CLEANING ===")
    print(raw_tables["complaints"]["status"].astype(str).value_counts(dropna=False).head(20).to_string())

    print("\n=== COMPLAINTS STATUS AFTER CLEANING ===")
    print(cleaned_tables["complaints"]["status_clean"].astype(str).value_counts(dropna=False).head(20).to_string())

    print("\n=== SUBSCRIPTIONS CURRENCY BEFORE CLEANING ===")
    print(raw_tables["subscriptions"]["currency"].astype(str).value_counts(dropna=False).head(20).to_string())

    print("\n=== SUBSCRIPTIONS CURRENCY AFTER CLEANING ===")
    print(cleaned_tables["subscriptions"]["currency_clean"].astype(str).value_counts(dropna=False).head(20).to_string())

    print("\n=== USERS DATE PREVIEW ===")
    print(
        cleaned_tables["users"][
            ["signup_date", "signup_date_clean", "last_seen", "last_seen_clean"]
        ].head(10).to_string(index=False)
    )


if __name__ == "__main__":
    main()