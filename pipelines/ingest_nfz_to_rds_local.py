import sys
from pathlib import Path
import hashlib
import re
import pandas as pd
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[1]))

from db import get_engine

LOCAL_FILE = Path("data/downloaded/małopolskie.xlsx")


def get_file_hash(file_bytes: bytes) -> str:
    return hashlib.sha256(file_bytes).hexdigest()[:20]


def batch_exists(engine, file_hash: str) -> bool:
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                select count(1)
                from rds.load_batch
                where file_hash = :file_hash
            """),
            {"file_hash": file_hash}
        )
        return result.scalar() > 0


def insert_load_batch(
    engine,
    file_name: str,
    file_url: str,
    file_hash: str,
    file_date,
    rows_raw: int,
    rows_loaded: int,
    status: str,
    error_message: str | None = None,
) -> int:
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                insert into rds.load_batch
                (
                    load_start_dt,
                    load_end_dt,
                    source_system,
                    file_name,
                    file_url,
                    file_hash,
                    file_date,
                    status,
                    rows_raw,
                    rows_loaded,
                    error_message
                )
                output inserted.load_batch_id
                values
                (
                    sysutcdatetime(),
                    sysutcdatetime(),
                    'NFZ_TERMINY_LECZENIA',
                    :file_name,
                    :file_url,
                    :file_hash,
                    :file_date,
                    :status,
                    :rows_raw,
                    :rows_loaded,
                    :error_message
                )
            """),
            {
                "file_name": file_name[:30],
                "file_url": file_url,
                "file_hash": file_hash,
                "file_date": file_date,
                "status": status,
                "rows_raw": rows_raw,
                "rows_loaded": rows_loaded,
                "error_message": error_message,
            }
        )
        return result.scalar_one()

def clean_int_value(value):
    if pd.isna(value):
        return None

    text = str(value).strip()

    text = text.replace("\xa0", "")
    text = text.replace(" ", "")

    if text == "":
        return None

    return int(text)


def clean_date_value(value):
    if pd.isna(value):
        return None

    dt = pd.to_datetime(value, errors="coerce")

    if pd.isna(dt):
        return None

    return dt.to_pydatetime()

def normalize_dataframe(df: pd.DataFrame, load_batch_id: int, file_name: str) -> pd.DataFrame:
    print("Oryginalne kolumny z Excela:")
    print(list(df.columns))

    column_mapping = {
        "Rok": "year",
        "Miesiąc": "month",
        "Kod OW NFZ": "nfz_code",
        "Nazwa województwa": "voivodeship",
        "Kod świadczenia": "healthcare_benefits_code",
        "Nazwa świadczenia": "benefits_name",
        "Kategoria medyczna": "category",
        "Kod świadczeniodawcy": "provider_code",
        "Nazwa świadczeniodawcy": "provider_name",
        "Kod techniczny komórki": "internal_provider_code",
        "Nazwa komórki": "internal_provider_name",
        "Adres komórki": "cell_address_raw",
        "Liczba oczekujących": "no_of_ppl_waiting",
        "Liczba osob skreślonych": "no_of_ppl_checked",
        "Średni czas oczekiwania": "avg_waiting_time",
        "Pierwszy wolny termin": "first_available_date",
        "Data przygotowania informacji o pierwszym wolnym terminie": "info_date",
    }

    df = df.rename(columns=column_mapping)
    if "cell_address_raw" in df.columns:
        parts = df["cell_address_raw"].astype(str).str.split(";", expand=True)
        location = parts[0].str.strip()
        loc_split = location.str.split("-", n=1, expand=True)
        df["city"] = loc_split[0].str.strip()
        if loc_split.shape[1] > 1:
            df["district"] = loc_split[1].str.strip()
        else:
            df["district"] = None
        if parts.shape[1] > 2:
            df["tel_number"] = parts[2].str.strip()
        elif parts.shape[1] > 1:
            df["tel_number"] = parts[1].str.strip()
        else:
            df["tel_number"] = None

        df["tel_number"] = df["tel_number"].str.replace(r"\s+", " ", regex=True).str.strip()



    df["load_batch_id"] = load_batch_id
    df["source_row_num"] = range(1, len(df) + 1)
    df["source_file_name"] = file_name

    needed_columns = [
        "load_batch_id",
        "source_row_num",
        "source_file_name",
        "year",
        "month",
        "nfz_code",
        "voivodeship",
        "healthcare_benefits_code",
        "benefits_name",
        "category",
        "provider_code",
        "provider_name",
        "internal_provider_code",
        "internal_provider_name",
        "city",
        "district",
        "tel_number",
        "no_of_ppl_waiting",
        "no_of_ppl_checked",
        "avg_waiting_time",
        "first_available_date",
        "info_date",
    ]

    for col in needed_columns:
        if col not in df.columns:
            df[col] = None

    df = df[needed_columns].copy()

    df["month"] = df["month"].astype(str).str.zfill(2)

    
    def make_row_hash(row) -> str:
        raw_text = "|".join(
            "" if pd.isna(v) else str(v)
            for v in [
                row["year"],
                row["month"],
                row["nfz_code"],
                row["voivodeship"],
                row["healthcare_benefits_code"],
                row["benefits_name"],
                row["category"],
                row["provider_code"],
                row["provider_name"],
                row["internal_provider_code"],
                row["internal_provider_name"],
                row["city"],
                row["district"],
                row["tel_number"],
                row["no_of_ppl_waiting"],
                row["no_of_ppl_checked"],
                row["avg_waiting_time"],
                row["first_available_date"],
                row["info_date"],
            ]
        )
        return hashlib.sha256(raw_text.encode("utf-8")).hexdigest()

    df["source_row_hash"] = df.apply(make_row_hash, axis=1)

    df["year"] = df["year"].apply(clean_int_value)
    df["no_of_ppl_waiting"] = df["no_of_ppl_waiting"].apply(clean_int_value)
    df["no_of_ppl_checked"] = df["no_of_ppl_checked"].apply(clean_int_value)
    df["avg_waiting_time"] = df["avg_waiting_time"].apply(clean_int_value)
    df["first_available_date"] = df["first_available_date"].apply(clean_date_value)
    df["info_date"] = df["info_date"].apply(clean_date_value)
    df["first_available_date"] = df["first_available_date"].fillna(pd.Timestamp("9999-12-31"))
    df["info_date"] = df["info_date"].fillna(pd.Timestamp("9999-12-31"))

    return df


def validate_required_columns(df: pd.DataFrame):
    required_not_null = [
        "year",
        "month",
        "nfz_code",
        "voivodeship",
        "healthcare_benefits_code",
        "benefits_name",
        "category",
        "provider_code",
        "provider_name",
        "internal_provider_name",
        "city",
        "tel_number",
        "no_of_ppl_waiting",
        "avg_waiting_time",
        "first_available_date",
        "info_date",
    ]

    for col in required_not_null:
        null_count = df[col].isna().sum()
        if null_count > 0:
            raise ValueError(f"Kolumna '{col}' ma {null_count} pustych wartości, a w SQL jest NOT NULL.")


def load_to_rds(engine, df: pd.DataFrame) -> int:
    df.to_sql(
        name="nfz_raw_wait_times",
        con=engine,
        schema="rds",
        if_exists="append",
        index=False
    )
    return len(df)


def main():
    engine = get_engine()

    if not LOCAL_FILE.exists():
        raise FileNotFoundError(f"Nie znaleziono pliku: {LOCAL_FILE}")

    file_name = LOCAL_FILE.name
    file_bytes = LOCAL_FILE.read_bytes()
    file_hash = get_file_hash(file_bytes)

    if batch_exists(engine, file_hash):
        print(f"Plik o hash {file_hash} już istnieje w rds.load_batch. Pomijam.")
        return

    df = pd.read_excel(LOCAL_FILE, header=2)
    print("Wczytano wierszy z Excela:", len(df))

    # na start file_date = czas modyfikacji pliku lokalnego
    file_date = pd.Timestamp(LOCAL_FILE.stat().st_mtime, unit="s")

    # najpierw insert batcha, bo raw wymaga load_batch_id
    load_batch_id = insert_load_batch(
        engine=engine,
        file_name=file_name,
        file_url="LOCAL_FILE",
        file_hash=file_hash,
        file_date=file_date.to_pydatetime(),
        rows_raw=len(df),
        rows_loaded=0,
        status="STARTED",
        error_message=None,
    )

    try:
        df = normalize_dataframe(df, load_batch_id, file_name)
        validate_required_columns(df)

        row_count = load_to_rds(engine, df)

        with engine.begin() as conn:
            conn.execute(
                text("""
                    update rds.load_batch
                    set
                        load_end_dt = sysutcdatetime(),
                        status = 'SUCCESS',
                        rows_loaded = :rows_loaded
                    where load_batch_id = :load_batch_id
                """),
                {"rows_loaded": row_count, "load_batch_id": load_batch_id}
            )

        print(f"Sukces. Załadowano {row_count} rekordów do rds.nfz_raw_wait_times")

    except Exception as e:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    update rds.load_batch
                    set
                        load_end_dt = sysutcdatetime(),
                        status = 'FAILED',
                        error_message = :error_message
                    where load_batch_id = :load_batch_id
                """),
                {
                    "error_message": str(e)[:1000],
                    "load_batch_id": load_batch_id
                }
            )
        print("Błąd:", e)
        raise


if __name__ == "__main__":
    main()