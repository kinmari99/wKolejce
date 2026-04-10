import sys
from pathlib import Path
from datetime import datetime, timedelta

#from prefect import flow, task, get_run_logger
from sqlalchemy import text
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))
from db import get_engine


SENTINEL_NO_DATE = datetime(9999, 12, 31)


def _scalar(conn, sql: str, params: dict | None = None):
    return conn.execute(text(sql), params or {}).scalar()


def _get_distinct_dates_for_batch(conn, load_batch_id: int) -> list[datetime]:
    rows = conn.execute(
        text("""
            select distinct cast(info_date as date) as dt
            from rds.nfz_raw_wait_times
            where load_batch_id = :load_batch_id
              and info_date is not null

            union

            select distinct cast(first_available_date as date) as dt
            from rds.nfz_raw_wait_times
            where load_batch_id = :load_batch_id
              and first_available_date is not null
              and cast(first_available_date as date) <> '9999-12-31'
        """),
        {"load_batch_id": load_batch_id}
    ).fetchall()

    return [r[0] for r in rows if r[0] is not None]


#@task
def validate_batch_exists(load_batch_id: int):
    engine = get_engine()

    with engine.connect() as conn:
        exists = _scalar(
            conn,
            """
            select count(1)
            from rds.load_batch
            where load_batch_id = :load_batch_id
            """,
            {"load_batch_id": load_batch_id},
        )

        if not exists:
            raise ValueError(f"Nie istnieje load_batch_id={load_batch_id} w rds.load_batch")

        raw_count = _scalar(
            conn,
            """
            select count(1)
            from rds.nfz_raw_wait_times
            where load_batch_id = :load_batch_id
            """,
            {"load_batch_id": load_batch_id},
        )

        if raw_count == 0:
            raise ValueError(f"Brak danych w rds.nfz_raw_wait_times dla load_batch_id={load_batch_id}")

        snapshot_exists = _scalar(
            conn,
            """
            select count(1)
            from dw.dim_load_snapshot
            where load_batch_id = :load_batch_id
            """,
            {"load_batch_id": load_batch_id},
        )

        if snapshot_exists > 0:
            raise ValueError(
                f"Dla load_batch_id={load_batch_id} istnieją już rekordy w dw.dim_load_snapshot. "
                f"Ten batch wygląda na już przetworzony do DW."
            )

    print(f"Walidacja OK dla batcha {load_batch_id}")


#@task
def load_dim_case():
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(
            text("""
                if not exists (select 1 from dw.dim_case where case_name = 'PRZYPADEK PILNY')
                    insert into dw.dim_case(case_name) values ('PRZYPADEK PILNY');

                if not exists (select 1 from dw.dim_case where case_name = 'PRZYPADEK STABILNY')
                    insert into dw.dim_case(case_name) values ('PRZYPADEK STABILNY');
            """)
        )

    print("Załadowano/uzupełniono dw.dim_case")


#@task
def load_dim_voivodeship(load_batch_id: int):
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(
            text("""
                insert into dw.dim_voivodeship (voivodeship_name, nfz_code)
                select distinct
                    r.voivodeship,
                    r.nfz_code
                from rds.nfz_raw_wait_times r
                left join dw.dim_voivodeship v
                    on v.voivodeship_name = r.voivodeship
                    or v.nfz_code = r.nfz_code
                where r.load_batch_id = :load_batch_id
                  and r.voivodeship is not null
                  and r.nfz_code is not null
                  and v.voivodeship_key is null
            """),
            {"load_batch_id": load_batch_id}
        )

    print("Załadowano/uzupełniono dw.dim_voivodeship")


#@task
def load_dim_benefit(load_batch_id: int):
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(
            text("""
                insert into dw.dim_benefit
                (
                    healthcare_benefits_code,
                    benefits_name,
                    is_active
                )
                select distinct
                    r.healthcare_benefits_code,
                    r.benefits_name,
                    1
                from rds.nfz_raw_wait_times r
                left join dw.dim_benefit b
                    on b.healthcare_benefits_code = r.healthcare_benefits_code
                where r.load_batch_id = :load_batch_id
                  and r.healthcare_benefits_code is not null
                  and r.benefits_name is not null
                  and b.benefit_key is null
            """),
            {"load_batch_id": load_batch_id}
        )


        conn.execute(
            text("""
                update b
                set b.benefits_name = r.benefits_name,
                    b.is_active = 1
                from dw.dim_benefit b
                join (
                    select distinct healthcare_benefits_code, benefits_name
                    from rds.nfz_raw_wait_times
                    where load_batch_id = :load_batch_id
                ) r
                  on b.healthcare_benefits_code = r.healthcare_benefits_code
                where isnull(b.benefits_name, '') <> isnull(r.benefits_name, '')
            """),
            {"load_batch_id": load_batch_id}
        )

    print("Załadowano/uzupełniono dw.dim_benefit")


#@task
def load_dim_provider(load_batch_id: int):
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(
            text("""
                insert into dw.dim_provider
                (
                    provider_code,
                    provider_name,
                    voivodeship_key,
                    is_active
                )
                select distinct
                    r.provider_code,
                    r.provider_name,
                    v.voivodeship_key,
                    1
                from rds.nfz_raw_wait_times r
                join dw.dim_voivodeship v
                    on v.voivodeship_name = r.voivodeship
                   and v.nfz_code = r.nfz_code
                left join dw.dim_provider p
                    on p.provider_code = r.provider_code
                where r.load_batch_id = :load_batch_id
                  and p.provider_key is null
            """),
            {"load_batch_id": load_batch_id}
        )

        conn.execute(
            text("""
                update p
                set
                    p.provider_name = r.provider_name,
                    p.voivodeship_key = v.voivodeship_key,
                    p.is_active = 1
                from dw.dim_provider p
                join (
                    select distinct
                        provider_code,
                        provider_name,
                        voivodeship,
                        nfz_code
                    from rds.nfz_raw_wait_times
                    where load_batch_id = :load_batch_id
                ) r
                    on p.provider_code = r.provider_code
                join dw.dim_voivodeship v
                    on v.voivodeship_name = r.voivodeship
                   and v.nfz_code = r.nfz_code
            """),
            {"load_batch_id": load_batch_id}
        )

    print("Załadowano/uzupełniono dw.dim_provider")


#@task
def load_dim_provider_unit(load_batch_id: int):
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(
            text("""
                insert into dw.dim_provider_unit
                (
                    provider_key,
                    internal_provider_code,
                    internal_provider_name,
                    city,
                    district,
                    tel_number,
                    is_active
                )
                select distinct
                    p.provider_key,
                    r.internal_provider_code,
                    r.internal_provider_name,
                    r.city,
                    r.district,
                    r.tel_number,
                    1
                from rds.nfz_raw_wait_times r
                join dw.dim_provider p
                    on p.provider_code = r.provider_code
                left join dw.dim_provider_unit pu
                    on pu.provider_key = p.provider_key
                   and pu.internal_provider_code = r.internal_provider_code
                where r.load_batch_id = :load_batch_id
                  and pu.provider_unit_key is null
            """),
            {"load_batch_id": load_batch_id}
        )

        conn.execute(
            text("""
                update pu
                set
                    pu.internal_provider_name = src.internal_provider_name,
                    pu.city = src.city,
                    pu.district = src.district,
                    pu.tel_number = src.tel_number,
                    pu.is_active = 1
                from dw.dim_provider_unit pu
                join (
                    select distinct
                        p.provider_key,
                        r.internal_provider_code,
                        r.internal_provider_name,
                        r.city,
                        r.district,
                        r.tel_number
                    from rds.nfz_raw_wait_times r
                    join dw.dim_provider p
                        on p.provider_code = r.provider_code
                    where r.load_batch_id = :load_batch_id
                ) src
                    on pu.provider_key = src.provider_key
                   and pu.internal_provider_code = src.internal_provider_code
            """),
            {"load_batch_id": load_batch_id}
        )

    print("Załadowano/uzupełniono dw.dim_provider_unit")

#@task
def load_dim_location(load_batch_id: int):
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(
            text("""
                insert into dw.dim_location
                (
                    city,
                    district,
                    voivodeship_key,
                    latitude,
                    longitude
                )
                select distinct
                    r.city,
                    r.district,
                    v.voivodeship_key,
                    null as latitude,
                    null as longitude
                from rds.nfz_raw_wait_times r
                join dw.dim_voivodeship v
                    on v.voivodeship_name = r.voivodeship
                   and v.nfz_code = r.nfz_code
                left join dw.dim_location l
                    on l.city = r.city
                   and isnull(l.district, '') = isnull(r.district, '')
                   and l.voivodeship_key = v.voivodeship_key
                where r.load_batch_id = :load_batch_id
                  and l.location_key is null
            """),
            {"load_batch_id": load_batch_id}
        )

    print("Załadowano/uzupełniono dw.dim_location")


#@task
def load_dim_date(load_batch_id: int):
    engine = get_engine()

    with engine.begin() as conn:
        dates = _get_distinct_dates_for_batch(conn, load_batch_id)

        if not dates:
            print("Brak dat do załadowania do dw.dim_date")
            return

        for d in dates:
            date_key = int(pd.Timestamp(d).strftime("%Y%m%d"))
            month_number = d.month
            quarter_number = ((d.month - 1) // 3) + 1

    
            day_of_week = d.weekday() + 1
            is_weekend = 1 if day_of_week in (6, 7) else 0

            conn.execute(
                text("""
                    if not exists (
                        select 1 from dw.dim_date where date_key = :date_key
                    )
                    begin
                        insert into dw.dim_date
                        (
                            date_key,
                            full_date,
                            day_number,
                            month_number,
                            month_name,
                            quarter_number,
                            year_number,
                            day_of_week,
                            is_weekend
                        )
                        values
                        (
                            :date_key,
                            :full_date,
                            :day_number,
                            :month_number,
                            :month_name,
                            :quarter_number,
                            :year_number,
                            :day_of_week,
                            :is_weekend
                        )
                    end
                """),
                {
                    "date_key": date_key,
                    "full_date": d,
                    "day_number": d.day,
                    "month_number": month_number,
                    "month_name": d.strftime("%B"),
                    "quarter_number": quarter_number,
                    "year_number": d.year,
                    "day_of_week": day_of_week,
                    "is_weekend": is_weekend,
                }
            )

    print("Załadowano/uzupełniono dw.dim_date")


#@task
def load_dim_load_snapshot(load_batch_id: int):
    engine = get_engine()

    with engine.begin() as conn:
        batch_meta = conn.execute(
            text("""
                select
                    load_batch_id,
                    file_name,
                    file_date,
                    status
                from rds.load_batch
                where load_batch_id = :load_batch_id
            """),
            {"load_batch_id": load_batch_id}
        ).mappings().one()

        voivodeships = conn.execute(
            text("""
                select distinct
                    v.voivodeship_key
                from rds.nfz_raw_wait_times r
                join dw.dim_voivodeship v
                    on v.voivodeship_name = r.voivodeship
                   and v.nfz_code = r.nfz_code
                where r.load_batch_id = :load_batch_id
            """),
            {"load_batch_id": load_batch_id}
        ).fetchall()

        for row in voivodeships:
            voivodeship_key = row[0]

            previous_snapshot_key = _scalar(
                conn,
                """
                select top 1 snapshot_key
                from dw.dim_load_snapshot
                where voivodeship_key = :voivodeship_key
                  and current_flag = 1
                order by snapshot_date desc, snapshot_key desc
                """,
                {"voivodeship_key": voivodeship_key},
            )

            if previous_snapshot_key is not None:
                conn.execute(
                    text("""
                        update dw.dim_load_snapshot
                        set current_flag = 0
                        where snapshot_key = :snapshot_key
                    """),
                    {"snapshot_key": previous_snapshot_key}
                )

            snapshot_date = datetime.utcnow()

            conn.execute(
                text("""
                    insert into dw.dim_load_snapshot
                    (
                        load_batch_id,
                        file_name,
                        file_date,
                        voivodeship_key,
                        snapshot_date,
                        current_flag,
                        previous_snapshot_key,
                        load_status
                    )
                    values
                    (
                        :load_batch_id,
                        :file_name,
                        :file_date,
                        :voivodeship_key,
                        :snapshot_date,
                        1,
                        :previous_snapshot_key,
                        :load_status
                    )
                """),
                {
                    "load_batch_id": batch_meta["load_batch_id"],
                    "file_name": batch_meta["file_name"],
                    "file_date": batch_meta["file_date"],
                    "voivodeship_key": voivodeship_key,
                    "snapshot_date": snapshot_date,
                    "previous_snapshot_key": previous_snapshot_key,
                    "load_status": batch_meta["status"],
                }
            )

    print("Załadowano dw.dim_load_snapshot")


#@task
def load_fact_waiting_list_snapshot(load_batch_id: int):
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(
            text("""
                insert into dw.fact_waiting_list_snapshot
                (
                    snapshot_key,
                    voivodeship_key,
                    benefit_key,
                    provider_key,
                    provider_unit_key,
                    case_key,
                    info_date_key,
                    first_available_date_key,
                    no_of_ppl_waiting,
                    no_of_ppl_checked,
                    avg_waiting_time,
                    days_to_first_available,
                    source_raw_id,
                    is_available
                )
                select
                    s.snapshot_key,
                    v.voivodeship_key,
                    b.benefit_key,
                    p.provider_key,
                    pu.provider_unit_key,
                    c.case_key,
                    dd_info.date_key as info_date_key,
                    dd_first.date_key as first_available_date_key,
                    r.no_of_ppl_waiting,
                    r.no_of_ppl_checked,
                    r.avg_waiting_time,
                    case
                        when cast(r.first_available_date as date) = '9999-12-31' then null
                        when r.first_available_date is null then null
                        else datediff(
                            day,
                            cast(r.info_date as date),
                            cast(r.first_available_date as date)
                        )
                    end as days_to_first_available,
                    r.source_row_num as source_raw_id,
                    case
                        when cast(r.first_available_date as date) = '9999-12-31' then 0
                        when r.first_available_date is null then 0
                        else 1
                    end as is_available
                from rds.nfz_raw_wait_times r
                join dw.dim_voivodeship v
                    on v.voivodeship_name = r.voivodeship
                   and v.nfz_code = r.nfz_code
                join dw.dim_benefit b
                    on b.healthcare_benefits_code = r.healthcare_benefits_code
                join dw.dim_provider p
                    on p.provider_code = r.provider_code
                join dw.dim_provider_unit pu
                    on pu.provider_key = p.provider_key
                   and pu.internal_provider_code = r.internal_provider_code
                join dw.dim_case c
                    on c.case_name = r.category
                join dw.dim_date dd_info
                    on dd_info.full_date = cast(cast(r.info_date as date) as datetime2)
                left join dw.dim_date dd_first
                    on dd_first.full_date = cast(cast(r.first_available_date as date) as datetime2)
                   and cast(r.first_available_date as date) <> '9999-12-31'
                join dw.dim_load_snapshot s
                    on s.load_batch_id = r.load_batch_id
                   and s.voivodeship_key = v.voivodeship_key
                where r.load_batch_id = :load_batch_id
            """),
            {"load_batch_id": load_batch_id}
        )

    print("Załadowano dw.fact_waiting_list_snapshot")


#@task
def update_load_batch_status_on_dw_success(load_batch_id: int):
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(
            text("""
                update rds.load_batch
                set load_end_dt = sysutcdatetime(),
                    status = 'SUCCESS'
                where load_batch_id = :load_batch_id
                  and status <> 'SUCCESS'
            """),
            {"load_batch_id": load_batch_id}
        )

    print(f"Batch {load_batch_id} oznaczony jako SUCCESS")


def run_transform(load_batch_id: int):
    validate_batch_exists(load_batch_id)
    load_dim_case()
    load_dim_voivodeship(load_batch_id)
    load_dim_benefit(load_batch_id)
    load_dim_provider(load_batch_id)
    load_dim_provider_unit(load_batch_id)
    load_dim_location(load_batch_id)
    load_dim_date(load_batch_id)
    load_dim_load_snapshot(load_batch_id)
    load_fact_waiting_list_snapshot(load_batch_id)
    update_load_batch_status_on_dw_success(load_batch_id)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        raise ValueError("Podaj load_batch_id, np. python pipelines/transform_rds_to_dw_local.py 7")

    batch_id = int(sys.argv[1])
    run_transform(batch_id)