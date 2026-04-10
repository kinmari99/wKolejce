import sys
from pathlib import Path

from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parents[1]))
from db import get_engine


def _scalar(conn, sql: str, params: dict | None = None):
    return conn.execute(text(sql), params or {}).scalar()


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

    print(f"Batch {load_batch_id} istnieje")


def validate_current_snapshots_for_batch(load_batch_id: int):
    engine = get_engine()

    with engine.connect() as conn:
        current_cnt = _scalar(
            conn,
            """
            select count(*)
            from dw.dim_load_snapshot
            where load_batch_id = :load_batch_id
              and current_flag = 1
              and load_status = 'SUCCESS'
            """,
            {"load_batch_id": load_batch_id},
        )

        if current_cnt == 0:
            raise ValueError(
                f"Batch {load_batch_id} nie ma aktualnych snapshotów "
                f"(current_flag = 1, load_status = 'SUCCESS')."
            )

    print(f"Batch {load_batch_id} ma aktualne snapshoty")


def delete_ads_for_affected_voivodeships(load_batch_id: int):
    engine = get_engine()

    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                select distinct voivodeship_key
                from dw.dim_load_snapshot
                where load_batch_id = :load_batch_id
                  and current_flag = 1
                  and load_status = 'SUCCESS'
            """),
            {"load_batch_id": load_batch_id}
        ).fetchall()

        if not rows:
            raise ValueError(
                f"Brak województw do odświeżenia w ADS dla batcha {load_batch_id}"
            )

        voivodeship_keys = [row[0] for row in rows]
        placeholders = ", ".join([f":vk{i}" for i in range(len(voivodeship_keys))])
        params = {f"vk{i}": v for i, v in enumerate(voivodeship_keys)}

        deleted = conn.execute(
            text(f"""
                delete from ads.current_waiting_list
                where voivodeship_key in ({placeholders})
            """),
            params
        )

    print(
        f"Usunięto stare rekordy ADS dla województw z batcha {load_batch_id}. "
        f"Rows affected: {deleted.rowcount}"
    )


def load_ads_current_waiting_list(load_batch_id: int):
    engine = get_engine()

    with engine.begin() as conn:
        inserted = conn.execute(
            text("""
                ;with current_snapshots as (
                    select
                        s.snapshot_key,
                        s.voivodeship_key,
                        s.snapshot_date,
                        s.file_date
                    from dw.dim_load_snapshot s
                    where s.load_batch_id = :load_batch_id
                      and s.current_flag = 1
                      and s.load_status = 'SUCCESS'
                )
                insert into ads.current_waiting_list
                (
                    snapshot_key,
                    provider_key,
                    benefit_key,
                    case_key,
                    voivodeship_key,
                    location_key,

                    voivodeship_name,
                    healthcare_benefits_code,
                    benefits_name,
                    case_name,

                    provider_code,
                    provider_name,
                    internal_provider_code,
                    internal_provider_name,
                    provider_display_name,

                    city,
                    district,
                    tel_number,

                    snapshot_date,
                    file_date,
                    info_date,
                    first_available_date,

                    no_of_ppl_waiting,
                    no_of_ppl_checked,
                    avg_waiting_time,
                    days_to_first_available,
                    is_available,

                    latitude,
                    longitude,

                    wait_bucket,
                    is_preferred_provider,
                    is_excluded_provider,
                    provider_unit_key
                )
                select
                    cs.snapshot_key,
                    p.provider_key,
                    b.benefit_key,
                    c.case_key,
                    v.voivodeship_key,
                    l.location_key,

                    v.voivodeship_name,
                    b.healthcare_benefits_code,
                    b.benefits_name,
                    c.case_name,

                    p.provider_code,
                    p.provider_name,
                    pu.internal_provider_code,
                    pu.internal_provider_name,
                    case
                        when pu.internal_provider_name is not null
                             and ltrim(rtrim(pu.internal_provider_name)) <> ''
                            then concat(p.provider_name, ' - ', pu.internal_provider_name)
                        else p.provider_name
                    end as provider_display_name,

                    pu.city,
                    pu.district,
                    pu.tel_number,

                    cs.snapshot_date,
                    cs.file_date,
                    d_info.full_date as info_date,
                    d_first.full_date as first_available_date,

                    f.no_of_ppl_waiting,
                    f.no_of_ppl_checked,
                    f.avg_waiting_time,
                    f.days_to_first_available,
                    f.is_available,

                    l.latitude,
                    l.longitude,

                    case
                        when f.is_available = 0 then 'niedostępne'
                        when f.avg_waiting_time is null then null
                        when f.avg_waiting_time = 0 then '0 dni'
                        when f.avg_waiting_time between 1 and 30 then '1-30 dni'
                        when f.avg_waiting_time between 31 and 90 then '31-90 dni'
                        when f.avg_waiting_time between 91 and 180 then '91-180 dni'
                        else '180+ dni'
                    end as wait_bucket,

                    cast(0 as bit) as is_preferred_provider,
                    cast(0 as bit) as is_excluded_provider,
                    pu.provider_unit_key
                from dw.fact_waiting_list_snapshot f
                join current_snapshots cs
                    on cs.snapshot_key = f.snapshot_key
                join dw.dim_voivodeship v
                    on v.voivodeship_key = f.voivodeship_key
                join dw.dim_benefit b
                    on b.benefit_key = f.benefit_key
                join dw.dim_case c
                    on c.case_key = f.case_key
                join dw.dim_provider p
                    on p.provider_key = f.provider_key
                join dw.dim_provider_unit pu
                    on pu.provider_unit_key = f.provider_unit_key
                join dw.dim_date d_info
                    on d_info.date_key = f.info_date_key
                left join dw.dim_date d_first
                    on d_first.date_key = f.first_available_date_key
                left join dw.dim_location l
                    on l.city = pu.city
                   and isnull(l.district, '') = isnull(pu.district, '')
                   and l.voivodeship_key = v.voivodeship_key
            """),
            {"load_batch_id": load_batch_id}
        )

    print(f"Załadowano ADS. Rows inserted: {inserted.rowcount}")


def validate_ads_rowcount(load_batch_id: int):
    engine = get_engine()

    with engine.connect() as conn:
        ads_count = _scalar(
            conn,
            """
            select count(*)
            from ads.current_waiting_list a
            join dw.dim_load_snapshot s
                on s.snapshot_key = a.snapshot_key
            where s.load_batch_id = :load_batch_id
            """,
            {"load_batch_id": load_batch_id},
        )

        fact_count = _scalar(
            conn,
            """
            select count(*)
            from dw.fact_waiting_list_snapshot f
            join dw.dim_load_snapshot s
                on s.snapshot_key = f.snapshot_key
            where s.load_batch_id = :load_batch_id
              and s.current_flag = 1
              and s.load_status = 'SUCCESS'
            """,
            {"load_batch_id": load_batch_id},
        )

    print(f"FACT current count: {fact_count}")
    print(f"ADS count: {ads_count}")

    if ads_count != fact_count:
        print("UWAGA: liczba rekordów ADS różni się od liczby current factów.")
    else:
        print("Walidacja OK: ADS = current FACT")


def run_ads_pipeline(load_batch_id: int):
    validate_batch_exists(load_batch_id)
    validate_current_snapshots_for_batch(load_batch_id)
    delete_ads_for_affected_voivodeships(load_batch_id)
    load_ads_current_waiting_list(load_batch_id)
    validate_ads_rowcount(load_batch_id)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise ValueError(
            "Podaj load_batch_id, np. python pipelines/load_ads_current_waiting_list_local.py 11"
        )

    batch_id = int(sys.argv[1])
    run_ads_pipeline(batch_id)