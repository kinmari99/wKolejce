create table ads.current_waiting_list(
    ads_row_id int identity(1,1) not null,
    snapshot_key int not null,
    provider_key int not null,
    benefit_key int not null,
    case_key int not null,
    voivodeship_key int not null,
    location_key int null,

    voivodeship_name nvarchar(50) not null,
    healthcare_benefits_code nvarchar(8) not null,
    benefits_name nvarchar(200) not null,
    case_name nvarchar(50) not null,

    provider_code nvarchar(12) not null,
    provider_name nvarchar(200) not null,
    internal_provider_code nvarchar(10) null,
    internal_provider_name nvarchar(100) null,
    provider_display_name nvarchar(350) not null,

    city nvarchar(50) not null,
    district nvarchar(75) null,
    tel_number nvarchar(15) null,

    snapshot_date datetime2 not null,
    file_date datetime2 not null,
    info_date datetime2 not null,
    first_available_date datetime2 null,

    no_of_ppl_waiting int null,
    no_of_ppl_checked int null,
    avg_waiting_time int null,
    days_to_first_available int null,
    is_available bit not null,

    latitude decimal(9,6) null,
    longitude decimal(9,6) null,

    wait_bucket nvarchar(20) null,
    is_preferred_provider bit not null
        constraint df_ads_current_waiting_list_is_preferred default (0),
    is_excluded_provider bit not null
        constraint df_ads_current_waiting_list_is_excluded default (0),

    load_dttm datetime2 not null
        constraint df_ads_current_waiting_list_load_dttm default sysutcdatetime(),

    constraint pk_ads_current_waiting_list
        primary key (ads_row_id),

    constraint fk_ads_current_waiting_list_snapshot
        foreign key (snapshot_key)
        references dw.dim_load_snapshot(snapshot_key),

    constraint fk_ads_current_waiting_list_provider
        foreign key (provider_key)
        references dw.dim_provider(provider_key),

    constraint fk_ads_current_waiting_list_benefit
        foreign key (benefit_key)
        references dw.dim_benefit(benefit_key),

    constraint fk_ads_current_waiting_list_case
        foreign key (case_key)
        references dw.dim_case(case_key),

    constraint fk_ads_current_waiting_list_voivodeship
        foreign key (voivodeship_key)
        references dw.dim_voivodeship(voivodeship_key),

    constraint fk_ads_current_waiting_list_location
        foreign key (location_key)
        references dw.dim_location(location_key),

    constraint uq_ads_current_waiting_list_grain
        unique (snapshot_key, benefit_key, provider_key, case_key),

    constraint chk_ads_current_waiting_list_no_of_ppl_waiting
        check (no_of_ppl_waiting is null or no_of_ppl_waiting >= 0),

    constraint chk_ads_current_waiting_list_no_of_ppl_checked
        check (no_of_ppl_checked is null or no_of_ppl_checked >= 0),

    constraint chk_ads_current_waiting_list_avg_waiting_time
        check (avg_waiting_time is null or avg_waiting_time >= 0),

    constraint chk_ads_current_waiting_list_days_to_first_available
        check (days_to_first_available is null or days_to_first_available >= 0),

    constraint chk_ads_current_waiting_list_is_available
        check (is_available in (0,1)),

    constraint chk_ads_current_waiting_list_is_preferred
        check (is_preferred_provider in (0,1)),

    constraint chk_ads_current_waiting_list_is_excluded
        check (is_excluded_provider in (0,1)),

    constraint chk_ads_current_waiting_list_availability_logic
        check (
            (is_available = 0 and first_available_date is null)
            or
            (is_available = 1 and first_available_date is not null)
        ),

    constraint chk_ads_current_waiting_list_provider_code_not_empty
        check (len(ltrim(rtrim(provider_code))) > 0),

    constraint chk_ads_current_waiting_list_provider_name_not_empty
        check (len(ltrim(rtrim(provider_name))) > 0),

    constraint chk_ads_current_waiting_list_benefits_name_not_empty
        check (len(ltrim(rtrim(benefits_name))) > 0),

    constraint chk_ads_current_waiting_list_case_name_not_empty
        check (len(ltrim(rtrim(case_name))) > 0),

    constraint chk_ads_current_waiting_list_city_not_empty
        check (len(ltrim(rtrim(city))) > 0),

    constraint chk_ads_current_waiting_list_latitude
        check (latitude is null or latitude between -90 and 90),

    constraint chk_ads_current_waiting_list_longitude
        check (longitude is null or longitude between -180 and 180)
);