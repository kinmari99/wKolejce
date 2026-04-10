Create table dw.dim_voivodeship(
voivodeship_key int identity(1,1) primary key not null,
voivodeship_name nvarchar(50) not null unique,
nfz_code nvarchar(3) not null unique
)


Create table dw.dim_benefit(
benefit_key int identity(1,1) not null primary key,
[healthcare_benefits_code] [nvarchar](8) not null unique,
[benefits_name] [nvarchar](400) NOT NULL,
is_active bit not null,

    constraint chk_dim_benefit_code_not_empty
        check (len(ltrim(rtrim(healthcare_benefits_code))) > 0),
		    constraint chk_dim_benefit_name_not_empty
        check (len(ltrim(rtrim(benefits_name))) > 0)
)
 
Create table dw.dim_case(
case_key int identity(1,1) not null primary key,
case_name nvarchar(50) not null unique,
 constraint chk_dim_case_name
        check (case_name in ('PRZYPADEK PILNY', 'PRZYPADEK STABILNY'))
)


Create table dw.dim_provider(
provider_key int identity(1,1) not null primary key,
provider_code [nvarchar](12) NOT NULL unique,
	[provider_name] [nvarchar](400) NOT NULL,
	voivodeship_key int not null,
	is_active bit not null default 1,
	constraint fk_dim_provider_voivodeship
    foreign key (voivodeship_key)
    references dw.dim_voivodeship(voivodeship_key),
	constraint chk_dim_provider_provider_code_not_empty
    check (len(ltrim(rtrim(provider_code))) > 0),
	constraint chk_dim_provider_provider_name_not_empty
    check (len(ltrim(rtrim(provider_name))) > 0),
)

Create table dw.dim_date(
date_key int primary key,
full_date datetime2 unique not null,
day_number int not null,
month_number int not null,
month_name nvarchar(50) not null,
quarter_number int not null,
year_number int not null,
day_of_week int not null,
is_weekend bit not null,
constraint chk_dim_date_day
    check (day_number between 1 and 31),
	constraint chk_dim_date_month
    check (month_number between 1 and 12),
	constraint chk_dim_date_quarter
    check (quarter_number between 1 and 4),
	constraint chk_dim_date_day_of_week
    check (day_of_week between 1 and 7),
	constraint chk_dim_date_weekend
    check (
        (day_of_week in (6,7) and is_weekend = 1)
        or
        (day_of_week not in (6,7) and is_weekend = 0)
    )
)

Create table dw.dim_load_snapshot(
snapshot_key int identity(1,1) primary key,
load_batch_id int not null,
[file_name] [nvarchar](30) NOT NULL,
[file_date] [datetime2](7) NOT NULL,
voivodeship_key int not null,
snapshot_date datetime2,
current_flag bit not null default(0),
previous_snapshot_key int,
load_status [nvarchar](10) NOT NULL,
constraint fk_dim_load_snapshot_load_batch
    foreign key (load_batch_id)
    references rds.load_batch(load_batch_id),
	constraint fk_dim_load_snapshot_voivodeship
    foreign key (voivodeship_key)
    references dw.dim_voivodeship(voivodeship_key),
	constraint fk_dim_load_snapshot_previous
    foreign key (previous_snapshot_key)
    references dw.dim_load_snapshot(snapshot_key),
	constraint chk_dim_load_snapshot_current_flag
    check (current_flag in (0,1)),
	constraint chk_dim_load_snapshot_status
    check (load_status in ('SUCCESS','FAILED','STARTED')),
	constraint chk_dim_load_snapshot_dates
    check (snapshot_date >= file_date)
)

Create table dw.fact_waiting_list_snapshot(
fact_waiting_list_snapshot_id int identity(1,1) primary key,
snapshot_key int not null,
voivodeship_key int not null,
benefit_key int not null,
provider_key int not null,
provider_unit_key INT NOT NULL,
case_key int not null,
info_date_key int not null,
first_available_date_key int,
no_of_ppl_waiting int,
no_of_ppl_checked int,
avg_waiting_time int,
days_to_first_available int,
source_raw_id int,
is_available bit,
constraint fk_fact_waiting_list_snapshot_snapshot
    foreign key (snapshot_key)
    references dw.dim_load_snapshot(snapshot_key),
	constraint fk_fact_waiting_list_snapshot_voivodeship
    foreign key (voivodeship_key)
    references dw.dim_voivodeship(voivodeship_key),
	constraint fk_fact_waiting_list_snapshot_benefit
    foreign key (benefit_key)
    references dw.dim_benefit(benefit_key),
	constraint fk_fact_waiting_list_snapshot_provider
    foreign key (provider_key)
    references dw.dim_provider(provider_key),
	constraint fk_fact_waiting_list_snapshot_case
    foreign key (case_key)
    references dw.dim_case(case_key),
	constraint fk_fact_waiting_list_snapshot_info_date
    foreign key (info_date_key)
    references dw.dim_date(date_key),
	constraint fk_fact_waiting_list_snapshot_first_available_date
    foreign key (first_available_date_key)
    references dw.dim_date(date_key),
	constraint chk_fact_waiting_list_snapshot_no_of_ppl_waiting
    check (no_of_ppl_waiting is null or no_of_ppl_waiting >= 0),

constraint chk_fact_waiting_list_snapshot_no_of_ppl_checked
    check (no_of_ppl_checked is null or no_of_ppl_checked >= 0),

constraint chk_fact_waiting_list_snapshot_avg_waiting_time
    check (avg_waiting_time is null or avg_waiting_time >= 0),

constraint chk_fact_waiting_list_snapshot_days_to_first_available
    check (days_to_first_available is null or days_to_first_available >= 0),
	constraint chk_fact_waiting_list_snapshot_is_available
    check (is_available in (0,1)),
	constraint chk_fact_waiting_list_snapshot_availability_logic
    check (
        (is_available = 0 and first_available_date_key is null)
        or
        (is_available = 1 and first_available_date_key is not null),
        CONSTRAINT fk_fact_provider_unit
FOREIGN KEY (provider_unit_key)
REFERENCES dw.dim_provider_unit(provider_unit_key);
    )

)

Create table dw.dim_keyword(
keyword_key int identity(1,1) primary key,
keyword_text nvarchar (50) not null unique,
is_active bit not null default(1)
)


create table dw.bridge_keyword_benefit(
keyword_key int identity(1,1) not null,
benefit_key int not null,
match_priority int not null,
   constraint pk_bridge_keyword_benefit
        primary key (keyword_key, benefit_key),

    constraint fk_bridge_keyword_benefit_keyword
        foreign key (keyword_key)
        references dw.dim_keyword(keyword_key),

    constraint fk_bridge_keyword_benefit_benefit
        foreign key (benefit_key)
        references dw.dim_benefit(benefit_key),

    constraint chk_bridge_keyword_benefit_match_priority
        check (match_priority >= 1)
)


Create table dw.dim_location(
location_key int identity(1,1) primary key,
city nvarchar (50),
district nvarchar(50),
voivodeship_key int,
latitude decimal(9,6),
longitude decimal(9,6),
constraint fk_dim_location_voivodeship
        foreign key (voivodeship_key)
        references dw.dim_voivodeship(voivodeship_key),

    constraint uq_dim_location
        unique (city, district, voivodeship_key),

    constraint chk_dim_location_city_not_empty
        check (len(ltrim(rtrim(city))) > 0),

    constraint chk_dim_location_district_not_empty
        check (
            district is null
            or len(ltrim(rtrim(district))) > 0
        ),

    constraint chk_dim_location_latitude
        check (
            latitude is null
            or latitude between -90 and 90
        ),

    constraint chk_dim_location_longitude
        check (
            longitude is null
            or longitude between -180 and 180
        )
)

USE [wKolejce]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dw].[dim_provider_unit](
    [provider_unit_key] INT IDENTITY(1,1) NOT NULL,
    [provider_key] INT NOT NULL,
    [internal_provider_code] NVARCHAR(10) NOT NULL,
    [internal_provider_name] NVARCHAR(400) NOT NULL,
    [city] NVARCHAR(50) NOT NULL,
    [district] NVARCHAR(75) NULL,
    [tel_number] NVARCHAR(30) NULL,
    [is_active] BIT NOT NULL DEFAULT (1),
    CONSTRAINT pk_dim_provider_unit
        PRIMARY KEY CLUSTERED ([provider_unit_key] ASC),
    CONSTRAINT fk_dim_provider_unit_provider
        FOREIGN KEY (provider_key)
        REFERENCES dw.dim_provider(provider_key),
    CONSTRAINT uq_dim_provider_unit_business
        UNIQUE (provider_key, internal_provider_code),
    CONSTRAINT chk_dim_provider_unit_code_not_empty
        CHECK (LEN(LTRIM(RTRIM(internal_provider_code))) > 0),
    CONSTRAINT chk_dim_provider_unit_name_not_empty
        CHECK (LEN(LTRIM(RTRIM(internal_provider_name))) > 0),
    CONSTRAINT chk_dim_provider_unit_city_not_empty
        CHECK (LEN(LTRIM(RTRIM(city))) > 0)
)


