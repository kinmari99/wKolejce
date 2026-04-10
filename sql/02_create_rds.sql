CREATE TABLE rds.load_batch( --rejestr każdego uruchomienia joba
load_batch_id int identity(1,1) not null,
load_start_dt datetime2 not null default sysutcdatetime(),
load_end_dt datetime2 not null,
source_system nvarchar(50) default('NFZ_TERMINY_LECZENIA'),
file_name nvarchar(30) not null,
file_url nvarchar(500) not null,
file_hash nvarchar(20) not null,
file_date datetime2 not null,
status nvarchar(10) not null,
rows_raw int not null,
rows_loaded int not null,
error_message nvarchar(1000)
constraint pk_rds_load_batch primary key (load_batch_id),
constraint chk_rds_load_batch_status
check (status in ('STARTED', 'SUCCESS', 'FAILED')),
constraint chk_rds_load_batch_dates
check (load_end_dt >= load_start_dt),
constraint chk_rds_load_batch_rows_raw
check (rows_raw >= 0),
constraint chk_rds_load_batch_rows_loaded
check (rows_loaded >= 0),
constraint chk_rds_load_batch_rows_logic
check (rows_loaded <= rows_raw),
constraint uq_rds_load_batch_file unique (file_hash),
constraint chk_rds_load_batch_error_logic
check (
    (status = 'FAILED' and error_message is not null)
    or
    (status in ('STARTED', 'SUCCESS'))
))

create table rds.nfz_raw_wait_times(--dane
raw_id int identity(1,1) not null primary key,
load_batch_id int not null, 
source_row_num int null,
source_file_name nvarchar(255) null,
source_insert_dt datetime2 not null default sysutcdatetime(),
source_row_hash nvarchar(64) null,
year int not null,
month nvarchar(2) not null, --trzeba sconvertowac na inta, ale w excelu jest jako tekst
nfz_code nvarchar(2) not null,
voivodeship nvarchar(50) not null,
healthcare_benefits_code nvarchar(8) not null,
benefits_name nvarchar(400) not null,
category nvarchar(25) not null,
provider_code nvarchar(12) not null,
provider_name nvarchar(200) not null,
internal_provider_code nvarchar(10),
internal_provider_name nvarchar(400) not null,
city nvarchar(50) not null,
district nvarchar(75),
tel_number nvarchar(30) not null,
no_of_ppl_waiting int not null,
no_of_ppl_checked int,
avg_waiting_time int not null,
first_available_date datetime2 not null,
info_date datetime2 not null
constraint fk_rds_nfz_raw_wait_times_load_batch
    foreign key (load_batch_id)
    references rds.load_batch(load_batch_id),
	constraint chk_rds_nfz_raw_wait_times_year
    check ([year] between 2000 and 2100),
	constraint chk_rds_nfz_raw_wait_times_month
    check (try_convert(int, [month]) between 1 and 12),
	constraint chk_rds_nfz_raw_wait_times_no_of_ppl_waiting
    check (no_of_ppl_waiting is null or no_of_ppl_waiting >= 0),
	constraint chk_rds_nfz_raw_wait_times_avg_waiting_time
    check (avg_waiting_time is null or avg_waiting_time >= 0),
	constraint chk_rds_nfz_raw_wait_times_dates
    check (
        first_available_date is null
        or info_date is null
        or first_available_date >= info_date
    )
	)