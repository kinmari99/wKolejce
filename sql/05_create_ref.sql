create table ref.city_coordinates (
    city_coordinates_id int identity(1,1) primary key,
    city_name nvarchar(100) not null,
    voivodeship_name nvarchar(100) null,

    latitude decimal(9,6) not null,
    longitude decimal(9,6) not null,

    source_name nvarchar(100) null,
    created_at datetime2 not null default sysutcdatetime(),

    constraint uq_ref_city_coordinates
        unique (city_name, voivodeship_name)
);