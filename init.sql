CREATE SCHEMA IF NOT EXISTS nrod;

-- T Records (Timing Point Location)
CREATE TABLE IF NOT EXISTS nrod.tiploc (
    tiploc_code                 text PRIMARY KEY,
    nalco                       int,
    check_char                  text,
    tps_description             text,
    stanox                      int,
    crs_code                    text,
    description                 text
);

-- AA Records (Association)
CREATE TABLE IF NOT EXISTS nrod.association (
    id                          integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    main_train_uid              text not null,
    assoc_train_uid             text not null,
    assoc_start_date            date not null,
    assoc_end_date              date not null,
    assoc_days                  bit(7) not null,
    category                    text,
    date_indicator              text,
    location                    text not null,
    base_location_suffix        smallint,
    assoc_location_suffix       smallint,
    diagram_type                text not null DEFAULT 'T',
    association_type            text,
    stp_indicator               text not null
);

CREATE UNIQUE INDEX IF NOT EXISTS ON nrod.association (main_train_uid, assoc_train_uid, assoc_start_date, diagram_type, location, base_location_suffix, assoc_location_suffix, stp_indicator);

-- BS/BX Records (Schedule)
CREATE TABLE IF NOT EXISTS nrod.schedule (
    id                          integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    is_vstp                     boolean not null DEFAULT FALSE,
    train_uid                   text not null,
    schedule_start_date         date not null,
    schedule_end_date           date,
    schedule_days_runs          bit(7),
    train_status                text,
    train_category              text,
    signalling_id               text,
    train_service_code          integer,
    power_type                  text,
    timing_load                 text,
    speed                       smallint,
    operating_characteristics   text,
    train_class                 text,
    sleepers                    text,
    reservations                text,
    catering_code               text,
    service_branding            text,
    stp_indicator               text not null,
    uic_code                    text,
    atoc_code                   text,
    applicable_timetable        boolean,
    last_modified               timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS ON nrod.schedule (train_uid, schedule_start_date, stp_indicator, is_vstp);

-- LO/LI/LT Records (Location)
CREATE TABLE IF NOT EXISTS nrod.schedule_location (
    schedule_id                 integer REFERENCES nrod.schedule (id) ON DELETE CASCADE,
    position                    smallint,
    tiploc_code                 text not null,
    tiploc_instance             smallint,
    arrival_day                 smallint,
    departure_day               smallint,
    arrival                     time without time zone,
    departure                   time without time zone,
    public_arrival              time without time zone,
    public_departure            time without time zone,
    platform                    text,
    line                        text,
    path                        text,
    activity                    text,
    engineering_allowance       text,
    pathing_allowance           text,
    performance_allowance       text,
    PRIMARY KEY (schedule_id, position)
);

-- CR Record (Changes En Route)
CREATE TABLE IF NOT EXISTS nrod.changes_en_route (
    schedule_id                 integer REFERENCES nrod.schedule (id) ON DELETE CASCADE,
    tiploc_code                 text not null,
    tiploc_instance             smallint,
    train_category              text,
    signalling_id               text,
    train_service_code          integer,
    power_type                  text,
    timing_load                 text,
    speed                       smallint,
    operating_characteristics   text,
    train_class                 text,
    sleepers                    text,
    reservations                text,
    catering_code               text,
    service_branding            text,
    uic_code                    text
);

CREATE INDEX IF NOT EXISTS ON nrod.changes_en_route (schedule_id);
