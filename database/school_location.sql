CREATE TABLE main.school_location (
    calendar_year          INTEGER NOT NULL,
    acara_sml_id           INTEGER NOT NULL,
    location_age_id        INTEGER,

    school_age_id          INTEGER,
    rolled_school_id       INTEGER,

    school_name            VARCHAR(200),
    suburb                 VARCHAR(100),
    state                  CHAR(3),
    postcode               CHAR(4),

    school_sector          VARCHAR(20),
    school_type            VARCHAR(20),
    special_school_flag    INTEGER,

    campus_type            VARCHAR(30),

    latitude               DOUBLE,
    longitude              DOUBLE,

    abs_ra_code            INTEGER,
    abs_ra_name            VARCHAR(30),

    meshblock_code         VARCHAR(15),
    sa1_code               VARCHAR(15),
    sa2_code               VARCHAR(15),
    sa2_name               VARCHAR(100),
    sa3_code               VARCHAR(15),
    sa3_name               VARCHAR(100),
    sa4_code               VARCHAR(15),
    sa4_name               VARCHAR(100),

    lga_code               VARCHAR(15),
    lga_name               VARCHAR(100),

    state_ed_code          VARCHAR(15),
    state_ed_name          VARCHAR(100),

    cth_ed_code            VARCHAR(15),
    cth_ed_name            VARCHAR(100),

    UNIQUE (calendar_year, acara_sml_id, location_age_id)
);
