CREATE TABLE main.school_profile (
    calendar_year                INTEGER NOT NULL,
    acara_sml_id                 INTEGER NOT NULL,

    location_age_id              INTEGER,
    school_age_id                INTEGER,

    school_name                  VARCHAR(200),
    suburb                       VARCHAR(100),
    state                        CHAR(3),
    postcode                     CHAR(4),

    school_sector                VARCHAR(20),
    school_type                  VARCHAR(20),
    campus_type                  VARCHAR(30),
    reporting_type               VARCHAR(30),

    school_url                   VARCHAR(300),
    governing_body               VARCHAR(200),
    governing_body_url           VARCHAR(300),

    year_range                   VARCHAR(20),
    geolocation                  VARCHAR(30),

    icsea_score                  INTEGER,
    icsea_percentile             INTEGER,

    sea_bottom_pct               DECIMAL(5,2),
    sea_lower_mid_pct            DECIMAL(5,2),
    sea_upper_mid_pct            DECIMAL(5,2),
    sea_top_pct                  DECIMAL(5,2),

    teaching_staff_cnt           INTEGER,
    teaching_staff_fte           DECIMAL(6,1),
    non_teaching_staff_cnt       INTEGER,
    non_teaching_staff_fte       DECIMAL(6,1),

    total_enrolments             INTEGER,
    girls_enrolments             INTEGER,
    boys_enrolments              INTEGER,
    enrolments_fte               DECIMAL(6,1),

    indigenous_enrol_pct         DECIMAL(5,2),
    lbote_yes_pct                DECIMAL(5,2),
    lbote_no_pct                 DECIMAL(5,2),
    lbote_not_stated_pct         DECIMAL(5,2),

    PRIMARY KEY (calendar_year, acara_sml_id)
    
);
