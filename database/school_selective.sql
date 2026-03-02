CREATE TABLE main.school_selective (
    school_id                    INTEGER NOT NULL,
    selective_status             VARCHAR(100),
    selective_status_desc        VARCHAR(100),
    fully_selective              VARCHAR(100),
    PRIMARY KEY (school_id)
);
