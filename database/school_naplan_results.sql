CREATE TABLE main.school_naplan_results (
    calendar_year                INTEGER NOT NULL,
    acara_sml_id        INTEGER NOT NULL,

    y3_read             INTEGER,
    y3_write            INTEGER,
    y3_spell            INTEGER,
    y3_grammar          INTEGER,
    y3_math             INTEGER,

    y5_read             INTEGER,
    y5_write            INTEGER,
    y5_spell            INTEGER,
    y5_grammar          INTEGER,
    y5_math             INTEGER,

    y7_read             INTEGER,
    y7_write            INTEGER,
    y7_spell            INTEGER,
    y7_grammar          INTEGER,
    y7_math             INTEGER,

    y9_read             INTEGER,
    y9_write            INTEGER,
    y9_spell            INTEGER,
    y9_grammar          INTEGER,
    y9_math             INTEGER,

    PRIMARY KEY (year, acara_sml_id)
);
