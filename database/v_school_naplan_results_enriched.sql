CREATE OR REPLACE VIEW main.v_school_naplan_results_enriched AS
WITH BASE AS (
SELECT
    r.calendar_year,
    r.acara_sml_id,
    r.y3_read,
    r.y3_write,
    r.y3_spell,
    r.y3_grammar,
    r.y3_math,
    CASE WHEN r.y3_read IS NOT NULL AND r.y3_write IS NOT NULL AND r.y3_spell IS NOT NULL
           AND r.y3_grammar IS NOT NULL AND r.y3_math IS NOT NULL THEN
                (r.y3_read + r.y3_write + r.y3_spell + r.y3_grammar + r.y3_math) / 5.0
         ELSE NULL END AS y3_avg,
    r.y5_read,
    r.y5_write,
    r.y5_spell,
    r.y5_grammar,
    r.y5_math,
    CASE WHEN r.y5_read IS NOT NULL AND r.y5_write IS NOT NULL AND r.y5_spell IS NOT NULL
           AND r.y5_grammar IS NOT NULL AND r.y5_math IS NOT NULL THEN
                (r.y5_read + r.y5_write + r.y5_spell + r.y5_grammar + r.y5_math) / 5.0
         ELSE NULL END AS y5_avg,
    r.y7_read,
    r.y7_write,
    r.y7_spell,
    r.y7_grammar,
    r.y7_math,
    CASE WHEN r.y7_read IS NOT NULL AND r.y7_write IS NOT NULL AND r.y7_spell IS NOT NULL
           AND r.y7_grammar IS NOT NULL AND r.y7_math IS NOT NULL THEN
                (r.y7_read + r.y7_write + r.y7_spell + r.y7_grammar + r.y7_math) / 5.0
         ELSE NULL END AS y7_avg,
    r.y9_read,
    r.y9_write,
    r.y9_spell,
    r.y9_grammar,
    r.y9_math,
    CASE WHEN r.y9_read IS NOT NULL AND r.y9_write IS NOT NULL AND r.y9_spell IS NOT NULL
           AND r.y9_grammar IS NOT NULL AND r.y9_math IS NOT NULL THEN
                (r.y9_read + r.y9_write + r.y9_spell + r.y9_grammar + r.y9_math) / 5.0
         ELSE NULL END AS y9_avg
FROM main.school_naplan_results AS r
), BASE_WTIH_PERCENTILES AS (
SELECT
       b.*,       
       QUANTILE(b.y3_avg , 0.02) OVER (PARTITION BY b.calendar_year) AS y3_p02_score,
       QUANTILE(b.y3_avg , 0.98) OVER (PARTITION BY b.calendar_year) AS y3_p98_score,
       QUANTILE(b.y5_avg , 0.02) OVER (PARTITION BY b.calendar_year) AS y5_p02_score,
       QUANTILE(b.y5_avg , 0.98) OVER (PARTITION BY b.calendar_year) AS y5_p98_score,
       QUANTILE(b.y7_avg , 0.02) OVER (PARTITION BY b.calendar_year) AS y7_p02_score,
       QUANTILE(b.y7_avg , 0.98) OVER (PARTITION BY b.calendar_year) AS y7_p98_score,
       QUANTILE(b.y9_avg , 0.02) OVER (PARTITION BY b.calendar_year) AS y9_p02_score,
       QUANTILE(b.y9_avg , 0.98) OVER (PARTITION BY b.calendar_year) AS y9_p98_score
FROM
       BASE b
), BASE_OUTLIER_SMOOTHED AS (
SELECT
       bp.*,
       CASE
         WHEN bp.y3_avg < bp.y3_p02_score THEN bp.y3_p02_score
         WHEN bp.y3_avg > bp.y3_p98_score THEN bp.y3_p98_score + (bp.y3_avg - bp.y3_p98_score) * 0.2 
         ELSE bp.y3_avg
       END y3_avg_smoothed,
       CASE
         WHEN bp.y5_avg < bp.y5_p02_score THEN bp.y5_p02_score
         WHEN bp.y5_avg > bp.y5_p98_score THEN bp.y5_p98_score + (bp.y5_avg - bp.y5_p98_score) * 0.2 
         ELSE bp.y5_avg
       END y5_avg_smoothed,
       CASE
         WHEN bp.y7_avg < bp.y7_p02_score THEN bp.y7_p02_score
         WHEN bp.y7_avg > bp.y7_p98_score THEN bp.y7_p98_score + (bp.y7_avg - bp.y7_p98_score) * 0.2 
         ELSE bp.y7_avg
       END y7_avg_smoothed,
       CASE
         WHEN bp.y9_avg < bp.y9_p02_score THEN bp.y9_p02_score
         WHEN bp.y9_avg > bp.y9_p98_score THEN bp.y9_p98_score + (bp.y9_avg - bp.y9_p98_score) * 0.2 
         ELSE bp.y9_avg
       END y9_avg_smoothed
FROM
       BASE_WTIH_PERCENTILES bp
), Z AS (
  SELECT
    t.*,
    -- Y3 Z
    CASE WHEN y3_avg_smoothed IS NULL THEN NULL ELSE
      (y3_avg_smoothed - AVG(y3_avg_smoothed) OVER (PARTITION BY calendar_year))
      / NULLIF(STDDEV_POP(y3_avg_smoothed) OVER (PARTITION BY calendar_year), 0)
    END AS y3z,
    -- Y5 Z
    CASE WHEN y5_avg_smoothed IS NULL THEN NULL ELSE
      (y5_avg_smoothed - AVG(y5_avg_smoothed) OVER (PARTITION BY calendar_year))
      / NULLIF(STDDEV_POP(y5_avg_smoothed) OVER (PARTITION BY calendar_year), 0)
    END AS y5z,
    -- Y7 Z
    CASE WHEN y7_avg_smoothed IS NULL THEN NULL ELSE
      (y7_avg_smoothed - AVG(y7_avg_smoothed) OVER (PARTITION BY calendar_year))
      / NULLIF(STDDEV_POP(y7_avg_smoothed) OVER (PARTITION BY calendar_year), 0)
    END AS y7z,
    -- Y9 Z
    CASE WHEN y9_avg_smoothed IS NULL THEN NULL ELSE
      (y9_avg_smoothed - AVG(y9_avg_smoothed) OVER (PARTITION BY calendar_year))
      / NULLIF(STDDEV_POP(y9_avg_smoothed) OVER (PARTITION BY calendar_year), 0)
    END AS y9z
  FROM BASE_OUTLIER_SMOOTHED t
),
B AS (
  SELECT
    z.*,
    MIN(y3z) OVER (PARTITION BY calendar_year) AS y3z_min,
    MAX(y3z) OVER (PARTITION BY calendar_year) AS y3z_max,
    MIN(y5z) OVER (PARTITION BY calendar_year) AS y5z_min,
    MAX(y5z) OVER (PARTITION BY calendar_year) AS y5z_max,
    MIN(y7z) OVER (PARTITION BY calendar_year) AS y7z_min,
    MAX(y7z) OVER (PARTITION BY calendar_year) AS y7z_max,
    MIN(y9z) OVER (PARTITION BY calendar_year) AS y9z_min,
    MAX(y9z) OVER (PARTITION BY calendar_year) AS y9z_max
  FROM z
), BYR AS (
SELECT
  b.*,
  -- Y3 Rating
  CASE WHEN y3z IS NULL THEN NULL ELSE
    ((y3z + ABS(y3z_min)) / NULLIF((y3z_max - y3z_min), 0)) * 9 + 1
  END AS y3_zrating,
  -- Y5 Rating
  CASE WHEN y5z IS NULL THEN NULL ELSE
    ((y5z + ABS(y5z_min)) / NULLIF((y5z_max - y5z_min), 0)) * 9 + 1
  END AS y5_zrating,
  -- Y7 Rating
  CASE WHEN y7z IS NULL THEN NULL ELSE
    ((y7z + ABS(y7z_min)) / NULLIF((y7z_max - y7z_min), 0)) * 9 + 1
  END AS y7_zrating,
  -- Y9 Rating
  CASE WHEN y9z IS NULL THEN NULL ELSE
    ((y9z + ABS(y9z_min)) / NULLIF((y9z_max - y9z_min), 0)) * 9 + 1
  END AS y9_zrating
FROM B
), BASE_WITH_FINAL_RATINGS AS (
SELECT
       b.*,
       CASE
        WHEN sp.state = 'SA' AND b.calendar_year < 2022 AND (COALESCE(b.y3_zrating,0) + COALESCE(b.y5_zrating,0) + COALESCE(b.y7_zrating,0)) > 0 THEN
          (COALESCE(b.y3_zrating,0) + COALESCE(b.y5_zrating,0) + COALESCE(b.y7_zrating,0)) /
          ( (CASE WHEN b.y3_zrating > 0 THEN 1 ELSE 0 END ) + (CASE WHEN b.y5_zrating > 0 THEN 1 ELSE 0 END )  + (CASE WHEN b.y7_zrating > 0 THEN 1 ELSE 0 END ) )
        WHEN (COALESCE(b.y3_zrating,0) + COALESCE(b.y5_zrating,0)) > 0 THEN
          (COALESCE(b.y3_zrating,0) + COALESCE(b.y5_zrating,0)) / 
          ( (CASE WHEN b.y3_zrating > 0 THEN 1 ELSE 0 END ) + (CASE WHEN b.y5_zrating > 0 THEN 1 ELSE 0 END ) )
        ELSE
          NULL
      END primary_rating,
       CASE
        WHEN sp.state = 'SA' AND b.calendar_year < 2022 AND (b.y9_zrating) > 0 THEN
          (b.y9_zrating)
        WHEN (COALESCE(b.y7_zrating,0) + COALESCE(b.y9_zrating,0)) > 0 THEN
          (COALESCE(b.y7_zrating,0) + COALESCE(b.y9_zrating,0)) / 
          ( (CASE WHEN b.y7_zrating > 0 THEN 1 ELSE 0 END ) + (CASE WHEN b.y9_zrating > 0 THEN 1 ELSE 0 END ) )
        ELSE
          NULL
      END secondary_rating
FROM
        BYR b
JOIN
       main.school_profile sp
       ON b.acara_sml_id = sp.acara_sml_id
       AND b.calendar_year = sp.calendar_year
)
SELECT
        -- Profile
        b.calendar_year,
        b.acara_sml_id,
        sp.school_name,
        sp.suburb,
        sp.state,
        sp.postcode,
        sp.campus_type,
        sp.reporting_type,
        sp.school_sector,
        ss.selective_status,
        ss.fully_selective,
        sp.school_type,
        sp.year_range,
        sp.icsea_score,
        sp.icsea_percentile,
        sp.sea_bottom_pct,
        sp.sea_lower_mid_pct,
        sp.sea_upper_mid_pct,
        sp.sea_top_pct,
        sp.teaching_staff_fte,
        sp.total_enrolments,
        sp.girls_enrolments,
        sp.boys_enrolments,
        ROUND(CASE WHEN sp.total_enrolments > 0 THEN
          (sp.boys_enrolments * 100.0 / sp.total_enrolments)
        ELSE
          NULL
        END, 0) AS boys_enrol_pct,
        ROUND(CASE WHEN sp.total_enrolments > 0 THEN
          (sp.girls_enrolments * 100.0 / sp.total_enrolments)
        ELSE
          NULL
        END, 0) AS girls_enrol_pct,
        sp.indigenous_enrol_pct,
        sp.lbote_yes_pct,
        --Location
        sl.latitude,
        sl.longitude,
        sl.sa2_name,
        sl.sa3_name,
        sl.sa4_name,
        sl.lga_name,
        sl.capital_city,
        -- Enrolments by grade
        seg.y1_enrolments,
        seg.y2_enrolments,
        seg.y3_enrolments,
        seg.y4_enrolments,
        seg.y5_enrolments,
        seg.y6_enrolments,
        seg.y7_enrolments,
        seg.y8_enrolments,
        seg.y9_enrolments,
        seg.y10_enrolments,
        seg.y11_enrolments,
        seg.y12_enrolments,
        -- Ratings and underlying scores
        b.y3_read,
        b.y3_write,
        b.y3_spell,
        b.y3_grammar,
        b.y3_math,
        b.y3_avg,
        b.y3_avg_smoothed,
        ROUND(b.y3_zrating,3) AS y3_zrating,
        b.y5_read,
        b.y5_write,
        b.y5_spell,
        b.y5_grammar,
        b.y5_math,
        b.y5_avg,
        b.y5_avg_smoothed,
        ROUND(b.y5_zrating,3) AS y5_zrating,
        b.y7_read,
        b.y7_write,
        b.y7_spell,
        b.y7_grammar,
        b.y7_math,
        b.y7_avg,
        b.y7_avg_smoothed,
        ROUND(b.y7_zrating,3) AS y7_zrating,
        b.y9_read,
        b.y9_write,
        b.y9_spell,
        b.y9_grammar,
        b.y9_math,
        b.y9_avg,
        b.y9_avg_smoothed,
        ROUND(b.y9_zrating,3) AS y9_zrating,
        ROUND(b.primary_rating,3) AS primary_rating,
        ROUND(b.secondary_rating,3) AS secondary_rating,
        --Rankings
        CASE WHEN primary_rating IS NULL OR sp.total_enrolments <= 100 THEN NULL
             ELSE DENSE_RANK() OVER (
               PARTITION BY b.calendar_year
               ORDER BY CASE WHEN sp.total_enrolments > 100 OR sp.total_enrolments IS NULL THEN primary_rating ELSE NULL END DESC
             )
        END AS primary_rank_au,
        CASE WHEN primary_rating IS NULL OR sp.total_enrolments <= 100 THEN NULL
             ELSE DENSE_RANK() OVER (
               PARTITION BY b.calendar_year, sp.state
               ORDER BY CASE WHEN sp.total_enrolments > 100 OR sp.total_enrolments IS NULL THEN primary_rating ELSE NULL END DESC
             )
        END AS primary_rank_state,
        CASE WHEN primary_rating IS NULL OR sp.total_enrolments <= 100 THEN NULL
             ELSE DENSE_RANK() OVER (
               PARTITION BY b.calendar_year, sp.school_sector
               ORDER BY CASE WHEN sp.total_enrolments > 100 OR sp.total_enrolments IS NULL THEN primary_rating ELSE NULL END DESC
             )
        END AS primary_rank_sector,
        CASE WHEN primary_rating IS NULL OR sp.total_enrolments <= 100 THEN NULL
             ELSE DENSE_RANK() OVER (
               PARTITION BY b.calendar_year, sp.state, sp.school_sector
               ORDER BY CASE WHEN sp.total_enrolments > 100 OR sp.total_enrolments IS NULL THEN primary_rating ELSE NULL END DESC
             )
        END AS primary_rank_state_sector,
        CASE WHEN secondary_rating IS NULL OR sp.total_enrolments <= 100 THEN NULL
             ELSE DENSE_RANK() OVER (
               PARTITION BY b.calendar_year
               ORDER BY CASE WHEN sp.total_enrolments > 100 OR sp.total_enrolments IS NULL THEN secondary_rating ELSE NULL END DESC
             )
        END AS secondary_rank_au,
        CASE WHEN secondary_rating IS NULL OR sp.total_enrolments <= 100 THEN NULL
             ELSE DENSE_RANK() OVER (
               PARTITION BY b.calendar_year, sp.state
               ORDER BY CASE WHEN sp.total_enrolments > 100 OR sp.total_enrolments IS NULL THEN secondary_rating ELSE NULL END DESC
             )
        END AS secondary_rank_state,
        CASE WHEN secondary_rating IS NULL OR sp.total_enrolments <= 100 THEN NULL
             ELSE DENSE_RANK() OVER (
               PARTITION BY b.calendar_year, sp.school_sector
               ORDER BY CASE WHEN sp.total_enrolments > 100 OR sp.total_enrolments IS NULL THEN secondary_rating ELSE NULL END DESC
             )
        END AS secondary_rank_sector,
        CASE WHEN secondary_rating IS NULL OR sp.total_enrolments <= 100 THEN NULL
             ELSE DENSE_RANK() OVER (
               PARTITION BY b.calendar_year, sp.state, sp.school_sector
               ORDER BY CASE WHEN sp.total_enrolments > 100 OR sp.total_enrolments IS NULL THEN secondary_rating ELSE NULL END DESC
             )
        END AS secondary_rank_state_sector,
        -- Ranking in percentiles (higher = better)
        ROUND(CASE WHEN primary_rating IS NULL OR sp.total_enrolments <= 100 THEN NULL
             ELSE 1.0 - PERCENT_RANK() OVER (
               PARTITION BY b.calendar_year
               ORDER BY CASE WHEN sp.total_enrolments > 100 OR sp.total_enrolments IS NULL THEN primary_rating ELSE NULL END DESC
             )
        END, 5) AS primary_pct_au,
        ROUND(CASE WHEN primary_rating IS NULL OR sp.total_enrolments <= 100 THEN NULL
             ELSE 1.0 - PERCENT_RANK() OVER (
               PARTITION BY b.calendar_year, sp.state
               ORDER BY CASE WHEN sp.total_enrolments > 100 OR sp.total_enrolments IS NULL THEN primary_rating ELSE NULL END DESC
             )
        END, 5) AS primary_pct_state,
        ROUND(CASE WHEN primary_rating IS NULL OR sp.total_enrolments <= 100 THEN NULL
             ELSE 1.0 - PERCENT_RANK() OVER (
               PARTITION BY b.calendar_year, sp.school_sector
               ORDER BY CASE WHEN sp.total_enrolments > 100 OR sp.total_enrolments IS NULL THEN primary_rating ELSE NULL END DESC
             )
        END, 5) AS primary_pct_sector,
        ROUND(CASE WHEN primary_rating IS NULL OR sp.total_enrolments <= 100 THEN NULL
             ELSE 1.0 - PERCENT_RANK() OVER (
               PARTITION BY b.calendar_year, sp.state, sp.school_sector
               ORDER BY CASE WHEN sp.total_enrolments > 100 OR sp.total_enrolments IS NULL THEN primary_rating ELSE NULL END DESC
             )
        END, 5) AS primary_pct_state_sector,
        ROUND(CASE WHEN secondary_rating IS NULL OR sp.total_enrolments <= 100 THEN NULL
             ELSE 1.0 - PERCENT_RANK() OVER (
               PARTITION BY b.calendar_year
               ORDER BY CASE WHEN sp.total_enrolments > 100 OR sp.total_enrolments IS NULL THEN secondary_rating ELSE NULL END DESC
             )
        END, 5) AS secondary_pct_au,
        ROUND(CASE WHEN secondary_rating IS NULL OR sp.total_enrolments <= 100 THEN NULL
             ELSE 1.0 - PERCENT_RANK() OVER (
               PARTITION BY b.calendar_year, sp.state
               ORDER BY CASE WHEN sp.total_enrolments > 100 OR sp.total_enrolments IS NULL THEN secondary_rating ELSE NULL END DESC
             )
        END, 5) AS secondary_pct_state,
        ROUND(CASE WHEN secondary_rating IS NULL OR sp.total_enrolments <= 100 THEN NULL
             ELSE 1.0 - PERCENT_RANK() OVER (
               PARTITION BY b.calendar_year, sp.school_sector
               ORDER BY CASE WHEN sp.total_enrolments > 100 OR sp.total_enrolments IS NULL THEN secondary_rating ELSE NULL END DESC
             )
        END, 5) AS secondary_pct_sector,
        ROUND(CASE WHEN secondary_rating IS NULL OR sp.total_enrolments <= 100 THEN NULL
             ELSE 1.0 - PERCENT_RANK() OVER (
               PARTITION BY b.calendar_year, sp.state, sp.school_sector
               ORDER BY CASE WHEN sp.total_enrolments > 100 OR sp.total_enrolments IS NULL THEN secondary_rating ELSE NULL END DESC
             )
        END, 5) AS secondary_pct_state_sector,
        CASE WHEN b.calendar_year = MAX(b.calendar_year) OVER () THEN 1 ELSE 0 END AS is_latest
FROM
        BASE_WITH_FINAL_RATINGS b
JOIN
       main.school_profile sp
       ON b.acara_sml_id = sp.acara_sml_id
       AND b.calendar_year = sp.calendar_year
JOIN
       main.v_school_location sl
       ON b.acara_sml_id = sl.acara_sml_id
       AND sl.is_latest = 1
LEFT JOIN
       main.school_enrolment_by_grade seg
       ON b.acara_sml_id = seg.acara_sml_id
       AND b.calendar_year = seg.calendar_year
LEFT JOIN
       main.school_selective ss
       ON b.acara_sml_id = ss.school_id
;
