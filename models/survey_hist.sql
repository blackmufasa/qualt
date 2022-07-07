SELECT {{ dbt_utils.current_timestamp() }} as stg_load_dttm,
    *
FROM rsdw_stg.stg_qltrcs_srvy_data_new
WHERE NOT (
        status = 8
        AND q_total_duration < 2
        AND finished = 0
    );