select
    1 as setup_ok,
    '{{ target.schema }}' as output_dataset,
    current_timestamp() as checked_at
