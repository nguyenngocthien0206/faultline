from dagster import DailyPartitionsDefinition

# A fixed end date: by default Dagster only offers partitions up to today,
# while generated datasets can run into the future.
daily = DailyPartitionsDefinition(start_date="2024-01-01", end_date="2031-01-01")
