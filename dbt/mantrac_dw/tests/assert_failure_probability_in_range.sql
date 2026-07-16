-- Singular test: fails (returns rows) if any failure_probability falls
-- outside [0, 1]. Written by hand rather than pulling in dbt_utils for a
-- one-line check - keeps the project dependency-free.
select signal_id, failure_probability
from {{ ref('mart_predictive_maintenance') }}
where failure_probability < 0 or failure_probability > 1
