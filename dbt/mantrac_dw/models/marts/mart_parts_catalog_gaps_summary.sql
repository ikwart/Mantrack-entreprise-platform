-- One row per (model, component) combination with at least one logged gap -
-- the "where does our catalog actually need work" view. Gaps clustering
-- around one model/component pair are a clear signal of where to prioritize
-- catalog completion; scattered one-off gaps are lower priority.

select
    model_id,
    model_name,
    category_name,
    component_id,
    component_name,
    system_category,
    count(*) as total_gaps,
    count(*) filter (where not resolved) as unresolved_gaps,
    min(scoring_date) as first_seen,
    max(scoring_date) as last_seen
from {{ ref('mart_parts_catalog_gaps') }}
group by model_id, model_name, category_name, component_id, component_name, system_category
order by total_gaps desc
