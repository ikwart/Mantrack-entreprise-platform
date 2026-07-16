-- One row per logged catalog gap - a signal whose predicted component
-- couldn't be resolved to any part compatible with that specific equipment
-- model (see pipeline/resolve_parts_demand.py for where these get logged).
--
-- This answers a real catalog-management question: "is our parts catalog
-- actually complete enough to support the predictive maintenance program?"
-- If gaps cluster around one equipment model or component, that's exactly
-- where catalog coverage needs work - invisible without this view, since an
-- unresolved signal otherwise just silently produces no parts forecast.

select
    g.gap_id,
    g.signal_id,
    s.scoring_date,
    s.equipment_id,
    e.serial_number,
    e.customer_id,
    c.customer_name,
    i.industry_name,
    g.component_id,
    comp.component_name,
    comp.system_category,
    g.model_id,
    em.model_name,
    ec.category_name,
    g.resolved
from {{ ref('stg_parts_mapping_gaps') }} g
left join {{ ref('stg_ml_signals') }} s on g.signal_id = s.signal_id
left join {{ ref('stg_equipment') }} e on s.equipment_id = e.equipment_id
left join {{ ref('stg_customers') }} c on e.customer_id = c.customer_id
left join {{ ref('stg_industries') }} i on c.industry_id = i.industry_id
left join {{ ref('stg_components') }} comp on g.component_id = comp.component_id
left join {{ ref('stg_equipment_models') }} em on g.model_id = em.model_id
left join {{ ref('stg_equipment_categories') }} ec on em.category_id = ec.category_id
