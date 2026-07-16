-- One row per signal, enriched with equipment/customer/component context.
-- Powers the Predictive Maintenance dashboard: signal funnel, ranked risk
-- list, and the predictive-vs-reactive ratio (computed here from
-- fact_maintenance_events.source_signal_id, not from the signals table
-- itself, since that ratio describes actual booked work, not raw signals).

select
    s.signal_id,
    s.equipment_id,
    e.serial_number,
    e.customer_id,
    c.customer_name,
    c.contract_tier,
    em.model_name,
    ec.category_name,
    s.scoring_date,
    s.predicted_component_id,
    comp.component_name,
    comp.system_category,
    s.failure_probability,
    s.model_version,
    s.recommended_action_window_days,
    s.signal_status,
    s.dismiss_reason,
    wo.work_order_id as resulting_work_order_id,
    wo.scheduled_date as resulting_scheduled_date
from {{ ref('stg_ml_signals') }} s
left join {{ ref('stg_equipment') }} e on s.equipment_id = e.equipment_id
left join {{ ref('stg_customers') }} c on e.customer_id = c.customer_id
left join {{ ref('stg_equipment_models') }} em on e.model_id = em.model_id
left join {{ ref('stg_equipment_categories') }} ec on em.category_id = ec.category_id
left join {{ ref('stg_components') }} comp on s.predicted_component_id = comp.component_id
left join {{ ref('stg_maintenance_events') }} wo on s.signal_id = wo.source_signal_id
