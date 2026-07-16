select
    filing_id,
    customer_id,
    period_start,
    period_end,
    vat_amount,
    nhil_amount,
    getfund_amount,
    vat_amount + nhil_amount + getfund_amount as total_output_tax,
    withholding_tax_amount,
    filing_status
from {{ source('raw', 'fact_tax_filings') }}
