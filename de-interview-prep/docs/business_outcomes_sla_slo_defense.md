# Business Outcomes, SLA / SLO Framework, and Domain Defense

### Executive & Senior Data Engineer Interview Preparation
**Domain:** Healthcare Payer & Financial Claims Lakehouse (Carelon / Elevance Health)  
**Related Docs:** [`interview_answer_sheet_claims_lakehouse.md`](file:///m:/databricks/databricks-wanderbricks/de-interview-prep/docs/interview_answer_sheet_claims_lakehouse.md), [`resume.md`](file:///m:/databricks/databricks-wanderbricks/de-interview-prep/docs/resume.md)

---

## 1. The 60-Second Executive Pitch

When an interviewer asks: *"What was the business outcome of this project and what were you building it for?"*

> *"At Carelon, we were building the **Core Financial & Adjudicated Claims Lakehouse**. In a health insurance organization, claims data isn't just an analytical asset—it directly drives real cash payments. Every month, our regional payer network disburses **\$25M to \$35M in medical reimbursements** to contracted hospitals, clinics, and physicians.
>
> The business outcome was threefold:
> 1. **Financial Integrity & Compliance:** We built an automated 3-way financial reconciliation ledger that guaranteed every dollar billed by clearinghouses reconciled down to the penny against our bank disbursement files, avoiding heavy regulatory penalties under state **Prompt Pay Laws**.
> 2. **Treasury Protection:** We prevented a **\$40M+ phantom cash over-reporting error** in executive treasury forecasts caused by $1:N$ Cartesian fan-out bugs on multi-line inpatient claims.
> 3. **Operational SLA Reliability:** We shrank our batch processing window from **8 hours down to 27 minutes**, guaranteeing that curated Gold reporting was ready by 6:00 AM with a massive 3-hour engineering buffer, while cutting cloud compute DBU spend by over 60%."*

---

## 2. What Were You Building It For? (The Business Architecture)

You were **not** just building a technical pipeline; you were building the **authoritative enterprise source of truth** for three vital business functions:

```text
               ┌────────────────────────────────────────────────────────┐
               │    Raw Clearinghouse Payloads (HIPAA 837 EDI)          │
               └───────────────────────────┬────────────────────────────┘
                                           │
                                           ▼
               ┌────────────────────────────────────────────────────────┐
               │     Carelon Core Financial Claims Lakehouse            │
               │   (Incremental PySpark + Delta Lake Medallion)         │
               └──────┬────────────────────┬────────────────────┬───────┘
                      │                    │                    │
                      ▼                    ▼                    ▼
          ┌──────────────────────┐ ┌───────────────┐ ┌──────────────────────┐
          │ Finance & Treasury   │ │  Claims Ops   │ │ Actuarial & Compliance│
          │                      │ │               │ │                      │
          │ • Cash Forecasting   │ │ • Adjudication│ │ • Medical Loss Ratio │
          │ • Weekly Bank EFTs   │ │   Exceptions  │ │   (MLR - ACA 85%)    │
          │ • Hospital Payouts   │ │ • Quarantine  │ │ • State Prompt Pay   │
          │   ($25M-$35M/month)  │ │   Triaging    │ │   Audit Defense      │
          └──────────────────────┘ └───────────────┘ └──────────────────────┘
```

1. **Finance & Treasury Operations:**
   * Calculated exact net provider liabilities. Treasury needed daily verified figures to fund commercial bank accounts ahead of weekly automated check and Electronic Funds Transfer (EFT) disbursement runs.
2. **Claims Operations & Clinical Review:**
   * Triaged exceptions and adjudication rejections. If an inpatient claim contained invalid CPT procedure codes or missing diagnoses, operations needed immediate notification so clean claims weren't left unpaid in violation of state regulations.
3. **Actuarial & Regulatory Compliance:**
   * Monitored the **Medical Loss Ratio (MLR)**. Under the US Affordable Care Act (ACA), health plans must spend at least **80% to 85% of premium revenue directly on healthcare claims and quality improvement**. Inaccurate claim ledgers directly trigger federal audits, financial restatements, and mandatory premium rebate penalties.

---

## 3. Quantified Business Outcomes (The ROI & Impact)

| Business Outcome | Metric / Impact | The Operational & Financial Reality |
| :--- | :--- | :--- |
| **Statutory Interest Avoidance** | **100% On-Time Disbursement SLA** | Under US State **"Prompt Pay Laws"**, health plans must adjudicate and pay clean claims within 30 days. Late disbursements incur mandatory statutory interest of **10%–12% per year** payable to providers. Ensuring daily pipeline completion eliminated late-payment interest exposure. |
| **Treasury Cash Safeguard** | **Safeguarded \$25M+ Monthly Payouts** | An analyst joining `claims` to `claim_services` created a $1:N$ fan-out multiplying header payments by 3 to 4 procedure lines. Fixing this prevented a **\$40M+ phantom cash requirement** from showing up on executive dashboards. |
| **Cloud FinOps Optimization** | **>60% Compute Cost Reduction** | Slashed runtime from 8 hours on a fixed 8-node `Standard_D16s_v4` cluster to 27 minutes on autoscaling `Standard_E16s_v4` nodes, drastically cutting daily Databricks DBU consumption. |
| **Operational Staff Productivity** | **~35% Review Time Reduction** | Replaced silent job crashes and raw spreadsheets with an automated `quarantine` table with explicit reason codes (`ERR_NULL_PK`, `ERR_INVALID_CODE`), reducing claims operations triage time by roughly a third. |
| **Audit & Governance Readiness** | **Zero Audit Findings (SOC 2 / HIPAA)** | Replaced 40+ brittle, unmonitored SQL views with centralized Unity Catalog dynamic masking and automated extraction anomaly detection on `system.access.audit`. |

---

## 4. SLA vs. SLO vs. SLI Framework

Interviewers probe deeply on the distinctions between **SLI** (the metric), **SLO** (the internal engineering objective), and **SLA** (the business-facing contractual commitment).

```text
        SLI (Metric)                     SLO (Internal Target)              SLA (Business Commitment)
┌───────────────────────────┐      ┌─────────────────────────────┐     ┌───────────────────────────────────┐
│ "What time does Gold data │ ───► │ "Pipeline completes by      │───► │ "Curated Gold published & Power   │
│ publish to the warehouse?"│      │ 4:00 AM EST (99% of runs)." │     │ BI refreshed by 6:00 AM EST daily"│
└───────────────────────────┘      └─────────────────────────────┘     └───────────────────────────────────┘
```

### The Production Master Matrix:

| Concept | Definition | Exact Value / Implementation at Carelon |
| :--- | :--- | :--- |
| **SLA (Service Level Agreement)** | The formal, business-facing commitment. Breaches incur financial, operational, or legal penalties. | **Data Freshness / Availability: 6:00 AM EST Daily.**<br>*Penalty / Impact:* If Gold reporting is delayed past 6:00 AM, the treasury payment export file cannot be dispatched to banking clearinghouses by 7:30 AM, halting hospital disbursements and risking Prompt Pay interest penalties. |
| **SLO 1 (Pipeline Latency)** | Internal target to ensure the SLA is never threatened. | **Pipeline Completion by 4:00 AM EST (99% of runs).**<br>Leaves a **2-hour safety buffer** before the 6:00 AM SLA for automated retries or on-call triaging. |
| **SLO 2 (Financial Integrity)** | Internal data accuracy target. | **\$0.00 Unexplained Financial Variance.**<br>Every dollar from source landing must equal curated target dollars + quarantined dollars + deduplicated dollars. |
| **SLO 3 (Data Completeness)** | Internal quality threshold. | **Null Rate $<0.01\%$ on primary keys** (`claim_id`, `member_id`, `provider_id`) and **Quarantine Rate $<0.5\%$** of incoming daily records. |
| **SLI (Service Level Indicators)** | The exact runtime telemetry metrics measured continuously. | 1. **Freshness SLI:** `MAX(ingested_at)` timestamp and Gold publication completion timestamp.<br>2. **Execution SLI:** Databricks Workflow end-to-end run duration (minutes).<br>3. **Quality SLI:** `quarantine_rate` and `reconciliation_variance_dollars`. |

---

## 5. Why the "8 Hours to 27 Minutes" Optimization Was a Business Imperative

When an interviewer asks: *"Why did it matter that the job took 8 hours? Wasn't it just a nightly batch?"*

> **The Reality:**  
> *"When the pipeline took 8 hours, it started at 10:00 PM and often didn't finish until 6:00 AM or 6:30 AM—right on the edge of our SLA. 
>
> If a cloud node failed, a shuffle spilled excessively, or a late-arriving clearinghouse batch triggered a rerun, the pipeline blew right past the 6:00 AM SLA. Operations and Treasury would log in at 8:30 AM to find stale data, delayed disbursement files, and broken Power BI reports.
>
> By optimizing the pipeline down to **27 minutes**, we shifted our start time to **2:30 AM**. The entire pipeline (Ingestion $\to$ Contract Validation $\to$ Silver Merge $\to$ Gold Aggregation $\to$ Reconciliation $\to$ DQ Gatekeeper) finishes by **3:00 AM**. 
>
> That converted a precarious 0-minute margin into a **3-hour engineering recovery window**. If a catastrophic failure occurred, our automated alerts fired by 3:15 AM, giving engineering ample time to fix and replay before any business user logged in at 8:30 AM."*

---

## 6. Word-for-Word Interview Script (Master Delivery)

```text
INTERVIEWER:
"Can you walk me through the business impact of your claims pipeline? 
What were you building, what were your SLAs, and why was this important to the business?"

CANDIDATE:
"At Carelon, we were building the core financial claims lakehouse, responsible for 
processing daily clearinghouse submissions to calculate our $25M to $35M in monthly 
provider reimbursements.

Our business-critical SLA was data freshness: our Gold tables and downstream Power BI 
models had to be fully certified and published by 6:00 AM EST every morning. In health 
insurance, missing that SLA isn't just an internal annoyance—under state Prompt Pay laws, 
delays in generating payment files expose the plan to statutory interest penalties of 
10% to 12% on delayed provider claims.

To guarantee that 6:00 AM SLA, our internal engineering SLO was to have all processing 
and reconciliation completed by 4:00 AM, leaving a 2-hour buffer. 

When the pipeline took 8 hours, any data skew or retries pushed us past 6:00 AM. 
By optimizing the run to 27 minutes using incremental watermarks, Z-ORDER, and AQE, 
our run started at 2:30 AM and finished by ~3:00 AM, giving us a rock-solid 3-hour buffer.

From a business standpoint, the ultimate outcome was financial integrity: our 3-way 
reconciliation ledger balanced landing dollars against curated and quarantined records 
with zero unexplained variance, and our grain-isolated models eliminated Cartesian 
fan-out bugs that had previously created $40M+ phantom cash inflation in preliminary 
treasury forecasts."
```
