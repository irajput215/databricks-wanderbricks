# Ishu Rajput: Interview Stories

One story per resume point, each about 1 to 1.5 minutes spoken. Built only from your resume. Anything in \[FILL: ...\] is a detail only you know. Put in the true detail, or delete that sentence. Never say a number you can't back up.

Format of every story: Situation, what I did, result. Say it out loud once tonight.

## 0. Tell me about yourself (30 seconds)

I'm a data engineer with two years of experience in healthcare claims at Carelon, working on Azure Databricks, Spark and Delta Lake. I built incremental, reconciled pipelines over a 3 TB claims dataset, and I cared a lot about data quality and governance. I then did my Master of IT at UNSW in Sydney, finishing this year, where I built two projects: an end-to-end Snowflake and dbt platform, and an agent that investigates failed data pipelines. I have full working rights in Australia, and I'm looking for a data engineering role where I can build reliable pipelines.

## 0b. The gap (April 2023 to September 2024)

&#91;FILL: one true, positive sentence. For example: I took time to prepare for my Master's at UNSW, \[study / upskilling / family / work you did\]. Say it calmly, then pivot to UNSW and the projects.\]

---

# Carelon: Associate Data Engineer III (June 2022 to April 2023)

## 1. Cutting batch runtime from 8 hours to 27 minutes

**Situation.** We had a batch job over a 3 TB healthcare claims dataset on Azure Databricks that took about 8 hours, which was too slow for downstream reporting.

**What I did.** I looked at the Spark UI to find where time was going. I partitioned the data and applied Z-ORDER on service date and provider id, because those were the main filter columns, so queries could skip files. I used broadcast joins for the small dimension tables, which removed shuffles for those joins, and I enabled AQE so Spark could adjust partitions and handle skew at runtime. I also added idempotent incremental loads, so each run processed new data instead of everything. \[FILL: which change gave the biggest gain, if you know.\]

**Result.** Runtime went from 8 hours to 27 minutes, a 94% reduction.

**If probed.** Broadcast joins avoid shuffles for those joins. AQE reduces and rebalances shuffles but doesn't eliminate them. Say "reduced".

## 2. Grain-isolated aggregation models

**Situation.** Claims data has headers and service lines. A header carries the paid amount, and one header links to many lines. Reports that joined them and then summed header amounts counted the same payment several times.

**What I did.** I built aggregation models that keep the two grains separate. Header metrics are calculated at header level, and line metrics at line level. If a report needs both, I aggregate the lines up to the claim level first and then join.

**Result.** Reporting on more than $25M in monthly provider disbursements stopped being exposed to this multi-counting bug.

**Example to give.** A claim with a $5,000 payment and 4 lines becomes $20,000 if you join first and sum. \[FILL: a real example you saw, if any.\]

## 3. 3-way financial reconciliation and quarantine

**Situation.** We processed more than 1M claim records daily, and finance needed every dollar accounted for between source and target.

**What I did.** I built an automated reconciliation ledger comparing three views of the data: \[FILL: your three, for example source, validated, and curated target\]. Records that failed schema or primary-key checks went to a quarantine table instead of breaking the load, with a reason attached.

**Result.** The ledger traced a $10,000 variance back to its source: \[FILL: the real cause, in one sentence\]. Across the batches we isolated more than 2,800 schema and primary-key violations for review instead of letting them silently flow downstream.

**If probed.** Know what the three ledgers were, and the timeframe of the 2,800.

## 4. Idempotent retries

**Situation.** If a pipeline fails halfway through, a rerun must not duplicate payments.

**What I did.** I deduplicated each batch with a deterministic PySpark window function, ordering by a timestamp and a tie-breaker so the same record always wins. Then I wrote to Delta using MERGE INTO, so reruns update existing rows instead of inserting copies. With Databricks Workflows, I used Repair-and-Rerun to re-run only the failed and downstream tasks.

**Result.** We could recover from mid-flight failures without duplicating payments.

**If probed.** MERGE fails if multiple source rows match one target row. That's why the dedup comes first. A mid-write failure leaves the Delta table at its last committed version because commits are atomic.

## 5. Pipeline observability

**Situation.** Data problems like null spikes or schema changes were often noticed late, by report users.

**What I did.** I set up monitoring on 9 core metrics, including null spikes, duplicate ratios, data lag against SLA, schema drift, and quarantine rate, with alerts when they crossed thresholds.

**Result.** Mean time to detect incidents dropped by 75%. \[FILL: baseline and new time, for example from about X hours to Y minutes, and how you measured it. If you don't remember the exact numbers, say: the exact figures were roughly this, and we measured from job completion to alert or ticket.\]

## 6. Governance with Unity Catalog

**Situation.** HIPAA-sensitive fields like SSN and date of birth needed protection, we had multi-tenant provider data, and the team had built many duplicated SQL views for access control.

**What I did.** I moved access control into Unity Catalog using grants and dynamic views. The views mask SSN and DOB depending on the user's group membership, and restrict rows so each provider tenant only sees their own data.

**Result.** One governed view replaced more than 40 duplicated SQL views, so there was less drift and less maintenance.

**If probed.** A dynamic view checks group membership, for example with `is_account_group_member()`, inside a CASE expression. However, the modern Unity Catalog approach is native **Row Filters** and **Dynamic Column Masks** attached directly to base tables, which cannot be bypassed and avoid view nesting overhead. *(See full deep-dive STAR answer in [star_interview_answers_data_governance.md](file:///m:/databricks/databricks-wanderbricks/de-interview-prep/docs/star_interview_answers_data_governance.md)).*

---

# Carelon: Associate Data Engineer II (Dec 2021 to May 2022)

## 7. Ingestion pipeline on ADLS Gen2

**Situation.** Claims ingestion was slow.

**What I did.** I built a PySpark pipeline on Azure Databricks reading from ADLS Gen2, organised as a medallion architecture. I made it incremental so it processed only new data, pruned unneeded columns early, and stored data as Parquet.

**Result.** Processing time dropped by 55%. \[FILL: before and after times if you remember them.\]

**If probed.** Parquet is columnar, so reading fewer columns means reading less data. Column pruning reduces both I/O and shuffle size.

## 8. Data quality with StructType and quarantine

**Situation.** Source files often had bad records, and clinical reviewers spent a lot of time checking them by hand.

**What I did.** I defined explicit PySpark StructType schemas instead of inferring them, so type problems were caught at the door. Invalid records were routed to quarantine tables with a reason, and clean records continued.

**Result.** Manual clinical review time fell by 35%, because reviewers got a focused list of bad records instead of digging through everything. \[FILL: how you measured the 35%.\]

## 9. Idempotent claims writes

**Situation.** Retries on recurring claims loads risked duplicating financial records.

**What I did.** I used partition-aware overwrites, which replace only the affected partitions, and MERGE operations for upserts, so running the same load twice gives the same result.

**Result.** Retries became safe and the recurring claims processing was more reliable.

## 10. Shared metric definitions

**Situation.** Different downstream reports calculated claim counts and paid amounts slightly differently, so the numbers didn't match.

**What I did.** I worked with business and data teams to agree on definitions for claim counts, paid amounts, and reconciliation exceptions. Then I implemented them once as shared, reusable metric logic.

**Result.** Every downstream report calculated those figures the same way. \[FILL: one concrete disagreement you resolved, for example what counts as a paid claim.\]

---

# Carelon: Associate Data Engineer I (June 2021 to Nov 2021)

## 11. SQL reconciliation controls

**Situation.** Loads sometimes dropped records and nobody noticed until later.

**What I did.** I wrote automated SQL checks comparing source and target tables on record counts and missing claim IDs.

**Result.** Load discrepancies were flagged for investigation right after the load, instead of being found downstream. This was my first step toward the later reconciliation ledger.

## 12. ICD code ingestion and search

**Situation.** Updating ICD code releases for downstream healthcare systems was manual.

**What I did.** I built a versioned workflow in Python using requests and BeautifulSoup to collect official code releases, stored on Azure Data Lake Storage, with a FastAPI service for search. Versioning meant we could keep each release and know which one downstream systems used.

**Result.** It automated collection and delivery and reduced manual data-update work. \[FILL: how often releases came and roughly how much time it saved.\]

---

# Projects

Be upfront: these are personal and academic projects, built end to end, not production systems.

## 13. Zomato AI Data Platform: the pipeline

**Situation.** I wanted a complete warehouse project using the modern stack, since my Carelon work was mostly Databricks.

**What I did.** I built a medallion pipeline from AWS S3 into Snowflake, with staging layers and Gold data marts over a dataset of more than 10M records, designed to serve analytical queries with low latency.

**Result.** A working end-to-end platform, which also taught me Snowflake. \[FILL: where the data came from and your Gold marts.\]

## 14. dbt incremental models and SCD Type 2

**What I did.** I built dbt incremental models using the merge strategy, so only new or changed rows are processed. For restaurant ratings I used SCD Type 2 snapshots, so each change creates a new row with valid-from and valid-to dates instead of overwriting history. dbt also generated lineage and documentation.

**Result.** Rating history is preserved and queryable, and the models are cheaper to run than full rebuilds.

**If probed.** Snapshot strategies: timestamp uses an updated\_at column and is preferred when reliable. Check compares column values and is the fallback. \[FILL: which you used and why.\]

## 15. Airflow, Docker and CI/CD

**What I did.** I scheduled the daily batch workflows with Airflow running in Docker, so the setup is reproducible. I added GitHub Actions CI/CD with a Makefile, so validation and deployment run the same steps locally and in the pipeline.

**Result.** Changes are validated automatically before they deploy. \[FILL: what the CI actually checks, for example dbt tests or linting.\]

## 16. Streamlit app: dashboards, RAG, Text-to-SQL

**What I did.** I built a self-service Streamlit app with operational KPI dashboards, search over reviews using RAG, and natural-language querying with LiteLLM, which turns a question into SQL over the warehouse marts.

**Result.** Non-technical users can explore the data without writing SQL.

**If probed.** Text-to-SQL runs only against the curated marts, and generated SQL should be validated before running, the same idea as the sqlglot check in my agent project.

## 17. Agentic incident-response platform: the agent

**Situation.** Debugging a failed pipeline means checking logs, upstream dependencies, and tables, which is slow and repetitive.

**What I did.** I built a LangGraph agent that investigates failed pipelines. I tested it on simulated failures, such as Spark crashes and missing files.

**Result.** On those simulated cases it identifies the root cause in under 20 seconds. I'm clear that the failures were simulated, so I treat this as a prototype showing the approach works.

## 18. The 9 diagnostic tools

**What I did.** I gave the agent 9 tools: inspecting error logs, tracing upstream pipeline dependencies, and querying warehouse tables. \[FILL: name 3 or 4 of the 9 from your code.\] I kept each tool narrow and single-purpose, so the agent's reasoning is easy to trace.

**Result.** The agent follows the same investigation steps an engineer would, but faster.

## 19. Safety: read-only access and human approval

**Situation.** An agent that can run SQL is risky, because a mistake or a bad generated query could change data.

**What I did.** I validate every query with sqlglot, which parses the SQL properly and blocks destructive statements. I chose parsing over keyword matching because keyword checks are easy to get wrong. Separately, any remediation requires human approval before it's applied.

**Result.** The agent can investigate freely but can't change anything on its own.

## 20. Observability with LangSmith

**What I did.** I instrumented the agent with LangSmith to trace step-by-step reasoning and monitor tool performance, latency, and LLM token costs.

**Result.** When the agent gave a wrong answer, I could see which step went wrong, and I could see where time and tokens were being spent. \[FILL: one real thing you found or fixed from the traces. If none, say what you would look for.\]

---

## Last-minute rules

- Say "roughly" when you don't remember an exact figure.
- For Snowflake, dbt, Airflow and AWS, say: project-level, built end to end, not production.
- Say "reduced shuffles", not "eliminated".
- If you don't know something, say how you would find out.

