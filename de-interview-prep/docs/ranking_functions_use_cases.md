# ROW_NUMBER, RANK & DENSE_RANK Use Cases

## 1. ROW_NUMBER(): Deduplication & SCD Type 1 Upserts

### Why ROW_NUMBER()?
Raw/Bronze tables often land multiple updates or duplicate records per primary key.  
`ROW_NUMBER()` is the **only** function that guarantees **strictly 1 row per key** because it never produces ties.

> **Why not `RANK()`?** If two duplicates arrive with the exact same `updated_at`, `RANK()` gives both `1`, letting duplicate records leak into Silver/Gold and breaking table grain.

### Pattern: Deduplication (Latest Record Wins)
```sql
-- Silver staging / clean query:
SELECT *
FROM bronze_claims
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY claim_id
    ORDER BY updated_at DESC, _ingest_batch DESC
) = 1;
```

### Pattern: SCD Type 1 MERGE (Upsert)
Before running a Delta `MERGE`, you must deduplicate the source batch so a single key does not trigger multiple conflicting updates:

```sql
MERGE INTO silver.claims AS target
USING (
    SELECT *
    FROM raw_batch
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY claim_id
        ORDER BY updated_at DESC
    ) = 1
) AS source
ON target.claim_id = source.claim_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

---

## 2. RANK(): Competition Leaderboards & Grant Allocation

### Behavior
Ties share rank, subsequent numbers **skip** (`1, 2, 2, 4`).

### Core Use Case: Relative Position Counts
Use when rank must reflect **how many entities performed better than you**.

* **Example:** Hospital performance grants for the "Top 5" providers.
  * If two providers tie for Rank 1, the next provider is **Rank 3** (because 2 providers scored better than them).
  * Awarding Rank 2 to the third provider would incorrectly imply only 1 person beat them.

---

## 3. DENSE_RANK(): Nth-Highest Values & Price Tiers

### Behavior
Ties share rank, subsequent numbers **never skip** (`1, 2, 2, 3`).

### Core Use Case: N-th Distinct Value per Entity
Whenever you need the **"2nd highest"** or **"3rd distinct tier"**, `RANK()` breaks if there is a tie at the top:

```text
Member claims: [$500, $500, $300]

RANK():       [1, 1, 3]  --> Searching for rank = 2 returns NOTHING (Bug!)
DENSE_RANK(): [1, 1, 2]  --> Searching for dense_rank = 2 correctly returns $300
```

### Pattern: 2nd Highest Claim per Member
```sql
SELECT member_id, claim_id, paid_amount
FROM claims
QUALIFY DENSE_RANK() OVER (
    PARTITION BY member_id
    ORDER BY paid_amount DESC
) = 2;
```

---

## Quick Decision Matrix

| Goal | Function | Why |
| :--- | :--- | :--- |
| **Deduplication / SCD1** | `ROW_NUMBER()` | Guarantees strictly 1 row per PK, eliminating duplicates |
| **Find N-th highest distinct value** | `DENSE_RANK()` | Never skips rank numbers when higher tiers tie |
| **Leaderboard / Top slots** | `RANK()` | Accurately accounts for total entities ahead of each row |
