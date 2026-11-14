"""Incremental data processing script.

Converted from the original Jupyter Notebook to a PEP 8 formatted Python file.
The original processing logic and business rules are preserved.
"""

import os
import datetime
import polars as pl

# -----------------------------
# Paths
# -----------------------------
input_folder  = r"C:\Users\IshitaSharma\OneDrive - COMSENSE TECHNOLOGIES PRIVATE LIMITED,\Desktop\RE_IndividualLOB_RFM\ind_rfm_op"
prev_rfm_path = r"C:\Users\IshitaSharma\OneDrive - COMSENSE TECHNOLOGIES PRIVATE LIMITED,\Desktop\RE_IndividualLOB_RFM\data\rfm_cltv_2026_07_31.csv"
output_path   = r"C:\Users\IshitaSharma\OneDrive - COMSENSE TECHNOLOGIES PRIVATE LIMITED,\Desktop\RE_IndividualLOB_RFM\ind_rfm_op\rfm_2026_08_31.csv"

DAYS_ELAPSED = 30

LOB_MAP = {
    "invoice"    : "invoice",
    "service"    : "service",
    "gma"        : "gma",
    "apstore"    : "apparelstore",
    "aponline"   : "apparelonline",
    "softproduct": "softproduct"
}
lobs = list(LOB_MAP.values())

# ================================================================
# HELPER FUNCTIONS
# ================================================================
# Define the reusable processing function.
def cast_dates_to_string(df: pl.DataFrame) -> pl.DataFrame:
    cast_exprs = [
        pl.col(col_name).cast(pl.String)
        for col_name, dtype in zip(df.columns, df.dtypes)
        if dtype in (pl.Date, pl.Datetime)
    ]
# Apply the required column transformations and data preparation steps.
    return df.with_columns(cast_exprs) if cast_exprs else df


# Define the reusable processing function.
def existing(cols, df: pl.DataFrame) -> list:
    return [c for c in cols if c in df.columns]


# ================================================================
# STEP 1: Read & Process New Transaction Files
# ================================================================
files = os.listdir(input_folder)
dfs   = []

for file in files:
    if not file.endswith(".csv"):
        continue

    raw_flag  = os.path.splitext(file)[0].split("_")[0].lower()
    flag      = LOB_MAP.get(raw_flag, raw_flag)
# Join the datasets using the defined customer-level matching logic.
    file_path = os.path.join(input_folder, file)

# Load the input data required for incremental processing.
    df = pl.read_csv(
        file_path,
        schema_overrides={
            "mobile_number"      : pl.String,
            "first_purchase_date": pl.String,
            "last_purchase_date" : pl.String,
            "Recency"            : pl.Int64,
            "Frequency"          : pl.Int64,
            "Monetary"           : pl.Float64,
            "Segment"            : pl.String,
        },
        ignore_errors=True,
    )

    df = df.rename({
        "mobile_number"      : f"mobile_number_{flag}",
        "first_purchase_date": f"first_purchase_date_{flag}",
        "last_purchase_date" : f"last_purchase_date_{flag}",
        "Recency"            : f"Recency_{flag}",
        "Frequency"          : f"Frequency_{flag}",
        "Monetary"           : f"Monetary_{flag}",
        "Segment"            : f"Segment_{flag}",
    })

    # Strip whitespace from mobile number
# Apply the required column transformations and data preparation steps.
    df = df.with_columns(
        pl.col(f"mobile_number_{flag}")
          .cast(pl.String)
          .str.strip_chars()
          .alias("Mobile_Number")
    )

    dfs.append(df)

# ── Combine all LOB files → one row per customer ─────────────
new_txn_raw = pl.concat(dfs, how="diagonal_relaxed")

agg_exprs = []
for col_name, dtype in zip(new_txn_raw.columns, new_txn_raw.dtypes):
    if col_name == "Mobile_Number":
        continue
    if col_name.startswith("mobile_number_"):
        agg_exprs.append(
            pl.col(col_name).drop_nulls().first().alias(col_name)
        )
    elif dtype in (pl.Int64, pl.Int32, pl.Float64, pl.Float32):
        if "Recency" in col_name:
            agg_exprs.append(pl.col(col_name).min().alias(col_name))
        elif "Frequency" in col_name or "Monetary" in col_name:
            agg_exprs.append(
                # Only sum if at least one non-null exists, else keep null
                pl.when(pl.col(col_name).drop_nulls().len() > 0)
                .then(pl.col(col_name).sum())
                .otherwise(None)
                .alias(col_name)
            )
        else:
            agg_exprs.append(pl.col(col_name).max().alias(col_name))
    else:
        agg_exprs.append(
            pl.col(col_name).drop_nulls().first().alias(col_name)
        )

# Aggregate records at the required customer level.
new_txn_df = new_txn_raw.group_by("Mobile_Number").agg(agg_exprs)

# Strip mobile in new_txn_df as well
# Apply the required column transformations and data preparation steps.
new_txn_df = new_txn_df.with_columns(
    pl.col("Mobile_Number").str.strip_chars()
)

print(f"🆕 New transaction rows  : {len(new_txn_df):,}")

# ================================================================
# STEP 2: Load Full Previous RFM
# ================================================================
# Load the input data required for incremental processing.
prev_df = pl.read_csv(
    prev_rfm_path,
    schema_overrides={
        "Mobile_Number"     : pl.String,
        "Recency"           : pl.Int64,
        "Frequency"         : pl.Int64,
        "Monetary"          : pl.Float64,
        "Segment"           : pl.String,
        "rfm_combined_score": pl.String,
        "recency_score"     : pl.Int64,
        "frequency_score"   : pl.Int64,
        "monetary_score"    : pl.Int64,
        "cltv"              : pl.Float64,
    },
    ignore_errors=True,
)
prev_df = cast_dates_to_string(prev_df)

# Strip mobile in prev_df
# Apply the required column transformations and data preparation steps.
prev_df = prev_df.with_columns(
    pl.col("Mobile_Number").cast(pl.String).str.strip_chars()
)

# Validate
required_prev = {"Mobile_Number", "Recency", "Frequency", "Monetary"}
missing = required_prev - set(prev_df.columns)
if missing:
    raise ValueError(f"prev_df is missing columns: {missing}")

print(f"📦 Previous RFM rows     : {len(prev_df):,}")

# ================================================================
# STEP 3: Strip prev_df to ONLY core columns before join
#         ← KEY FIX: prev_df has old LOB columns which clash
#            with new_txn_df LOB columns during join
# ================================================================

# Columns to keep from prev_df — core RFM only, NO old LOB columns
# Rename old LOB columns with _prev suffix to avoid collision
prev_lob_cols = [c for c in prev_df.columns if c not in [
    "Recency", "Frequency", "Monetary", "Segment",
    "rfm_combined_score", "recency_score", "frequency_score",
    "monetary_score", "cltv", "updated_at", "has_transaction", "total_profit"
] and c != "Mobile_Number"]

rename_map = {c: f"{c}_prev" for c in prev_lob_cols}

prev_df_core = prev_df.rename(rename_map)

# Also rename core RFM to avoid collision with new_txn_df
prev_df_core = prev_df_core.rename({
    "Recency"  : "prev_Recency",
    "Frequency": "prev_Frequency",
    "Monetary" : "prev_Monetary",
    "Segment"  : "prev_Segment",
})

# ================================================================
# STEP 4: Left Join — ALL prev customers kept
# ================================================================
merged_df = prev_df_core.join(
    new_txn_df,
    on="Mobile_Number",
    how="left"
)

print(f"📊 Merged rows           : {len(merged_df):,}")

# ── Verify no _right columns exist after join ─────────────────
right_cols = [c for c in merged_df.columns if c.endswith("_right")]
if right_cols:
    print(f"⚠️  WARNING: _right columns still exist: {right_cols}")
    print(f"   This means LOB columns still overlap — dropping _right cols")
    merged_df = merged_df.drop(right_cols)
else:
    print(f"✅ No _right columns — join is clean")

# ================================================================
# STEP 5: Find Brand NEW customers
# ================================================================
# Join the datasets using the defined customer-level matching logic.
new_customers_df = new_txn_df.join(
    prev_df_core.select("Mobile_Number"),
    on="Mobile_Number",
    how="anti"
)
print(f"✨ Brand new customers   : {len(new_customers_df):,}")

# ================================================================
# STEP 6: Collect THIS MONTH'S LOB columns from merged_df
#         These now come exclusively from new_txn_df (no clash)
# ================================================================
recency_cols_m   = existing([f"Recency_{lob}"   for lob in lobs], merged_df)
frequency_cols_m = existing([f"Frequency_{lob}" for lob in lobs], merged_df)
monetary_cols_m  = existing([f"Monetary_{lob}"  for lob in lobs], merged_df)

# print(f"\n📌 LOB Recency  cols : {recency_cols_m}")
# print(f"📌 LOB Frequency cols: {frequency_cols_m}")
# print(f"📌 LOB Monetary  cols: {monetary_cols_m}")

# ================================================================
# STEP 7: Flag who transacted this month
# ================================================================
if frequency_cols_m:
# Apply the required column transformations and data preparation steps.
    merged_df = merged_df.with_columns(
        pl.any_horizontal(
            # Only flag if frequency > 0 (not just non-null)
            [pl.col(c).is_not_null() & (pl.col(c) > 0)
             for c in frequency_cols_m]
        ).cast(pl.Int8).alias("has_transaction")
    )
else:
# Apply the required column transformations and data preparation steps.
    merged_df = merged_df.with_columns(
        pl.lit(0).cast(pl.Int8).alias("has_transaction")
    )

# ================================================================
# STEP 8: Cross-LOB aggregates from THIS MONTH only
# ================================================================

# Recency → min across LOBs (most recent = smallest days)
# Only consider LOBs where there was actual transaction (freq > 0)
if recency_cols_m:
# Apply the required column transformations and data preparation steps.
    merged_df = merged_df.with_columns(
        pl.min_horizontal([
            pl.when(
                pl.col(f"Frequency_{lob}").is_not_null() &
                (pl.col(f"Frequency_{lob}") > 0)
            )
            .then(pl.col(f"Recency_{lob}"))
            .otherwise(None)
            for lob in lobs
            if f"Recency_{lob}" in merged_df.columns
              and f"Frequency_{lob}" in merged_df.columns
        ]).alias("Recency_lob")
    )
else:
# Apply the required column transformations and data preparation steps.
    merged_df = merged_df.with_columns(
        pl.lit(None).cast(pl.Int64).alias("Recency_lob")
    )

# Frequency → sum across LOBs (fill null as 0)
if frequency_cols_m:
# Apply the required column transformations and data preparation steps.
    merged_df = merged_df.with_columns(
        pl.sum_horizontal(
            [pl.col(c).fill_null(0) for c in frequency_cols_m]
        ).alias("Frequency_lob")
    )
else:
# Apply the required column transformations and data preparation steps.
    merged_df = merged_df.with_columns(
        pl.lit(0).cast(pl.Int64).alias("Frequency_lob")
    )

# Monetary → sum across LOBs (fill null as 0)
if monetary_cols_m:
# Apply the required column transformations and data preparation steps.
    merged_df = merged_df.with_columns(
        pl.sum_horizontal(
            [pl.col(c).fill_null(0) for c in monetary_cols_m]
        ).alias("Monetary_lob")
    )
else:
# Apply the required column transformations and data preparation steps.
    merged_df = merged_df.with_columns(
        pl.lit(0.0).cast(pl.Float64).alias("Monetary_lob")
    )

# ================================================================
# STEP 9: Incremental RFM Logic
# ================================================================
# Apply the required column transformations and data preparation steps.
merged_df = merged_df.with_columns([

    # Recency: fresh LOB value if transacted, else age by DAYS_ELAPSED
    pl.when(pl.col("has_transaction") == 1)
      .then(pl.col("Recency_lob"))
      .otherwise(pl.col("prev_Recency") + DAYS_ELAPSED)
      .alias("Recency_final"),

    # Frequency: cumulative (prev + this month)
    pl.when(pl.col("has_transaction") == 1)
      .then(pl.col("prev_Frequency") + pl.col("Frequency_lob"))
      .otherwise(pl.col("prev_Frequency"))
      .alias("Frequency_final"),

    # Monetary: cumulative (prev + this month)
    pl.when(pl.col("has_transaction") == 1)
      .then(pl.col("prev_Monetary") + pl.col("Monetary_lob"))
      .otherwise(pl.col("prev_Monetary"))
      .alias("Monetary_final"),
])


# ================================================================
# STEP 9b: Coalesce LOB columns
#   - If customer transacted this month → use new value from new_txn_df
#   - If customer did NOT transact      → restore old value from _prev
# ================================================================
lob_coalesce_exprs = []

for lob in lobs:
    for prefix in ["mobile_number", "last_purchase_date",
                   "Recency", "Frequency", "Monetary", "Segment"]:
        new_col  = f"{prefix}_{lob}"
        prev_col = f"{prefix}_{lob}_prev"
        freq_col = f"Frequency_{lob}"  # use THIS LOB's frequency as the flag

        new_exists  = new_col  in merged_df.columns
        prev_exists = prev_col in merged_df.columns
        freq_exists = freq_col in merged_df.columns

        if new_exists and prev_exists:
            if freq_exists:
                # Only update if THIS specific LOB had a transaction
                lob_coalesce_exprs.append(
                    pl.when(
                        pl.col(freq_col).is_not_null() & (pl.col(freq_col) > 0)
                    )
                    .then(pl.col(new_col))
                    .otherwise(pl.col(prev_col))
                    .alias(new_col)
                )
            else:
                lob_coalesce_exprs.append(
                    pl.col(prev_col).alias(new_col)
                )
        elif prev_exists:
            lob_coalesce_exprs.append(
                pl.col(prev_col).alias(new_col)
            )
        elif new_exists:
            lob_coalesce_exprs.append(pl.col(new_col))

if lob_coalesce_exprs:
# Apply the required column transformations and data preparation steps.
    merged_df = merged_df.with_columns(lob_coalesce_exprs)

# Drop all _prev LOB columns — no longer needed
prev_lob_final_cols = [c for c in merged_df.columns if c.endswith("_prev")]
merged_df = merged_df.drop(prev_lob_final_cols)


# ================================================================
# STEP 10: Handle Brand New Customers
# ================================================================
if len(new_customers_df) > 0:

    recency_cols_nc   = existing([f"Recency_{lob}"   for lob in lobs], new_customers_df)
    frequency_cols_nc = existing([f"Frequency_{lob}" for lob in lobs], new_customers_df)
    monetary_cols_nc  = existing([f"Monetary_{lob}"  for lob in lobs], new_customers_df)

# Apply the required column transformations and data preparation steps.
    new_customers_df = new_customers_df.with_columns([

        pl.min_horizontal(recency_cols_nc).alias("Recency_final")
        if recency_cols_nc
        else pl.lit(None).cast(pl.Int64).alias("Recency_final"),

        pl.sum_horizontal(
            [pl.col(c).fill_null(0) for c in frequency_cols_nc]
        ).alias("Frequency_final")
        if frequency_cols_nc
        else pl.lit(0).cast(pl.Int64).alias("Frequency_final"),

        pl.sum_horizontal(
            [pl.col(c).fill_null(0) for c in monetary_cols_nc]
        ).alias("Monetary_final")
        if monetary_cols_nc
        else pl.lit(0.0).cast(pl.Float64).alias("Monetary_final"),

        pl.lit(1).cast(pl.Int8).alias("has_transaction"),
    ])

# ================================================================
# STEP 11: Select final columns & stack frames
# ================================================================
lob_detail_cols = (
    [f"mobile_number_{lob}"       for lob in lobs] +
    [f"first_purchase_date_{lob}" for lob in lobs] +
    [f"last_purchase_date_{lob}"  for lob in lobs] +
    [f"Recency_{lob}"             for lob in lobs] +
    [f"Frequency_{lob}"           for lob in lobs] +
    [f"Monetary_{lob}"            for lob in lobs] +
    [f"Segment_{lob}"             for lob in lobs]
)

core_cols = [
    "Mobile_Number",
    "Recency_final",
    "Frequency_final",
    "Monetary_final",
    "has_transaction",
]

keep_cols_m  = core_cols + existing(lob_detail_cols, merged_df)
keep_cols_nc = core_cols + existing(lob_detail_cols, new_customers_df) \
               if len(new_customers_df) > 0 else core_cols

frames = [cast_dates_to_string(merged_df.select(keep_cols_m))]

if len(new_customers_df) > 0:
    frames.append(cast_dates_to_string(new_customers_df.select(keep_cols_nc)))

full_df = pl.concat(frames, how="diagonal_relaxed")

print(f"\n📊 Total rows in full RFM: {len(full_df):,}")

# ================================================================
# STEP 12: Rename finals → canonical names
# ================================================================
full_df = full_df.rename({
    "Recency_final"  : "Recency",
    "Frequency_final": "Frequency",
    "Monetary_final" : "Monetary",
})

# ================================================================
# STEP 13: Quantile Scoring
# ================================================================
r1 = full_df["Recency"].drop_nulls().quantile(0.33)
r2 = full_df["Recency"].drop_nulls().quantile(0.66)
f1 = full_df["Frequency"].drop_nulls().quantile(0.33)
f2 = full_df["Frequency"].drop_nulls().quantile(0.66)
m1 = full_df["Monetary"].drop_nulls().quantile(0.33)
m2 = full_df["Monetary"].drop_nulls().quantile(0.66)

print(f"\n📐 Quantile Thresholds:")
print(f"   Recency   : ≤{r1:.1f}  |  ≤{r2:.1f}")
print(f"   Frequency : ≤{f1:.1f}  |  ≤{f2:.1f}")
print(f"   Monetary  : ≤{m1:.1f}  |  ≤{m2:.1f}")

# Apply the required column transformations and data preparation steps.
full_df = full_df.with_columns([

    pl.when(pl.col("Recency").is_null()).then(None)
      .when(pl.col("Recency") <= r1).then(3)
      .when(pl.col("Recency") <= r2).then(2)
      .otherwise(1)
      .alias("recency_score"),

    pl.when(pl.col("Frequency").is_null()).then(None)
      .when(pl.col("Frequency") <= f1).then(1)
      .when(pl.col("Frequency") <= f2).then(2)
      .otherwise(3)
      .alias("frequency_score"),

    pl.when(pl.col("Monetary").is_null()).then(None)
      .when(pl.col("Monetary") <= m1).then(1)
      .when(pl.col("Monetary") <= m2).then(2)
      .otherwise(3)
      .alias("monetary_score"),
])

# Apply the required column transformations and data preparation steps.
full_df = full_df.with_columns(
    (
        pl.col("recency_score").cast(pl.String) +
        pl.col("frequency_score").cast(pl.String) +
        pl.col("monetary_score").cast(pl.String)
    ).alias("rfm_combined_score")
)

# ================================================================
# STEP 14: Segmentation
# ================================================================
# Apply the required column transformations and data preparation steps.
full_df = full_df.with_columns(
    pl.when(pl.col("rfm_combined_score").is_in(["111", "112"]))
        .then(pl.lit("Churn"))
    .when(pl.col("rfm_combined_score").is_in(
        ["113","122","131","123","132","133","213","223"]))
        .then(pl.lit("Good but Gone"))
    .when(pl.col("rfm_combined_score").is_in(
        ["121","211","212","221","222"]))
        .then(pl.lit("Hibernating"))
    .when(pl.col("rfm_combined_score").is_in(
        ["231","232","233","331","322","323","321"]))
        .then(pl.lit("Potential Loyalist"))
    .when(pl.col("rfm_combined_score").is_in(["311","312","313"]))
        .then(pl.lit("Recent Customers"))
    .when(pl.col("rfm_combined_score") == "332")
        .then(pl.lit("Loyalist"))
    .when(pl.col("rfm_combined_score") == "333")
        .then(pl.lit("Champion"))
    .alias("Segment")
)

# ================================================================
# STEP 15: CLTV
# ================================================================
# Apply the required column transformations and data preparation steps.
full_df = full_df.with_columns(
    (pl.col("Monetary") * 0.20).alias("total_profit")
).with_columns(
    (pl.col("total_profit") * ((1 - (1 + 0.12) ** -6) / 0.12))
    .round(2)
    .alias("cltv")
)

# ================================================================
# STEP 16: Timestamp & Save
# ================================================================
# Apply the required column transformations and data preparation steps.
full_df = full_df.with_columns(
    pl.lit(str(datetime.datetime.now().date())).alias("updated_at")
)


# ================================================================
# STEP 17: Summary Report
# ================================================================
print(f"\n✅ Final RFM saved → {output_path}")
print(f"   Total customers  : {len(full_df):,}")
print(f"   Transacted       : {full_df['has_transaction'].sum():,}")
print(f"   Not transacted   : {(len(full_df) - full_df['has_transaction'].sum()):,}")

segment_summary = (
# Aggregate records at the required customer level.
    full_df.group_by("Segment")
    .agg([
        pl.len().alias("customer_count"),
        pl.col("Monetary").mean().round(2).alias("avg_monetary"),
        pl.col("Recency").mean().round(1).alias("avg_recency"),
        pl.col("cltv").mean().round(2).alias("avg_cltv"),
    ])
    .sort("customer_count", descending=True)
)
print("\n📊 Segment Summary:")
print(segment_summary)


# Export the final processed dataset to CSV.
full_df.write_csv(r"C:\Users\IshitaSharma\OneDrive - COMSENSE TECHNOLOGIES PRIVATE LIMITED,\Desktop\RE_IndividualLOB_RFM\final_Output\rfm_cltv_data_2026_08_31.csv")



