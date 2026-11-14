"""Imports"""

import polars as pl
import datetime
current_date = datetime.datetime.now()

# Load the source data for processing.
service_data = pl.read_csv(r"C:\Users\IshitaSharma\OneDrive - COMSENSE TECHNOLOGIES PRIVATE LIMITED,\Desktop\RE_IndividualLOB_RFM\data\service_svoc_20260830.csv", ignore_errors=True)

service_data

# Create or transform columns required for downstream processing.
df = service_data.with_columns([
# Convert the relevant column to the required date format.
    pl.col("first_purchase_date").str.to_date(format="%Y-%m-%d").alias("first_purchase_date"),
# Convert the relevant column to the required date format.
    pl.col("last_purchase_date").str.to_date(format="%Y-%m-%d").alias("last_purchase_date")
])

# Define reference date as current date
reference_date = pl.lit(datetime.datetime.now().date()).cast(pl.Date)

# Compute RFM metrics
# Compute RFM metrics
df_rfm = df.select([
    pl.col("mobile_number"),
    pl.col("first_purchase_date"),
    pl.col("last_purchase_date"),
    (reference_date - pl.col("last_purchase_date")).dt.total_days().alias("Recency"),
    pl.col("total_transactions").alias("Frequency"),
    pl.col("total_amount_spent").alias("Monetary")
# Aggregate records at the required grouping level.
]).group_by("mobile_number").agg([
    pl.col("first_purchase_date").min(),
    pl.col("last_purchase_date").max(),
    pl.col("Recency").min(),
    pl.col("Frequency").sum(),
    pl.col("Monetary").sum()
])

df_rfm

# Define a reusable function for the processing step.
def add_rfm_calc(
    df: pl.DataFrame | pl.LazyFrame, n_segments: int = 3
) -> pl.DataFrame | pl.LazyFrame:
    return (
        df
# Sort the records for consistent output.
        .sort(
            "Frequency", "Monetary", "Recency", descending=[False, False, True]
        )
# Create or transform columns required for downstream processing.
        .with_columns(
            pl.col("Recency")
            .qcut(
                quantiles=[n / n_segments for n in range(1, n_segments)],
                labels=[str(n) for n in range(n_segments, 0, -1)],
            )
            .fill_null("1")
            .cast(pl.String)
            .cast(pl.UInt8)
            .alias("recency_score"),

            pl.col("Frequency")
            .qcut(
                quantiles=[n / n_segments for n in range(1, n_segments)],
                labels=[str(n) for n in range(1, n_segments + 1)],
                allow_duplicates=True,
            )
            .fill_null("1")
            .cast(pl.String)
            .cast(pl.UInt8)
            .alias("frequency_score"),

            pl.col("Monetary")
            .qcut(
                quantiles=[n / n_segments for n in range(1, n_segments)],
                labels=[str(n) for n in range(1, n_segments + 1)],
                allow_duplicates=True,
            )
            .fill_null("1")
            .cast(pl.String)
            .cast(pl.UInt8)
            .alias("monetary_score"),
        )
# Create or transform columns required for downstream processing.
        .with_columns(
            (
                pl.col("recency_score").cast(pl.String)
                + pl.col("frequency_score").cast(pl.String)
                + pl.col("monetary_score").cast(pl.String)
            ).alias("rfm_combined_score")
        )
# Create or transform columns required for downstream processing.
        .with_columns(
            pl.when(
                (pl.col("Recency") <= 30) & (pl.col("Frequency") == 1)
            )
            .then(pl.lit("Recent Customers"))
            .otherwise(
                pl.when(pl.col("rfm_combined_score").is_in(["111", "112"]))
                .then(pl.lit("Churn"))
                .when(pl.col("rfm_combined_score").is_in(["113", "122", "131", "123", "132", "133", "213", "223"]))
                .then(pl.lit("Good but Gone"))
                .when(pl.col("rfm_combined_score").is_in(["121", "211", "212", "221", "222"]))
                .then(pl.lit("Hibernating"))
                .when(pl.col("rfm_combined_score").is_in(["231", "232", "233", "331", "322", "323", "321"]))
                .then(pl.lit("Potential Loyalist"))
                # .when(pl.col("rfm_combined_score").is_in(["311", "312", "313"]))
                # .then(pl.lit("Recent Customers"))
                .when(pl.col("rfm_combined_score").is_in(["332"]))
                .then(pl.lit("Loyalist"))
                .when(pl.col("rfm_combined_score").is_in(["333"]))
                .then(pl.lit("Champion"))
                .otherwise(pl.lit("Potential Loyalist"))
            )
            .alias("Segment")
        )
# Remove duplicate records where required.
        .unique(subset=["mobile_number"])
    )

service_rfm = df_rfm.pipe(add_rfm_calc)

service_final_rfm = service_rfm.select(pl.col("mobile_number"),
    pl.col("first_purchase_date"),
    pl.col("last_purchase_date"),
    pl.col("Recency"),
    pl.col("Frequency"),
    pl.col("Monetary"),
    pl.col("Segment"))
service_final_rfm

# Write the processed data to the output CSV.
service_final_rfm.write_csv(r"C:\Users\IshitaSharma\OneDrive - COMSENSE TECHNOLOGIES PRIVATE LIMITED,\Desktop\RE_IndividualLOB_RFM\ind_rfm_op\service_rfm_output_2026-08.csv")
