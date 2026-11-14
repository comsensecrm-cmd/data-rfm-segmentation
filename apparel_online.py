# Import required libraries for date handling and Polars data processing.
import datetime

import polars as pl

# Capture the current timestamp for reference.
current_date = datetime.datetime.now()


# Load the source CSV containing the customer transaction data.
astore_data = pl.read_csv(
    r"C:\Users\IshitaSharma\OneDrive - COMSENSE TECHNOLOGIES PRIVATE LIMITED,\Desktop\RE_IndividualLOB_RFM\data\aponline_svoc_20260830.csv",
    ignore_errors=True,
)


# Convert purchase-date columns from strings to Polars Date values.
df = astore_data.with_columns(
    [
        pl.col("first_purchase_date")
        .str.to_date(format="%Y-%m-%d")
        .alias("first_purchase_date"),
        pl.col("last_purchase_date")
        .str.to_date(format="%Y-%m-%d")
        .alias("last_purchase_date"),
    ]
)

# Use today's date as the reference date for calculating recency.
reference_date = pl.lit(datetime.datetime.now().date()).cast(pl.Date)

# Create the RFM input dataset and calculate customer-level metrics.
df_rfm = (
    df.select(
        [
            pl.col("mobile_number"),
            pl.col("first_purchase_date"),
            pl.col("last_purchase_date"),
            (
                reference_date - pl.col("last_purchase_date")
            )
            .dt.total_days()
            .alias("Recency"),
            pl.col("total_transactions").alias("Frequency"),
            pl.col("total_amount_spent").alias("Monetary"),
        ]
    )
    .group_by("mobile_number")
    .agg(
        [
            pl.col("first_purchase_date").min(),
            pl.col("last_purchase_date").max(),
            pl.col("Recency").min(),
            pl.col("Frequency").sum(),
            pl.col("Monetary").sum(),
        ]
    )
)


# Calculate RFM scores and assign each customer to an RFM segment.
def add_rfm_calc(
    df: pl.DataFrame | pl.LazyFrame, n_segments: int = 3
) -> pl.DataFrame | pl.LazyFrame:
    # Sort customers before calculating the RFM scores.
    return (
        df.sort(
            "Frequency",
            "Monetary",
            "Recency",
            descending=[False, False, True],
        )
        # Assign quantile-based scores for Recency, Frequency, and Monetary.
        .with_columns(
            pl.col("Recency").qcut(
                quantiles=[n / n_segments for n in range(1, n_segments)],
                labels=[str(n) for n in range(n_segments, 0, -1)],
            ).fill_null("1").cast(pl.String).cast(pl.UInt8).alias("recency_score"),
            pl.col("Frequency").qcut(
                quantiles=[n / n_segments for n in range(1, n_segments)],
                labels=[str(n) for n in range(1, n_segments + 1)],
                allow_duplicates=True,
            ).fill_null("1").cast(pl.String).cast(pl.UInt8).alias("frequency_score"),
            pl.col("Monetary").qcut(
                quantiles=[n / n_segments for n in range(1, n_segments)],
                labels=[str(n) for n in range(1, n_segments + 1)],
                allow_duplicates=True,
            ).fill_null("1").cast(pl.String).cast(pl.UInt8).alias("monetary_score"),
        )
        # Combine the three individual RFM scores into one score.
        .with_columns(
            (
                pl.col("recency_score").cast(pl.String)
                + pl.col("frequency_score").cast(pl.String)
                + pl.col("monetary_score").cast(pl.String)
            ).alias("rfm_combined_score")
        )
        # Map RFM score combinations to business segments.
        .with_columns(
            pl.when(
                (pl.col("Recency") <= 30)
                & (pl.col("Frequency") == 1)
            )
            .then(pl.lit("Recent Customers"))
            .otherwise(
                pl.when(
                    pl.col("rfm_combined_score").is_in(["111", "112"])
                )
                .then(pl.lit("Churn"))
                .when(
                    pl.col("rfm_combined_score").is_in(
                        ["113","122","131","123","132","133","213","223"]
                    )
                )
                .then(pl.lit("Good but Gone"))
                .when(
                    pl.col("rfm_combined_score").is_in(
                        ["121", "211", "212", "221", "222"]
                    )
                )
                .then(pl.lit("Hibernating"))
                .when(
                    pl.col("rfm_combined_score").is_in(
                        ["231","232","233","331","322","323","321"]
                    )
                )
                .then(pl.lit("Potential Loyalist"))
                # The following mapping is intentionally retained as commented code.
                # .when(
                #     pl.col("rfm_combined_score").is_in(
                #         ["311", "312", "313"]
                #     )
                # )
                # .then(pl.lit("Recent Customers"))
                .when(
                    pl.col("rfm_combined_score").is_in(["332"])
                )
                .then(pl.lit("Loyalist"))
                .when(
                    pl.col("rfm_combined_score").is_in(["333"])
                )
                .then(pl.lit("Champion"))
                .otherwise(pl.lit("Potential Loyalist"))
            )
            .alias("Segment")
        )
        # Keep one unique record per mobile number.
        .unique(subset=["mobile_number"])
    )


# Apply the RFM scoring and customer-segmentation function.
astore_data_rfm = df_rfm.pipe(add_rfm_calc)


# Select the final columns required in the RFM output.
astore_data_final_rfm = astore_data_rfm.select(
    pl.col("mobile_number"),
    pl.col("first_purchase_date"),
    pl.col("last_purchase_date"),
    pl.col("Recency"),
    pl.col("Frequency"),
    pl.col("Monetary"),
    pl.col("Segment"),
)


# Standardize mobile numbers by removing the country-code prefix when
# the cleaned number contains more than 10 digits.
astore_data_final_rfm = astore_data_final_rfm.with_columns(
    pl.when(
        pl.col("mobile_number")
        .str.replace_all(r"\D", "")
        .str.len_chars()
        > 10
    )
    .then(
        pl.col("mobile_number").str.replace(
            r"^\+91",
            "",
        )
    )
    .otherwise(
        pl.col("mobile_number").str.replace(
            r"^\+",
            "",
        )
    )
    .alias("mobile_number")
)


# Export the final RFM dataset to a CSV file.
astore_data_final_rfm.write_csv(
    r"C:\Users\IshitaSharma\OneDrive - COMSENSE TECHNOLOGIES PRIVATE LIMITED,\Desktop\RE_IndividualLOB_RFM\ind_rfm_op\aponline_rfm_output_2026-08.csv"
)
