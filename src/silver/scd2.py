"""
Slowly Changing Dimension Type 2 merge, implemented in pure PySpark.

Why hand-rolled instead of a Delta / Iceberg MERGE? Two reasons:

  1. It runs anywhere - plain Parquet on S3, no table-format jars, which
     matters when the same code has to work in Glue, EMR and a laptop.
  2. It makes the history semantics explicit and testable rather than
     hiding them inside a MERGE statement.

Semantics
---------
For each business key we keep one row per distinct version of the tracked
attributes:

    effective_from   timestamp the version became true
    effective_to     timestamp it stopped being true (high date if current)
    is_current       boolean convenience flag
    version_number   1, 2, 3 ...
    record_hash      sha2 of the tracked columns, used for change detection

Change detection compares `record_hash`, not column-by-column equality,
so adding a tracked column is a one-line change.

Late-arriving records are handled: the incoming batch is ordered by its
own effective timestamp, so an out-of-order file still produces a
correctly ordered history.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

HIGH_DATE = "9999-12-31 23:59:59"


def record_hash(tracked_cols: list[str]) -> Column:
    """SHA-256 over the tracked attributes; nulls normalised to a sentinel."""
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in tracked_cols]
    return F.sha2(F.concat_ws("||", *parts), 256)


def _stamp(df: DataFrame, business_key: str, tracked_cols: list[str],
           effective_col: str) -> DataFrame:
    return (df
            .withColumn("record_hash", record_hash(tracked_cols))
            .withColumn("effective_from", F.col(effective_col).cast("timestamp")))


def initial_load(source: DataFrame, business_key: str, tracked_cols: list[str],
                 effective_col: str) -> DataFrame:
    """
    Build a dimension from scratch. The source may already contain multiple
    versions of the same key (e.g. a base extract plus a delta file), so we
    collapse consecutive identical versions and close out superseded ones.
    """
    stamped = _stamp(source, business_key, tracked_cols, effective_col)

    w = Window.partitionBy(business_key).orderBy("effective_from")

    # Drop consecutive rows whose tracked attributes did not actually change.
    deduped = (stamped
               .withColumn("_prev_hash", F.lag("record_hash").over(w))
               .filter(F.col("_prev_hash").isNull() | (F.col("_prev_hash") != F.col("record_hash")))
               .drop("_prev_hash"))

    return _close_out(deduped, business_key)


def _close_out(df: DataFrame, business_key: str) -> DataFrame:
    """Assign effective_to / is_current / version_number over an ordered history."""
    w = Window.partitionBy(business_key).orderBy("effective_from")
    return (df
            .withColumn("_next_from", F.lead("effective_from").over(w))
            .withColumn("effective_to",
                        F.when(F.col("_next_from").isNull(), F.lit(HIGH_DATE).cast("timestamp"))
                         .otherwise(F.col("_next_from") - F.expr("INTERVAL 1 SECOND")))
            .withColumn("is_current", F.col("_next_from").isNull())
            .withColumn("version_number", F.row_number().over(w))
            .drop("_next_from"))


def apply_scd2(target: DataFrame | None, source: DataFrame, business_key: str,
               tracked_cols: list[str], effective_col: str) -> DataFrame:
    """
    Merge `source` into an existing SCD2 `target` and return the full new
    dimension (all versions, correctly closed out).

    Rules applied
    -------------
    NEW key           -> inserted as version 1, is_current = true
    CHANGED hash      -> previous current row is closed, new version opened
    UNCHANGED hash    -> no-op, existing row untouched (effective_from kept)
    DELETED from src  -> left alone (soft-delete handling is a Gold concern)
    """
    if target is None:
        return initial_load(source, business_key, tracked_cols, effective_col)

    src = _stamp(source, business_key, tracked_cols, effective_col)

    # Only consider source rows that are genuinely newer than what we hold.
    current = target.filter(F.col("is_current"))
    current_slim = current.select(
        F.col(business_key).alias("_k"),
        F.col("record_hash").alias("_cur_hash"),
        F.col("effective_from").alias("_cur_from"),
    )

    joined = src.join(current_slim, src[business_key] == F.col("_k"), "left")

    changed_or_new = (joined
                      .filter(F.col("_cur_hash").isNull()
                              | ((F.col("_cur_hash") != F.col("record_hash"))
                                 & (F.col("effective_from") > F.col("_cur_from"))))
                      .drop("_k", "_cur_hash", "_cur_from"))

    # Everything we already hold, plus the new versions, re-closed as one history.
    history_cols = [c for c in target.columns
                    if c not in ("effective_to", "is_current", "version_number")]
    combined = (target.select(*history_cols)
                .unionByName(changed_or_new.select(*history_cols), allowMissingColumns=True))

    return _close_out(combined, business_key)


def current_view(dim: DataFrame) -> DataFrame:
    """The 'today' slice of a Type 2 dimension."""
    return dim.filter(F.col("is_current"))


def as_of(dim: DataFrame, as_of_ts: str) -> DataFrame:
    """
    Point-in-time slice. This is what makes the warehouse defensible in an
    audit: 'what did we believe about this patient on the claim date?'
    """
    ts = F.lit(as_of_ts).cast("timestamp")
    return dim.filter((F.col("effective_from") <= ts) & (F.col("effective_to") >= ts))
