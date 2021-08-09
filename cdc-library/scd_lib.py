from typing import List, Union
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, lit, hash, array, explode, md5, concat_ws

SCD_COLS = ["rec_eff_dt", "row_opern"]

def rename_columns(df: DataFrame, alias: str, keys_list: List, add_suffix: int = 1) -> DataFrame:
    """ Rename columns to denote if they belong to history ot current.

    Args:
        df (DataFrame): dataframe to perform renaming on
        alias (str): alias to add as suffix
        keys_list (List): list of keys which are used for joins
        add_suffix (int, optional): 0 - remove suffix, 1- add suffix. Defaults to 1.

    Returns:
        DataFrame: dataframe with performed adding/removing of suffix
    """
    if add_suffix == 1:
        for column in set(df.columns) - set(SCD_COLS) - set(keys_list):
            df = df.withColumnRenamed(column, column + "_" + alias)
    else:
        for column in [cols for cols in df.columns if f"_{alias}" in cols]:
            df = df.withColumnRenamed(column, column.replace("_" + alias, ""))
    return df


def get_hash_column(dataframe: DataFrame, keys_list: List, ignored_columns: List = []) -> DataFrame:
    """ Hash column to generate hash for non-key columns. Algorithm used: MD5

    Args:
        dataframe (DataFrame): dataframe to add "hash" column to
        keys_list (List): keys_list which will be used for joins
        ignored_columns (List): columns which will not be included in hash apart from SCD_COLS and keys_list

    Returns:
        DataFrame: Dataframe with added column "hash"
    """
    cols = list(set(dataframe.columns) - set(keys_list) - set(SCD_COLS) - set(ignored_columns))
    columns = [col(column) for column in cols]
    if columns:
        return dataframe.withColumn("hash", md5(concat_ws("|", *columns)))
    else:
        return dataframe.withColumn("hash", md5(lit(1)))

def get_no_change_records(history: DataFrame,
                        current: DataFrame,
                        keys_list: List, ignored_columns: List = []) -> DataFrame:
    """ handling of not changed rows.

    Args:
        history (DataFrame): open SCD rows
        current (DataFrame): current status
        keys_list (List): keys list used for joins
        ignored_columns (List, optional): keys not to be considered for determining change
        Defaults to empty list.

    Returns:
        DataFrame: Spark DF with not changed rows
    """

    history_hash = rename_columns(get_hash_column(history, keys_list, ignored_columns), alias="history", keys_list=keys_list)
    current_hash = rename_columns(get_hash_column(current, keys_list, ignored_columns), alias="current", keys_list=keys_list)

    not_changed = rename_columns(history_hash
                                 .join(other=current_hash, on=keys_list, how="inner")
                                 .where(history_hash["hash_history"] == current_hash["hash_current"])
                                 .drop(*["hash_history", "hash_current"])
                                 .drop(*[column for column in current_hash.columns if "_current" in column])
                                 , alias="history", keys_list=keys_list, add_suffix=0).select(history.columns)
    not_changed = not_changed.withColumn("row_opern", lit("N"))


    return (not_changed)

def get_updated_records(history: DataFrame,
                        current: DataFrame,
                        keys_list: List, rec_eff_dt: str, ignored_columns: List = []) -> DataFrame:
    """ handling of updated rows.

    Args:
        history (DataFrame): open SCD rows
        current (DataFrame): current status
        keys_list (List): keys list used for joins
        rec_eff_dt (str): to mark the effective date for updated records
        ignored_columns (List, optional): keys not to be considered for determining change
        Defaults to empty list.

    Returns:
        DataFrame: Spark DF with updated rows
    """

    history_hash = rename_columns(get_hash_column(history, keys_list, ignored_columns), alias="history", keys_list=keys_list)
    current_hash = rename_columns(get_hash_column(current, keys_list, ignored_columns), alias="current", keys_list=keys_list)

    changed = (history_hash
               .join(other=current_hash, on=keys_list, how="inner")
               .where(history_hash["hash_history"] != current_hash["hash_current"])
               )

    updated = (rename_columns((changed.withColumn("rec_eff_dt", lit(rec_eff_dt))
                               .drop(*["hash_history", "hash_current", "hist_flag"])
                               .drop(*[column for column in changed.columns if "_history" in column])
                               ), alias="current", keys_list=keys_list, add_suffix=0)
               .withColumn("row_opern", lit("U"))
               .select(history.columns))

    return (updated)


def get_new_records(history: DataFrame,
             current: DataFrame,
             keys_list: List, rec_eff_dt: str) ->DataFrame:
    """Handling new rows insertion

    Args:
        history (DataFrame): SCD open rows DF
        current (DataFrame): current state
        keys_list (List): keys list for joins
        rec_eff_dt (str): to mark the effective date for new records

    Returns:
        DataFrame: Only new rows part is returned
    """
    new = current.join(other=history, on=keys_list, how="left_anti")
    new = (new.withColumn("row_opern", lit("I")))

    return new.withColumn("rec_eff_dt", lit(rec_eff_dt))


def get_deleted_rows(history: DataFrame,
                 current: DataFrame,
                 keys_list: List, rec_eff_dt: str) -> DataFrame:
    """Handling of deleted rows

    Args:
        history (DataFrame): SCD open rows
        current (DataFrame): current state DF
        keys_list (List): keys list for joins
        rec_eff_dt (str): to mark the effective date for deleted records

    Returns:
        DataFrame: DF with deleted rows changes
    """

    deleted = history.join(other=current, on=keys_list, how="left_anti")
    deleted = (deleted.withColumn("row_opern", lit("D")))
    return deleted.withColumn("rec_eff_dt", lit(rec_eff_dt))