import pandas as pd
import numpy as np
from buildstock_query import MappedColumn, BuildStockQuery


bldgmap = {
    "Mobile Home": "MH",
    "Single-Family Attached": "SF",
    "Single-Family Detached": "SF",
    "Multi-Family with 2 - 4 Units": "MF",
    "Multi-Family with 5+ Units": "MF",
}


def clean_and_pivot_savings(savings_df, scout_col2resstock):
    # add hour of year
    savings_df.insert(loc=0, column='Hour of Year',
                      value=np.arange(len(savings_df), dtype=int)+1)
    savings_df = savings_df.drop(columns='time')
    # delete the end use columns that don't have changes (or less than 0.1% changes)
    available_enduses = []
    for resstock_col_prefixes in scout_col2resstock.keys():
        savings_vals = savings_df[f"{resstock_col_prefixes}__savings"]
        pct_savings = savings_df[f"{resstock_col_prefixes}__savings"] / savings_df[f"{resstock_col_prefixes}__baseline"]
        if (((savings_vals <= 0.1) & (savings_vals >= -0.1)).all(axis=0) or  # absolute savings less than 0.1
           ((pct_savings <= 0.001) & (pct_savings >= -0.001)).all(axis=0)):  # percentage savings less then 0.1%
            savings_df = savings_df.drop(
                [f"{resstock_col_prefixes}__baseline"], axis=1)
            savings_df = savings_df.drop(
                [f"{resstock_col_prefixes}__savings"], axis=1)
        else:
            available_enduses.append(resstock_col_prefixes)

    # save individual end use data to results
    all_frames = []
    for enduse in available_enduses:
        enduse_df = savings_df[['Hour of Year', 'EMM Region', 'Building Type']].copy()
        enduse_df['End Use'] = enduse
        enduse_df['Baseline'] = savings_df[f'{enduse}__baseline']
        enduse_df['Measure'] = savings_df[f'{enduse}__baseline'] - savings_df[f'{enduse}__savings']
        all_frames.append(enduse_df)
    return pd.concat(all_frames)


def end_use_savings_emm(upgrade_num, emm_county, my_run: BuildStockQuery, scout_col2resstock):
    all_enduses = [enduse for enduse_list in scout_col2resstock.values()
                   for enduse in enduse_list]
    bldg_col = my_run.get_column('build_existing_model.geometry_building_type_recs', table_name=my_run.bs_table)
    new_bldg_type = MappedColumn(bsq=my_run, name="Building Type", mapping_dict=bldgmap, key=bldg_col)

    batch_queries = []
    emm_regions = list(emm_county.keys())
    for count, key in enumerate(emm_regions, start=1):
        print(f"Querying for EMM {key} for upgrade {upgrade_num}. {count}/{len(emm_county)} EMM region.")
        res_savings_df_query = my_run.savings.savings_shape(upgrade_id=upgrade_num,
                                                            applied_only=True,
                                                            restrict=[('county', emm_county[key])],
                                                            enduses=all_enduses,
                                                            group_by=[new_bldg_type],
                                                            annual_only=False,
                                                            timestamp_grouping_func='hour',
                                                            get_query_only=True)
        batch_queries.append(res_savings_df_query)
    batch_query_id = my_run.submit_batch_query(batch_queries)
    my_run.wait_for_batch_query(batch_query_id)
    batch_df = my_run.get_batch_query_result(batch_query_id, combine=False)
    result_dfs = []
    for key, res_savings_df in zip(emm_regions, batch_df):
        res_savings_df['EMM Region'] = key  # add emm region
        scout_savings_df = res_savings_df[['time', 'EMM Region', 'Building Type']].copy()
        for col_type in ['baseline', 'savings']:
            for resstock_col_prefixes in scout_col2resstock.keys():
                resstock_cols = [f"{resstock_col_prefix}__{col_type}"
                                 for resstock_col_prefix in scout_col2resstock[resstock_col_prefixes]]
                scout_savings_df[f"{resstock_col_prefixes}__{col_type}"] = res_savings_df[resstock_cols].sum(axis=1)

        dfs = []
        for bldg_type in ['MH', 'SF', 'MF']:
            bldg_type_savings_df = scout_savings_df.loc[res_savings_df['Building Type'] == bldg_type]
            clean_savings_df = clean_and_pivot_savings(bldg_type_savings_df, scout_col2resstock)
            dfs.append(clean_savings_df)
        all_savings_df = pd.concat(dfs)
        result_dfs.append(all_savings_df)

    final_df = pd.concat(result_dfs)
    final_df.to_csv('upgrade'+str(upgrade_num)+'.csv', index=False)
