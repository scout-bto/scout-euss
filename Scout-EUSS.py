from buildstock_query import BuildStockQuery
import pandas as pd
import numpy as np

upgrade_num = 3 #define upgrade number, change this number for different upgrade packages

# get county name list for each EMM region
emm_mapping = pd.read_csv('county_to_emm.csv')
emm_county = {k: g["county"].tolist() for k,g in emm_mapping.groupby("emm")}

my_run = BuildStockQuery(db_name='euss-final',
                        table_name='euss_res_final_2018_550k_20220901',
                        workgroup='scout',
                        buildstock_type='resstock')

def end_use(building_temp, scout_col2resstock, agg_functions):
    building = building_temp.groupby(building_temp['time']).aggregate(agg_functions)
    #add hour of year
    building.insert(loc=0, column='Hour of Year', value=np.arange(len(building), dtype=int)+1)
    # delete the end use columns that don't have changes
    # delete the end use columns that have very small negative savings (less than 1%)
    for resstock_col_prefixes in scout_col2resstock.keys():
        if (building[f"{resstock_col_prefixes}__savings"] == 0).all(axis=0):
            building = building.drop([f"{resstock_col_prefixes}__baseline"], axis=1)
            building = building.drop([f"{resstock_col_prefixes}__savings"], axis=1)
        elif (building[f"{resstock_col_prefixes}__savings"] <= 0).all(axis=0) and (building[f"{resstock_col_prefixes}__savings"]/building[f"{resstock_col_prefixes}__baseline"]>-0.01).all(axis=0):
            building = building.drop([f"{resstock_col_prefixes}__baseline"], axis=1)
            building = building.drop([f"{resstock_col_prefixes}__savings"], axis=1)
            
    # save individual end use data to results
    building_results = pd.DataFrame(columns=['Hour of Year', 'EMM Region', 'Building Type'])
    num_end_use = int((building.shape[1]-3)/2)
    all_frames = []
    for j in range (num_end_use):
        building_end_use = building[['Hour of Year','EMM Region', 'Building Type']].copy()
        building_end_use['End Use'] = building.columns[4+j].split('__')[0]
        building_end_use['Baseline'] = building[building.columns[4+j]]
        building_end_use['Measure'] = building[building.columns[4+j]]-building[building.columns[4+num_end_use+j]]
        all_frames.append(building_end_use)
    building_results = pd.concat(all_frames)
    return building_results

#group end use data to scout-euss format
scout_col2resstock = {
    'heating': ['end_use__electricity__heating__kwh', 'end_use__electricity__heating_heat_pump_backup__kwh'],
    'cooling': ['end_use__electricity__cooling__kwh'],
    'water heating': ['end_use__electricity__hot_water__kwh'],
    'lighting': ['end_use__electricity__lighting_exterior__kwh','end_use__electricity__lighting_garage__kwh','end_use__electricity__lighting_interior__kwh'],
    'cooking': ['end_use__electricity__range_oven__kwh'],
    'refrigeration': ['end_use__electricity__freezer__kwh','end_use__electricity__refrigerator__kwh'],
    'clothes washing': ['end_use__electricity__clothes_washer__kwh'],
    'clothes drying': ['end_use__electricity__clothes_dryer__kwh'],
    'dishwasher': ['end_use__electricity__dishwasher__kwh'],
    'pool heaters and pumps': ['end_use__electricity__pool_heater__kwh','end_use__electricity__pool_pump__kwh'],
    'fans and pumps': ['end_use__electricity__cooling_fans_pumps__kwh','end_use__electricity__heating_fans_pumps__kwh','end_use__electricity__mech_vent__kwh'],
    'plug loads': ['end_use__electricity__plug_loads__kwh'],
    'other': ['end_use__electricity__ceiling_fan__kwh','end_use__electricity__hot_tub_heater__kwh','end_use__electricity__hot_tub_pump__kwh','end_use__electricity__well_pump__kwh']  
    }

all_enduses = [enduse for enduse_list in scout_col2resstock.values() for enduse in enduse_list]

results = pd.DataFrame(columns=['Hour of Year', 'EMM Region', 'Building Type'])
for key in emm_county:
    #get data in 15 min timestep
    temp_15 = my_run.savings.savings_shape(upgrade_id=upgrade_num,applied_only=True, restrict=[('county',emm_county[key])], 
                                        enduses = all_enduses,
                                        group_by = ['geometry_building_type_recs'], annual_only=False)
    #rename column in the Scout-EUSS format
    temp_15.rename(columns={'geometry_building_type_recs': 'Building Type'}, inplace=True)
    
    #change the data to 60 min timestep
    temp_60 = pd.DataFrame(columns=['time', 'EMM Region', 'Building Type','End Use'])
    temp_60['time'] = temp_15['time'].groupby(np.arange(len(temp_15))//4).first().astype(str).str[5:13]
    temp_60['Building Type'] = temp_15['Building Type'].groupby(np.arange(len(temp_15))//4).first()
    temp_other_60= temp_15.groupby(np.arange(len(temp_15))//4).sum()
    temp_60=pd.concat([temp_60,temp_other_60],axis=1)

    temp_60['EMM Region'] = key #add emm region
    
    #get the data needed by Scout-EUSS
    temp = temp_60[['time', 'EMM Region', 'Building Type','End Use']]
    agg_functions = {'time': 'first','EMM Region': 'first','Building Type': 'first'}
    for col_type in ['baseline', 'savings']:
        for resstock_col_prefixes in scout_col2resstock.keys():
            resstock_cols = [f"{resstock_col_prefix}__{col_type}" for resstock_col_prefix in scout_col2resstock[resstock_col_prefixes]]
            temp[f"{resstock_col_prefixes}__{col_type}"] = temp_60[resstock_cols].sum(axis=1)
            agg_functions[f"{resstock_col_prefixes}__{col_type}"] = 'sum'   
    
    #rename Mobile Home to MH
    MH_temp=temp.loc[temp['Building Type'].isin(['Mobile Home'])]
    MH_temp['Building Type'] = MH_temp['Building Type'].replace('Mobile Home','MH')
    MH = end_use(MH_temp, scout_col2resstock, agg_functions)
    
    #aggragate Single-Family Attached and Single-Family Detached to SF
    SF_temp=temp.loc[temp['Building Type'].isin(['Single-Family Attached','Single-Family Detached'])]
    SF_temp['Building Type'] = SF_temp['Building Type'].replace('Single-Family Attached','SF').replace('Single-Family Detached','SF')
    SF = end_use(SF_temp, scout_col2resstock, agg_functions)
    
    #aggragate Multi-Family with 2 - 4 Units and Multi-Family with 5+ Units to MF
    MF_temp=temp.loc[temp['Building Type'].isin(['Multi-Family with 2 - 4 Units','Multi-Family with 5+ Units'])]
    MF_temp['Building Type'] = MF_temp['Building Type'].replace('Multi-Family with 2 - 4 Units','MF').replace('Multi-Family with 5+ Units','MF')
    MF = end_use(MF_temp, scout_col2resstock, agg_functions)
    
    frames = [results,MH, SF, MF]
    results = pd.concat(frames)

results.to_csv('upgrade'+str(upgrade_num)+'.csv', index=False)