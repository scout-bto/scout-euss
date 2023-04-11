from buildstock_query import BuildStockQuery
import pandas as pd
import numpy as np

upgrade_num = 2 #define upgrade number, change this number for different upgrade packages

# get county name list for each EMM region
emm_name = ['SRSE', 'SRCE', 'SRSG', 'MISS', 'SPPS', 'CANO', 'NWPP', 'CASO', 'RMRG', 
            'ISNE', 'PJME', 'PJMD', 'FRCC', 'BASN', 'MISC', 'PJMC','MISW', 'PJMW', 
            'SPPN', 'SPPC', 'MISE', 'NYUP', 'NYCW', 'SRCA', 'TRE']
emm_county = []

emm_mapping = pd.read_csv('county_to_emm.csv')
for i in range (len(emm_name)):
    emm_temp = emm_mapping.loc[emm_mapping['emm'] == emm_name[i], 'county'].values.tolist()
    emm_county += [emm_temp]

my_run = BuildStockQuery(db_name='euss-final',
                        table_name='euss_res_final_2018_550k_20220901',
                        workgroup='scout',
                        buildstock_type='resstock')

results = pd.DataFrame(columns=['Hour of Year', 'EMM Region', 'Building Type'])
for i in range (len(emm_name)):
    #get data in 15 min timestep
    temp_15 = my_run.savings.savings_shape(upgrade_id=upgrade_num,applied_only=True, restrict=[('county',emm_county[i])], 
                                        enduses = ['fuel_use__electricity__total__kwh',
                                                   'end_use__electricity__cooling__kwh',
                                                   'end_use__electricity__heating__kwh',
                                                   'end_use__electricity__heating_heat_pump_backup__kwh',
                                                   'end_use__electricity__clothes_dryer__kwh',
                                                   'end_use__electricity__clothes_washer__kwh',
                                                   'end_use__electricity__range_oven__kwh',
                                                   'end_use__electricity__dishwasher__kwh',
                                                   'end_use__electricity__cooling_fans_pumps__kwh',
                                                   'end_use__electricity__heating_fans_pumps__kwh',
                                                   'end_use__electricity__mech_vent__kwh',
                                                   'end_use__electricity__lighting_exterior__kwh',
                                                   'end_use__electricity__lighting_garage__kwh',
                                                   'end_use__electricity__lighting_interior__kwh',
                                                   'end_use__electricity__ceiling_fan__kwh',
                                                   'end_use__electricity__hot_tub_heater__kwh',
                                                   'end_use__electricity__hot_tub_pump__kwh',
                                                   'end_use__electricity__well_pump__kwh',
                                                   'end_use__electricity__plug_loads__kwh',
                                                   'end_use__electricity__pool_heater__kwh',
                                                   'end_use__electricity__pool_pump__kwh',
                                                   'end_use__electricity__freezer__kwh',
                                                   'end_use__electricity__refrigerator__kwh',
                                                   'end_use__electricity__hot_water__kwh'],
                                        group_by = ['geometry_building_type_recs'], annual_only=False)
    #rename column in the Scout-EUSS format
    temp_15.rename(columns={'geometry_building_type_recs': 'Building Type'}, inplace=True)
    
    #change the data to 60 min timestep
    temp = pd.DataFrame(columns=['time', 'EMM Region', 'Building Type','End Use'])
    temp['time'] = temp_15['time'].groupby(np.arange(len(temp_15))//4).first().astype(str).str[5:13]
    temp['Building Type'] = temp_15['Building Type'].groupby(np.arange(len(temp_15))//4).first()
    temp_other= temp_15.groupby(np.arange(len(temp_15))//4).sum()
    temp=pd.concat([temp,temp_other],axis=1)

    # temp['EMM Region'] = pd.Series([emm_name[i] for x in range(len(temp.index))])
    temp['EMM Region'] = emm_name[i] #add emm region

    #group end use data to scout-euss format
    temp['heating__baseline'] = temp['end_use__electricity__heating__kwh__baseline'] + temp['end_use__electricity__heating_heat_pump_backup__kwh__baseline']
    temp['heating__savings'] = temp['end_use__electricity__heating__kwh__savings'] + temp['end_use__electricity__heating_heat_pump_backup__kwh__savings']
    temp['cooling__baseline'] = temp['end_use__electricity__cooling__kwh__baseline']
    temp['cooling__savings'] = temp['end_use__electricity__cooling__kwh__savings']
    temp['water heating__baseline'] = temp['end_use__electricity__hot_water__kwh__baseline']
    temp['water heating__savings'] = temp['end_use__electricity__hot_water__kwh__savings']
    temp['lighting__baseline'] = temp['end_use__electricity__lighting_exterior__kwh__baseline']+temp['end_use__electricity__lighting_garage__kwh__baseline']+temp['end_use__electricity__lighting_interior__kwh__baseline']
    temp['lighting__savings'] = temp['end_use__electricity__lighting_exterior__kwh__savings']+temp['end_use__electricity__lighting_garage__kwh__savings']+temp['end_use__electricity__lighting_interior__kwh__savings']
    temp['cooking__baseline'] = temp['end_use__electricity__range_oven__kwh__baseline']
    temp['cooking__savings'] = temp['end_use__electricity__range_oven__kwh__savings']
    temp['refrigeration__baseline'] = temp['end_use__electricity__freezer__kwh__baseline']+temp['end_use__electricity__refrigerator__kwh__baseline']
    temp['refrigeration__savings'] = temp['end_use__electricity__freezer__kwh__savings']+temp['end_use__electricity__refrigerator__kwh__savings']
    temp['clothes washing__baseline'] = temp['end_use__electricity__clothes_washer__kwh__baseline']
    temp['clothes washing__savings'] = temp['end_use__electricity__clothes_washer__kwh__savings']
    temp['clothes drying__baseline'] = temp['end_use__electricity__clothes_dryer__kwh__baseline']
    temp['clothes drying__savings'] = temp['end_use__electricity__clothes_dryer__kwh__savings']
    temp['dishwasher__baseline'] = temp['end_use__electricity__dishwasher__kwh__baseline']
    temp['dishwasher__savings'] = temp['end_use__electricity__dishwasher__kwh__savings']
    temp['pool heaters and pumps__baseline'] = temp['end_use__electricity__pool_heater__kwh__baseline']+temp['end_use__electricity__pool_pump__kwh__baseline']
    temp['pool heaters and pumps__savings'] = temp['end_use__electricity__pool_heater__kwh__savings']+temp['end_use__electricity__pool_pump__kwh__savings']
    temp['fans and pumps__baseline'] = temp['end_use__electricity__cooling_fans_pumps__kwh__baseline']+temp['end_use__electricity__heating_fans_pumps__kwh__baseline']+temp['end_use__electricity__mech_vent__kwh__baseline']
    temp['fans and pumps__savings'] = temp['end_use__electricity__cooling_fans_pumps__kwh__savings']+temp['end_use__electricity__heating_fans_pumps__kwh__savings']+temp['end_use__electricity__mech_vent__kwh__savings']
    temp['plug loads__baseline'] = temp['end_use__electricity__plug_loads__kwh__baseline']
    temp['plug loads__savings'] = temp['end_use__electricity__plug_loads__kwh__savings']
    temp['other__baseline'] = temp['end_use__electricity__ceiling_fan__kwh__baseline']+temp['end_use__electricity__hot_tub_heater__kwh__baseline']+temp['end_use__electricity__hot_tub_pump__kwh__baseline']+temp['end_use__electricity__well_pump__kwh__baseline']
    temp['other__savings'] = temp['end_use__electricity__ceiling_fan__kwh__savings']+temp['end_use__electricity__hot_tub_heater__kwh__savings']+temp['end_use__electricity__hot_tub_pump__kwh__savings']+temp['end_use__electricity__well_pump__kwh__savings']
    
    #get the data needed by Scout-EUSS
    temp = temp[['time','EMM Region', 'Building Type','heating__baseline','heating__savings',
                 'cooling__baseline','cooling__savings','water heating__baseline','water heating__savings',
                 'lighting__baseline','lighting__savings','cooking__baseline','cooking__savings',
                 'refrigeration__baseline','refrigeration__savings','clothes washing__baseline','clothes washing__savings',
                 'clothes drying__baseline','clothes drying__savings','dishwasher__baseline','dishwasher__savings',
                 'pool heaters and pumps__baseline','pool heaters and pumps__savings',
                 'fans and pumps__baseline','fans and pumps__savings','plug loads__baseline','plug loads__savings',
                 'other__baseline','other__savings']]

    agg_functions = {'time': 'first','EMM Region': 'first','Building Type': 'first',
                     'heating__baseline': 'sum', 'heating__savings': 'sum',
                     'cooling__baseline': 'sum', 'cooling__savings': 'sum',
                     'water heating__baseline': 'sum','water heating__savings': 'sum',
                     'lighting__baseline': 'sum','lighting__savings': 'sum',
                     'cooking__baseline': 'sum','cooking__savings': 'sum',
                     'refrigeration__baseline': 'sum','refrigeration__savings': 'sum',
                     'clothes washing__baseline': 'sum','clothes washing__savings': 'sum',
                     'clothes drying__baseline': 'sum','clothes drying__savings': 'sum',
                     'dishwasher__baseline': 'sum','dishwasher__savings': 'sum',
                     'pool heaters and pumps__baseline': 'sum','pool heaters and pumps__savings': 'sum',
                     'fans and pumps__baseline': 'sum','fans and pumps__savings': 'sum',
                     'plug loads__baseline': 'sum','plug loads__savings': 'sum',
                     'other__baseline': 'sum','other__savings': 'sum'}
    
    #rename Mobile Home to MH
    MH_temp=temp.loc[temp['Building Type'].isin(['Mobile Home'])]
    MH = MH_temp.groupby(MH_temp['time']).aggregate(agg_functions)
    MH['Building Type'] = MH['Building Type'].replace('Mobile Home','MH')
    #add hour of year
    MH.insert(loc=0, column='Hour of Year', value=np.arange(len(MH), dtype=int)+1)
    # delete the end use columns that doesn't have savings
    if (MH['heating__savings'] >= 0).all(axis=0):
        MH = MH.drop(['heating__baseline','heating__savings'], axis=1)
    if (MH['cooling__savings'] >= 0).all(axis=0):
        MH = MH.drop(['cooling__baseline','cooling__savings'], axis=1)
    if (MH['water heating__savings'] >= 0).all(axis=0):
        MH = MH.drop(['water heating__baseline','water heating__savings'], axis=1)
    if (MH['lighting__savings'] >= 0).all(axis=0):
        MH = MH.drop(['lighting__baseline','lighting__savings'], axis=1)
    if (MH['cooking__savings'] >= 0).all(axis=0):
        MH = MH.drop(['cooking__baseline','cooking__savings'], axis=1)
    if (MH['refrigeration__savings'] >= 0).all(axis=0):
        MH = MH.drop(['refrigeration__baseline','refrigeration__savings'], axis=1)
    if (MH['clothes washing__savings'] >= 0).all(axis=0):
        MH = MH.drop(['clothes washing__baseline','clothes washing__savings'], axis=1)
    if (MH['clothes drying__savings'] >= 0).all(axis=0):
        MH = MH.drop(['clothes drying__baseline','clothes drying__savings'], axis=1)
    if (MH['dishwasher__savings'] >= 0).all(axis=0):
        MH = MH.drop(['dishwasher__baseline','dishwasher__savings'], axis=1)
    if (MH['pool heaters and pumps__savings'] >= 0).all(axis=0):
        MH = MH.drop(['pool heaters and pumps__baseline','pool heaters and pumps__savings'], axis=1)
    if (MH['fans and pumps__savings'] >= 0).all(axis=0):
        MH = MH.drop(['fans and pumps__baseline','fans and pumps__savings'], axis=1)
    if (MH['plug loads__savings'] >= 0).all(axis=0):
        MH = MH.drop(['plug loads__baseline','plug loads__savings'], axis=1)
    if (MH['other__savings'] >= 0).all(axis=0):
        MH = MH.drop(['other__baseline','other__savings'], axis=1)
    # save individual end use data to results
    num_end_use = int((MH.shape[1]-3)/2)
    for j in range (num_end_use):
        MH_end_use = MH[['Hour of Year','EMM Region', 'Building Type']]
        MH_end_use['End Use'] = MH.columns[4+j*2].split('__')[0]
        MH_end_use['Baseline'] = MH[MH.columns[4+j*2]]
        MH_end_use['Measure'] = MH[MH.columns[4+j*2]]-MH[MH.columns[5+j*2]]
        frames = [results, MH_end_use]
        results = pd.concat(frames)
    
    #aggragate Single-Family Attached and Single-Family Detached to SF
    SF_temp=temp.loc[temp['Building Type'].isin(['Single-Family Attached','Single-Family Detached'])]
    SF = SF_temp.groupby(SF_temp['time']).aggregate(agg_functions)
    SF['Building Type'] = SF['Building Type'].replace('Single-Family Attached','SF').replace('Single-Family Detached','SF')
    #add hour of year
    SF.insert(loc=0, column='Hour of Year', value=np.arange(len(SF), dtype=int)+1)
    # delete the end use columns that doesn't have savings
    if (SF['heating__savings'] >= 0).all(axis=0):
        SF = SF.drop(['heating__baseline','heating__savings'], axis=1)
    if (SF['cooling__savings'] >= 0).all(axis=0):
        SF = SF.drop(['cooling__baseline','cooling__savings'], axis=1)
    if (SF['water heating__savings'] >= 0).all(axis=0):
        SF = SF.drop(['water heating__baseline','water heating__savings'], axis=1)
    if (SF['lighting__savings'] >= 0).all(axis=0):
        SF = SF.drop(['lighting__baseline','lighting__savings'], axis=1)
    if (SF['cooking__savings'] >= 0).all(axis=0):
        SF = SF.drop(['cooking__baseline','cooking__savings'], axis=1)
    if (SF['refrigeration__savings'] >= 0).all(axis=0):
        SF = SF.drop(['refrigeration__baseline','refrigeration__savings'], axis=1)
    if (SF['clothes washing__savings'] >= 0).all(axis=0):
        SF = SF.drop(['clothes washing__baseline','clothes washing__savings'], axis=1)
    if (SF['clothes drying__savings'] >= 0).all(axis=0):
        SF = SF.drop(['clothes drying__baseline','clothes drying__savings'], axis=1)
    if (SF['dishwasher__savings'] >= 0).all(axis=0):
        SF = SF.drop(['dishwasher__baseline','dishwasher__savings'], axis=1)
    if (SF['pool heaters and pumps__savings'] >= 0).all(axis=0):
        SF = SF.drop(['pool heaters and pumps__baseline','pool heaters and pumps__savings'], axis=1)
    if (SF['fans and pumps__savings'] >= 0).all(axis=0):
        SF = SF.drop(['fans and pumps__baseline','fans and pumps__savings'], axis=1)
    if (SF['plug loads__savings'] >= 0).all(axis=0):
        SF = SF.drop(['plug loads__baseline','plug loads__savings'], axis=1)
    if (SF['other__savings'] >= 0).all(axis=0):
        SF = SF.drop(['other__baseline','other__savings'], axis=1)
    # save individual end use data to results
    num_end_use = int((SF.shape[1]-3)/2)
    for j in range (num_end_use):
        SF_end_use = SF[['Hour of Year','EMM Region', 'Building Type']]
        SF_end_use['End Use'] = SF.columns[4+j*2].split('__')[0]
        SF_end_use['Baseline'] = SF[SF.columns[4+j*2]]
        SF_end_use['Measure'] = SF[SF.columns[4+j*2]]-SF[SF.columns[5+j*2]]
        frames = [results, SF_end_use]
        results = pd.concat(frames)

    #aggragate Multi-Family with 2 - 4 Units and Multi-Family with 5+ Units to MF
    MF_temp=temp.loc[temp['Building Type'].isin(['Multi-Family with 2 - 4 Units','Multi-Family with 5+ Units'])]
    MF = MF_temp.groupby(MF_temp['time']).aggregate(agg_functions)
    MF['Building Type'] = MF['Building Type'].replace('Multi-Family with 2 - 4 Units','MF').replace('Multi-Family with 5+ Units','MF')
    #add hour of year
    MF.insert(loc=0, column='Hour of Year', value=np.arange(len(MF), dtype=int)+1)
    # delete the end use columns that doesn't have savings
    if (MF['heating__savings'] >= 0).all(axis=0):
        MF = MF.drop(['heating__baseline','heating__savings'], axis=1)
    if (MF['cooling__savings'] >= 0).all(axis=0):
        MF = MF.drop(['cooling__baseline','cooling__savings'], axis=1)
    if (MF['water heating__savings'] >= 0).all(axis=0):
        MF = MF.drop(['water heating__baseline','water heating__savings'], axis=1)
    if (MF['lighting__savings'] >= 0).all(axis=0):
        MF = MF.drop(['lighting__baseline','lighting__savings'], axis=1)
    if (MF['cooking__savings'] >= 0).all(axis=0):
        MF = MF.drop(['cooking__baseline','cooking__savings'], axis=1)
    if (MF['refrigeration__savings'] >= 0).all(axis=0):
        MF = MF.drop(['refrigeration__baseline','refrigeration__savings'], axis=1)
    if (MF['clothes washing__savings'] >= 0).all(axis=0):
        MF = MF.drop(['clothes washing__baseline','clothes washing__savings'], axis=1)
    if (MF['clothes drying__savings'] >= 0).all(axis=0):
        MF = MF.drop(['clothes drying__baseline','clothes drying__savings'], axis=1)
    if (MF['dishwasher__savings'] >= 0).all(axis=0):
        MF = MF.drop(['dishwasher__baseline','dishwasher__savings'], axis=1)
    if (MF['pool heaters and pumps__savings'] >= 0).all(axis=0):
        MF = MF.drop(['pool heaters and pumps__baseline','pool heaters and pumps__savings'], axis=1)
    if (MF['fans and pumps__savings'] >= 0).all(axis=0):
        MF = MF.drop(['fans and pumps__baseline','fans and pumps__savings'], axis=1)
    if (MF['plug loads__savings'] >= 0).all(axis=0):
        MF = MF.drop(['plug loads__baseline','plug loads__savings'], axis=1)
    if (MF['other__savings'] >= 0).all(axis=0):
        MF = MF.drop(['other__baseline','other__savings'], axis=1)
    # save individual end use data to results
    num_end_use = int((MF.shape[1]-3)/2)
    for j in range (num_end_use):
        MF_end_use = MF[['Hour of Year','EMM Region', 'Building Type']]
        MF_end_use['End Use'] = MF.columns[4+j*2].split('__')[0]
        MF_end_use['Baseline'] = MF[MF.columns[4+j*2]]
        MF_end_use['Measure'] = MF[MF.columns[4+j*2]]-MF[MF.columns[5+j*2]]
        frames = [results, MF_end_use]
        results = pd.concat(frames)

results.to_csv('upgrade'+str(upgrade_num)+'.csv', index=False)