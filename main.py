import Scout_EUSS as SE
from buildstock_query import BuildStockQuery
import pandas as pd

upgrade_nums = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
# get county name list for each EMM region
emm_mapping = pd.read_csv('county_to_emm.csv')
emm_county = {k: g["county"].tolist() for k, g in emm_mapping.groupby("emm")}

my_run = BuildStockQuery(db_name='euss-final',
                         table_name='euss_res_final_2018_550k_20220901',
                         workgroup='scout',
                         buildstock_type='resstock')

# group end use data to scout-euss format
scout_col2resstock = {
    'heating': ['end_use__electricity__heating__kwh', 'end_use__electricity__heating_heat_pump_backup__kwh'],
    'cooling': ['end_use__electricity__cooling__kwh'],
    'water heating': ['end_use__electricity__hot_water__kwh'],
    'lighting': ['end_use__electricity__lighting_exterior__kwh', 'end_use__electricity__lighting_garage__kwh',
                 'end_use__electricity__lighting_interior__kwh'],
    'cooking': ['end_use__electricity__range_oven__kwh'],
    'refrigeration': ['end_use__electricity__freezer__kwh', 'end_use__electricity__refrigerator__kwh'],
    'clothes washing': ['end_use__electricity__clothes_washer__kwh'],
    'clothes drying': ['end_use__electricity__clothes_dryer__kwh'],
    'dishwasher': ['end_use__electricity__dishwasher__kwh'],
    'pool heaters and pumps': ['end_use__electricity__pool_heater__kwh', 'end_use__electricity__pool_pump__kwh'],
    'fans and pumps': ['end_use__electricity__cooling_fans_pumps__kwh', 'end_use__electricity__heating_fans_pumps__kwh',
                       'end_use__electricity__mech_vent__kwh'],
    'plug loads': ['end_use__electricity__plug_loads__kwh'],
    'other': ['end_use__electricity__ceiling_fan__kwh', 'end_use__electricity__hot_tub_heater__kwh',
              'end_use__electricity__hot_tub_pump__kwh', 'end_use__electricity__well_pump__kwh']
}

for upgrade_num in upgrade_nums:
    SE.save_enduse_savings(upgrade_num, emm_county, my_run, scout_col2resstock)
