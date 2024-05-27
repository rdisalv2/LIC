# data cleaning for Long Island City development analysis
# This script loads the 2009 and 2024 New York City property tax rolls,
# collects required variables, and stacks them.
# the raw files for this script are not shared, but the cleaned
# the data is exported as parquet at the end of the script.
# the parquet file is shared (on google drive) and used in subsequent scripts in the project.

import pandas as pd
import numpy as np
import os

# clean fy24 taxroll assessment data ------------

os.chdir("C:/Users/Owner/Dropbox/Proj/propertytax/data/nyc-proptax-rolls/fy24/fy24_avroll1234")

tmp = pd.read_csv('fy24_avroll1234.txt',sep='\t',header=None,low_memory=False)

codebook = pd.read_excel('layout-pts-property-master.xlsx',skiprows=4,sheet_name='Property Master Layout')

codebook = codebook.loc[~(codebook['PTS Field Name'].isin(['PYTXBLAND','TENTXBLAND','CBNTXBLAND','FINTXBLAND','CURTXBLAND'])),:]
codebook = codebook.loc[~pd.isna(codebook['PTS Field Name']),:]

tmp.columns = codebook['PTS Field Name'].tolist() + ['junk']

del tmp['junk']

# "I consider number of properties, number of units, full market value, and total building square footage, by tax class."
# keep these variables only:
tmp['YEAR'].value_counts()

fy2024taxrolls = tmp.loc[:,['BORO','BLOCK','LOT','EASE','CURTAXCLASS','CURMKTTOT','UNITS','BLDG_CLASS','GROSS_SQFT','ZIP_CODE','COOP_NUM','CONDO_Number']].copy()


# clean fy09 taxroll assessment data -----------
os.chdir("C:/Users/Owner/Dropbox/Proj/propertytax/data/nyc-proptax-rolls/fy09/")
# these were converted to csv files from mdb files using the export function in microsoft access. (right clicked the table name in microsoft access, then export > text file, then selected the right dialogue options for csv - including clicking the box to export column headers)
tc1 = pd.read_csv('tc1.csv',low_memory=False)
tc234 = pd.read_csv('tc234.csv',low_memory=False)
fy2009 = pd.concat([tc1,tc234],axis=0)
fy2009['YEAR4'].value_counts(dropna=False) # hm this says year4 = 2007? confusing
fy2009['TXCL'].value_counts(dropna=False)

fy2009taxrolls = fy2009.loc[:,['BORO','BLOCK','LOT','EASE','TOT_UNIT','CUR_FV_T','TXCL','BLDGCL','GR_SQFT','ZIP','COOP_NUM','CONDO_NM']].copy()


# combine the two datasets, keeping only necessary variables ------

fy2009taxrolls.rename(columns = {'TOT_UNIT':'UNITS','CUR_FV_T':'FMV','TXCL':'TC','BLDGCL':'BC','GR_SQFT':'SQFT'},inplace=True)
fy2024taxrolls.rename(columns = {'CURTAXCLASS':'TC','CURMKTTOT':'FMV','BLDG_CLASS':'BC','GROSS_SQFT':'SQFT','ZIP_CODE':'ZIP'},inplace=True)

fy2009taxrolls['is_coop_or_condo'] = ((fy2009taxrolls['COOP_NUM']>0) & (~pd.isna(fy2009taxrolls['COOP_NUM']))) | ((fy2009taxrolls['CONDO_NM']>0) & (~pd.isna(fy2009taxrolls['CONDO_NM'])))
fy2024taxrolls['CONDO_Number'] = pd.to_numeric(fy2024taxrolls['CONDO_Number'],errors='coerce',downcast='integer')
fy2024taxrolls['is_coop_or_condo'] = ((fy2024taxrolls['COOP_NUM']>0) & (~pd.isna(fy2024taxrolls['COOP_NUM']))) | ((fy2024taxrolls['CONDO_Number']>0) & (~pd.isna(fy2024taxrolls['CONDO_Number'])))


fy2009taxrolls['is_condo'] = (fy2009taxrolls['CONDO_NM']>0) & (~pd.isna(fy2009taxrolls['CONDO_NM']))
fy2024taxrolls['is_condo'] = (fy2024taxrolls['CONDO_Number']>0) & (~pd.isna(fy2024taxrolls['CONDO_Number']))

fy2009taxrolls['is_coop'] = (fy2009taxrolls['COOP_NUM']>0) & (~pd.isna(fy2009taxrolls['COOP_NUM']))
fy2024taxrolls['is_coop'] = (fy2024taxrolls['COOP_NUM']>0) & (~pd.isna(fy2024taxrolls['COOP_NUM']))


del fy2009taxrolls['COOP_NUM']
del fy2009taxrolls['CONDO_NM']
del fy2024taxrolls['CONDO_Number']
del fy2024taxrolls['COOP_NUM']

fy2009taxrolls['fy'] = 2009
fy2024taxrolls['fy'] = 2024


taxrolls = pd.concat([fy2024taxrolls,fy2009taxrolls],axis=0)
taxrolls.reset_index(inplace=True,drop=True)


taxrolls['TCfull'] = taxrolls['TC']
taxrolls['TC'] = taxrolls['TC'].str[0:1]

taxrolls['ZIP'] = taxrolls['ZIP'].astype(str)


# export to parquet for subsequent analysis----------
os.chdir(r"C:\Users\Owner\Dropbox\Proj\LIC_Development")
taxrolls.to_parquet('taxrolls0924.parquet.gz', compression='gzip', index=False)
