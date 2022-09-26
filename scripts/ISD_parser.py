# -*- coding: utf-8 -*-
"""
Created on Mon May  4 09:24:49 2020

@author: RDCRLDDH
"""
import os
import ish_parser
import gzip
import ftplib
import io
import pandas as pd
import datetime as dt
from pydsstools.heclib.dss import HecDss
from pydsstools.core import TimeSeriesContainer

#possible additional keys
lu = {'AA1':'LIQUID-PRECIP', 
 'AA2':'LIQUID-PRECIP', 
 'AA3':'LIQUID-PRECIP', 
 'AJ1':'SNOW_DEPTH', 
 'AL1':'SNOW-ACCUMULATION', 
 'AU1':'WEATHER-OCCURANCE', 
 'AU2':'WEATHER-OCCURANCE', 
 'AW1':'PRESENT-WEATHER-OBSERVATION', 
 'AW2':'PRESENT-WEATHER-OBSERVATION', 
 'AW3':'PRESENT-WEATHER-OBSERVATION', 
 'GA1':'SKY-COVER-LAYER', 
 'GA2':'SKY-COVER-LAYER', 
 'GA3':'SKY-COVER-LAYER', 
 'GD1':'SKY-COVER-SUMMATION', 
 'GD2':'SKY-COVER-SUMMATION', 
 'GD3':'SKY-COVER-SUMMATION', 
 'GD4':'SKY-COVER-SUMMATION', 
 'GE1':'SKY-CONDITION',
 'GF1':'SKY-CONDITION',
 'KA1':'EXTREME-AIR-TEMPERATURE', 
 'KA2':'EXTREME-AIR-TEMPERATURE', 
 'MA1':'ATMOSPHERIC-PRESSURE', 
 'MD1':'ATMOSPHERIC-PRESSURE-CHANGE', 
 'MW1':'PRESENT-WEATHER-OBS', 
 'MW2':'PRESENT-WEATHER-OBS', 
 'OC1':'WIND-GUST-OBSERVATION', 
 'OD1':'SUPPLEMENTARY-WIND-OBSERVATION',
 'WA1':'PLATFORM-ICE-ACCRETION'}


units = {'HUMIDITY': 'UNIT',
 'WIND-SPEED': 'METER-PER-SEC',
 'WIND-DIRECTION': 'ANGLE',
 'AIR-TEMP': 'degC',
 'SKY-CEILING': 'M',
 'SEA-LEVEL-PRESSURE':'hpA',
 'VISABILITY-DISTANCE':'M',
 'DEW-POINT':'degC',
 'AA1':'mm', 
 'AA2':'mm', 
 'AA3':'mm', 
 'AJ1':'CM', 
 'AL1':'CM', 
 'AU1':'UNIT', 
 'AU2':'UNIT', 
 'AW1':'PRESENT-WEATHER-OBSERVATION', 
 'AW2':'PRESENT-WEATHER-OBSERVATION', 
 'AW3':'PRESENT-WEATHER-OBSERVATION', 
 'GA1':'SKY-COVER-LAYER', 
 'GA2':'SKY-COVER-LAYER', 
 'GA3':'SKY-COVER-LAYER', 
 'GD1':'SKY-COVER-SUMMATION', 
 'GD2':'SKY-COVER-SUMMATION', 
 'GD3':'SKY-COVER-SUMMATION', 
 'GD4':'SKY-COVER-SUMMATION', 
 'GE1':'SKY-CONDITION',
 'GF1':'SKY-CONDITION',
 'KA1':'degC', 
 'KA2':'degC', 
 'MA1':'HECTOPASCALS', 
 'MD1':'HECTOPASCALS', 
 'MW1':'UNIT', 
 'MW2':'UNIT', 
 'OC1':'METER-PER-SECOND', 
 'OD1':'UNIT',
 'WA1':'CM'}

coverage_map = {0.0:0,
                1.0:1/20,
                2.0:2.5/10,
                3.0:4/10,
                4.0:5/10,
                5.0:6/10,
                6.0:7.5/10,
                7.0:9.5/10,
                8.0:1}

def make_data(r):
    out = []

    if isinstance(r.sky_cover, list):
        for num, i in enumerate(r.sky_cover):
            for key in i.keys():
                try:
                    out.append([r.datetime, num, key,i[key].get_numeric(), str(r.report_type)])
                except:
                    pass
    return out


def convert_timezone_to_local(me):
    try:
        me.index = me.index.droplevel(['obs_level','variable'])
    except:
        pass
    me.index = pd.to_datetime(me.index)
    me.index  = me.index.tz_convert('US/Central')
    me.index = pd.DatetimeIndex([i.replace(tzinfo=None) for i in me.index])
    return me
    

def process_sky_cover_variables(reports, name, USAF_ID, WBAN_ID):
    sky_reports = [r for r in reports if r.sky_cover is not None]
     
    sky_dicts = [make_data(r) for r in sky_reports]
    flat_list = [item for sublist in sky_dicts for item in sublist]
    
    foo2 = pd.DataFrame(flat_list, columns = ['date','level','variable','value','rpt_type'])
    lu_observation_level = {0: 'GD1', 1: 'GD2', 2: 'GD3', 3: 'GD4', 4: 'GD5'}
    foo2.loc[:,'obs_level'] = foo2.level.map(lu_observation_level)
    foo2 = foo2.drop('level', axis=1)
    foo2 = foo2.set_index(['date','obs_level'])
    foo2.loc[foo2.variable == 'coverage','value'] = foo2.loc[foo2.variable == 'coverage','value'].map(coverage_map)
    foo2 = foo2.set_index('variable', append=True)
    foo2 = foo2.set_index('rpt_type', append=True)


    #Make sure each record is complete, dont want to mix report types
    sub = foo2.loc[foo2.index.get_level_values('variable').isin(['coverage','base_height']), :]
    counts = sub.groupby(['date','obs_level','rpt_type']).value.count()
    counts = counts[counts==2]
    foo2 = foo2.reset_index('variable')
    foo2 = foo2.loc[counts.index, :]
    foo2 = foo2.set_index('variable', append=True)
    foo2 = foo2.reset_index('rpt_type')

    unit_lu = {'base_height':'M',
           'coverage':'percent',
           'cloud_type':'-'}
    
    out = pd.DataFrame()
    #Loop through all reports
    for (obs_level, variable), data in foo2.groupby(['obs_level', 'variable']):

        try:
            if len(data.loc[data.rpt_type.str.startswith('METAR'), :])>0:
                #Look for metar records
                me = data.loc[data.rpt_type.str.startswith('METAR'), :]
                me = convert_timezone_to_local(me)
                me.index = me.index.round('1H')
                me = me.loc[~me.index.duplicated(keep='first')]
                
                #Resample METAR report to regular time series
                idx = pd.date_range(me.index.min(), me.index.max(), freq='1H',)
                me = me.reindex(idx, method='nearest', tolerance=dt.timedelta(minutes=15)) 
                
                
                #look for other recrods
                others = data.loc[~data.rpt_type.str.startswith('METAR'), :]
                others = convert_timezone_to_local(others)
                others = others.loc[~others.index.duplicated(keep='first')]
                
                #Resample other reports to regular time series
                idx = pd.date_range(others.index.min(), others.index.max(), freq='1H')
                others = others.reindex(idx, method='nearest', tolerance=dt.timedelta(minutes=15)).dropna() 
                
                
                #Find indicies in common
                idx_fill = me.loc[me.value.isna(), :].index
                idx_fill = idx_fill[idx_fill.isin(others.index)]
                me.loc[idx_fill, 'rpt_type'] = others.loc[idx_fill, 'rpt_type']
                me.loc[idx_fill, 'value'] = others.loc[idx_fill, 'value']
                tmp = me.groupby('rpt_type').count()
                
                
                tmp = pd.concat([tmp], keys=[obs_level] , names = ['obs_level'])
                tmp = pd.concat([tmp], keys=[variable] , names = ['variable'])
                tmp.columns = ['count']
        
                out = pd.concat([out, tmp])
                me = me.drop('rpt_type',axis=1)
                me = me.fillna(-901.0)
            
            else:
                
                others = data.loc[~data.rpt_type.str.startswith('METAR'), :]
                others = convert_timezone_to_local(others)
                others = others.loc[~others.index.duplicated(keep='first')]
                
                #Resample other reports to regular time series
                idx = pd.date_range(others.index.min(), others.index.max(), freq='1H')
                others = others.reindex(idx, method='nearest', tolerance=dt.timedelta(minutes=15)).dropna() 
                tmp = others.groupby('rpt_type').count()
                
                
                tmp = pd.concat([tmp], keys=[obs_level] , names = ['obs_level'])
                tmp = pd.concat([tmp], keys=[variable] , names = ['variable'])
                tmp.columns = ['count']
        
                out = pd.concat([out, tmp])
                
                me = others
    
                me = me.drop('rpt_type',axis=1)
                me = me.fillna(-901.0)
                
            #create string for start Date
            start_date =me.index.min().strftime('%d%b%Y %H:%M:%S')
            
            #construct dss pathname using dictionary mappings
            pname = f"/NEBRASKA/{name}/{'CLOUD-COVER-' + variable.upper() +'-' + obs_level}//1HOUR/USAF_{USAF_ID}_WBAN_{WBAN_ID}_METAR/"
            print(pname)
            tsc = TimeSeriesContainer()
            tsc.granularity = 60 #seconds i.e. minute granularity
            tsc.numberValues = me.size
            tsc.startDateTime=start_date
            tsc.pathname = pname
            tsc.units = unit_lu[variable]
            tsc.type = "INST-VAL"
            tsc.interval = 1
            #must a +ve integer for regular time-series
            #actual interval implied from E part of pathname
            tsc.values =me.values.squeeze()
            #values may be list,array, numpy array
        
            #Write DSS time sereis to file
            fid = HecDss.Open(dss_file)
            
            #Optional
            #fid.deletePathname(tsc.pathname)
            status = fid.put(tsc)
            
            #Close the file
            fid.close()
        except:
            pname = f"/NEBRASKA/{name}/{'CLOUD-COVER-' + variable.upper() +'-' + obs_level}//1HOUR/USAF_{USAF_ID}_WBAN_{WBAN_ID}/"
            print(f'!!!!!!!!!!!!!!!!!!!!!!!!!!!!!Could not process {pname}')
    
    return out

def process_mandatory_variables(reports, name, USAF_ID, WBAN_ID):
    foo = pd.DataFrame.from_records(((r.datetime, 
                                      r.air_temperature.get_numeric(), 
                                      r.humidity.get_numeric(), 
                                      r.sky_ceiling.get_numeric(),
                                      r.sea_level_pressure.get_numeric(),
                                      r.visibility_distance.get_numeric(),
                                      r.wind_speed.get_numeric(),
                                      r.dew_point.get_numeric()) for r in reports),
                                    columns=['datetime','AIR-TEMP','HUMIDITY','SKY-CEILING','SEA-LEVEL-PRESSURE','VISABILITY-DISTANCE', 'WIND-SPEED','DEW-POINT'],
                                    index='datetime')
    foo.index = pd.to_datetime(foo.index) 
    
    foo = foo.stack()
    foo.index.names = ['datetime','variable']
    foo.name = 'value'
    
    for variable, group in foo.groupby('variable'):
        #Drop variable name from index
        group.index = group.index.droplevel(1)
          
        group.index = pd.to_datetime(group.index)
        
        group = convert_timezone_to_local(group)
        
        group = group.loc[~group.index.duplicated(keep='first')]
        

        idx = pd.date_range(group.index.min(), group.index.max(), freq='1H',)
        
        group = group.sort_index()
        group = group.reindex(idx, method='nearest', tolerance=dt.timedelta(minutes=15), fill_value=-901.0)
    
        #create string for start Date
        start_date =group.index.min().strftime('%d%b%Y %H:%M:%S')
        
        #construct dss pathname using dictionary mappings
        pname = f'/NEBRASKA/{name}/{variable}//1HOUR/USAF_{USAF_ID}_WBAN_{WBAN_ID}/'
        print(pname)
        tsc = TimeSeriesContainer()
        tsc.granularity = 60 #seconds i.e. minute granularity
        tsc.numberValues = group.size
        tsc.startDateTime=start_date
        tsc.pathname = pname
        tsc.units = units[variable]
        tsc.type = "INST-VAL"
        tsc.interval = 1
        #must a +ve integer for regular time-series
        #actual interval implied from E part of pathname
        tsc.values =group.values
        #values may be list,array, numpy array
    
        #Write DSS time sereis to file
        fid = HecDss.Open(dss_file)
        
        #Optional
        #fid.deletePathname(tsc.pathname)
        status = fid.put(tsc)
        fid.close()


def get_isd_reports(USAF_ID, WBAN_ID, YEARS):
    ftp_host = "ftp.ncdc.noaa.gov"
    parser = ish_parser.ish_parser()
    
    with ftplib.FTP(host=ftp_host) as ftpconn:
        ftpconn.login()
    
        for year in YEARS:
            ftp_file = "pub/data/noaa/{YEAR}/{USAF}-{WBAN}-{YEAR}.gz".format(USAF=USAF_ID, WBAN=WBAN_ID, YEAR=year)
            print(ftp_file)
            local_file = r"data\isd_download\{USAF}-{WBAN}-{YEAR}.gz".format(USAF=USAF_ID, WBAN=WBAN_ID, YEAR=year)
            if not os.path.exists(local_file):
                
                # read the whole file and save it to a BytesIO (stream)
                response = io.BytesIO()
                try:
                    ftpconn.retrbinary('RETR '+ftp_file, response.write)
                except ftplib.error_perm as err:
                    if str(err).startswith('550 '):
                        print('ERROR:', err)
                    else:
                        raise
                        
                try:
                    with open(r"data\isd_download\{USAF}-{WBAN}-{YEAR}.gz".format(USAF=USAF_ID, WBAN=WBAN_ID, YEAR=year), 'wb') as myfile:
                        ftpconn.retrbinary('RETR '+ftp_file, myfile.write)
                except:
                    pass
        
                    # decompress and parse each line 
                    response.seek(0) # jump back to the beginning of the stream
                
                
                with gzip.open(response, mode='rb') as gzstream:
                    for line in gzstream:
                        parser.loads(line.decode('latin-1'))
            else:
                with gzip.open(local_file, mode='rb') as gzstream:
                    for line in gzstream:
                        parser.loads(line.decode('latin-1'))
                        
    # get the list of all reports
    reports = parser.get_reports()
    print(len(reports), "records")   
    return reports





def main(name, USAF_ID, WBAN_ID, year_start):
    
    if year_start < 1950:
        YEARS = range(1940, 1950) # so 1980, 1981, 1982
        reports = get_isd_reports(USAF_ID, WBAN_ID, YEARS)
        #Get all METAR Reports
        me = [r for r in reports if r.report_type =='METAR Aviation routine weather report']
        if len(reports)>0:
            process_mandatory_variables(reports, name, USAF_ID, WBAN_ID)
            process_sky_cover_variables(me, name, USAF_ID, WBAN_ID)

    if year_start < 1960:    
        YEARS = range(1950, 1960) # so 1980, 1981, 1982
        reports = get_isd_reports(USAF_ID, WBAN_ID, YEARS)
        #Get all METAR Reports
        me = [r for r in reports if r.report_type =='Airways report (includes record specials)']
        if len(reports)>0:
            process_mandatory_variables(reports, name, USAF_ID, WBAN_ID)
            process_sky_cover_variables(me, name, USAF_ID, WBAN_ID)
    if year_start < 1970:        
        YEARS = range(1960, 1970) # so 1980, 1981, 1982
        reports = get_isd_reports(USAF_ID, WBAN_ID, YEARS)
        #Get all METAR Reports
        me = [r for r in reports if r.report_type =='Airways report (includes record specials)']
        if len(reports)>0:
            process_mandatory_variables(reports, name, USAF_ID, WBAN_ID)
            process_sky_cover_variables(me, name, USAF_ID, WBAN_ID)
    if year_start < 1980:    
        YEARS = range(1970, 1980) # so 1980, 1981, 1982
        reports = get_isd_reports(USAF_ID, WBAN_ID, YEARS)
        #Get all METAR Reports
        me = [r for r in reports if r.report_type =='Airways report (includes record specials)']
        if len(reports)>0:
            process_mandatory_variables(reports, name, USAF_ID, WBAN_ID)
            process_sky_cover_variables(me, name, USAF_ID, WBAN_ID)
    
    if year_start < 1990:    
        YEARS = range(1980, 1990) # so 1980, 1981, 1982
        reports = get_isd_reports(USAF_ID, WBAN_ID, YEARS)
        #Get all METAR Reports
        me = [r for r in reports if r.report_type =='Airways report (includes record specials)']
        if len(reports)>0:
            process_mandatory_variables(reports, name, USAF_ID, WBAN_ID)
            process_sky_cover_variables(me, name, USAF_ID, WBAN_ID)
    
    if year_start < 2000:
        YEARS = range(1990, 2000) # so 1980, 1981, 1982
        reports = get_isd_reports(USAF_ID, WBAN_ID, YEARS)
        #Get all METAR Reports
        me = [r for r in reports if r.report_type =='Airways report (includes record specials)']
        if len(reports)>0:
            process_mandatory_variables(reports, name, USAF_ID, WBAN_ID)
            process_sky_cover_variables(me, name, USAF_ID, WBAN_ID)
        
    if year_start < 2010:    
        YEARS = range(2000, 2010) # so 1980, 1981, 1982
        reports = get_isd_reports(USAF_ID, WBAN_ID, YEARS)

        if len(reports)>0:
            process_mandatory_variables(reports, name, USAF_ID, WBAN_ID)
            process_sky_cover_variables(reports, name, USAF_ID, WBAN_ID)
    
    if year_start < 2021:
        YEARS = range(2010, 2021) # so 1980, 1981, 1982
        reports = get_isd_reports(USAF_ID, WBAN_ID, YEARS)
        
        if len(reports)>0:
            process_mandatory_variables(reports, name, USAF_ID, WBAN_ID)
            out = process_sky_cover_variables(reports, name, USAF_ID, WBAN_ID)

if __name__ == '__main__':

    dss_file = r"data\Nebraska_ISD.dss"
    fid = HecDss.Open(dss_file,version=6)
    fid.close()
    
    
    
    #######################################################################################################################################
    #                   'North Platte Regional AP'
    
    
    name = 'North Platte Regional AP'.upper()
    

    USAF_ID = '725620'
    WBAN_ID = '24023'
    year_start = 1960
    year_end = 2021

    main(name, USAF_ID, WBAN_ID, year_start)
    
    
    




