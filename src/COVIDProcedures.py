# -*- coding: utf-8 -*-
"""
Created on Mon Sep 14 20:25:51 2020

@author: Nouman
"""

import numpy as np
import pandas as pd
import datetime
import time
import os
import us_state_abbrev as stateabbrev
import sys
import EVHelper as EVHelper
import sympy
import calendar
# import praw
import asyncio
import configparser


MasterDataFrame = pd.DataFrame()
casesByPHU = pd.DataFrame()
casesByAge = pd.DataFrame()
changeInCasesByAge = pd.DataFrame()
changeInReportingLag = pd.DataFrame()
changeInDeathsByPHU = pd.DataFrame()
pd.options.display.float_format = '{:.2f}'.format
TODAYS_DATE_GLOBAL = datetime.datetime.now()
FOLDER_LOCATION_INI_FILE = '.config/folder_location.ini'
config = configparser.ConfigParser()
config.read(FOLDER_LOCATION_INI_FILE)


###########################################
###########################################

def PostDailyReports():
    """
    Post daily reports to reddit using the praw module

    Returns
    -------
    None.

    """
    starttime = time.time()
    import praw as praw
    print('------------------------------------------------------------------------')
    print(f'PostDailyReports \nStarted: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}')

    PostTitleFileName = 'TextOutput/PostTitle.txt'
    config = configparser.ConfigParser()
    config.read('.config/auth.ini')

    user_agent = config.get('reddit_credentials', 'user_agent')
    client_id = config.get('reddit_credentials', 'client_id')
    client_secret = config.get('reddit_credentials', 'client_secret')
    username = config.get('reddit_credentials', 'username')
    password = config.get('reddit_credentials', 'password')

    reddit = praw.Reddit(
        user_agent=user_agent, client_id=client_id, client_secret=client_secret,
        username=username, password=password)

    fileObject = open(PostTitleFileName, "r", encoding='utf-8')
    title = fileObject.read()

    fileObject = open("TextFileOutput.txt", "r", encoding='utf-8')
    selftext = fileObject.read()

    # ###########################################
    # #Post to user page
    # subreddit = reddit.subreddit(f"u_{username}")
    # subreddit.submit(title,selftext=selftext)
    # print(f"Posted to u_{username}')
    # time.sleep(5)

    ###########################################
    # #Get all flair submissions
    # submission.flair.choices()

    ###########################################
    # Post to ontario
    subreddit = reddit.subreddit('ontario')
    subreddit.submit(title, selftext=selftext, flair_id='94683316-f375-11ea-ab68-0ef1b69c9181',
                     send_replies=False)
    print('Posted to r/ontario')
    time.sleep(3)
    ###########################################
    # Post to CanadaCoronavirus
    subreddit = reddit.subreddit('CanadaCoronavirus')
    subreddit.submit(title, selftext=selftext, flair_id='bcce6d7a-5bf9-11ea-84ac-0e46e250f275',
                     send_replies=False)
    print('Posted to r/CanadaCoronavirus')
    ###########################################

    endtime = datetime.datetime.now()
    print(f"Ended:   {endtime:%Y-%m-%d %H:%M:%S} {round(time.time() - starttime, 2)} seconds")
    print('------------------------------------------------------------------------')


def DownloadFile(filename=f"{datetime.datetime.now():%Y-%m-%d}"):
    """
    This is the main method that generates the daily reports.

    Parameters
    ----------
    filename : string
        This is just today's current date if not specified. If specified it needs to be in the
        2022-12-31 format.

    Returns
    -------
    None.

    """
    starttime = time.time()
    GlobalData()
    # CanadaData()
    # OntarioZones()
    print('Downloading cases file:')
    df = pd.read_csv('https://data.ontario.ca/dataset/f4112442-bdc8-45d2-be3c-12efae72fb27/resource/455fd63b-603d-4608-8216-7d8647f43350/download/conposcovidloc.csv')

    config = configparser.ConfigParser()
    config.read('.config/folder_location.ini')

    path = config.get('folder_location', 'confirmed_cases_file')
    df.to_csv(os.path.join(path, (filename + ' Data.csv')), index=False)
    del df
    print('File downloaded (seconds):', round(time.time() - starttime))

    LoadCOVIDData(filename)
    # DailyReports()
    DailyReports_Individual(filename)
    # input('Pausing: ')
    # DailyReportExtraction(filename)
    VaccineData()

    # HospitalMetrics()
    # DeathProjection()
    if TODAYS_DATE_GLOBAL.isoweekday() in [5, 6]:
        RTData()
        TestingData()

    JailData()
    COVIDAppData()
    OutbreakData()
    # LTCData()
    icu_capacity_stats()
    DailyReports_Compile()
    COVIDCharts()

    if TODAYS_DATE_GLOBAL.isoweekday() in [6]:
        TorontoCOVID()


def DailyReports_Individual(FileNameIn):
    starttime = time.time()
    # ConsoleOut = sys.stdout

    DeathDetailFileName = 'TextOutput/DeathDetail.txt'

    config = configparser.ConfigParser()
    config.read('.config/folder_location.ini')
    SourcePath = config.get('folder_location', 'indiv_pickles')

    FileNameToday = FileNameIn + ' - ' + 'Source.pickle'
    TodaysDate = datetime.datetime(int(FileNameIn[0:4]), int(FileNameIn[5:7]),
                                   int(FileNameIn[8:10]))
    YesterdaysDate = TodaysDate - datetime.timedelta(days=1)

    MasterDataFrame = pd.read_pickle(SourcePath + '\\' + FileNameIn + ' - Source.pickle')
    YesterdaysDataFrame = pd.read_pickle(SourcePath + '\\' + YesterdaysDate.strftime("%Y-%m-%d")
                                         + ' - Source.pickle')
    MasterDataFrame = pd.concat([MasterDataFrame, YesterdaysDataFrame], ignore_index=True)
    MasterDataFrame['Outbreak_Related'] = MasterDataFrame['Outbreak_Related'].fillna('No')
    del YesterdaysDataFrame

    ###############################################################################################
    ###############################################################################################
    # Done
    "Case count by PHU"
    casesByPHU = pd.pivot_table(MasterDataFrame, values='Row_ID',
                                index=['Reporting_PHU'], columns='File_Date',
                                aggfunc=np.count_nonzero)
    casesByPHU = casesByPHU.reindex(columns=sorted(casesByPHU.columns, reverse=True))
    casesByPHU.sort_values(by=casesByPHU.columns[0], ascending=False, inplace=True)
    casesByPHU.to_pickle('PickleNew/CasesByPHU.pickle')

    "Change in Case count by PHU"
    changeInCasesByPHU = (casesByPHU - casesByPHU.shift(-1, axis=1))
    if os.path.exists('PickleNew/ChangeInCasesByPHU.pickle'):
        tempDF = pd.read_pickle('PickleNew/ChangeInCasesByPHU.pickle')
        tempDF = tempDF.merge(changeInCasesByPHU[TodaysDate], left_index=True, right_index=True,
                              how='outer', suffixes=('drop', None))
        tempDF = tempDF.drop([col for col in tempDF.columns if 'drop' in str(col)], axis=1)
        changeInCasesByPHU = tempDF

    changeInCasesByPHU = changeInCasesByPHU.fillna(0)
    changeInCasesByPHU = changeInCasesByPHU.reindex(columns=sorted(changeInCasesByPHU.columns,
                                                                   reverse=True))
    changeInCasesByPHU = changeInCasesByPHU.reindex(casesByPHU.index)

    changeInCasesByPHU.insert(0, 'TotalToDate', casesByPHU[casesByPHU.columns[0]])
    changeInCasesByPHU.to_pickle('PickleNew/ChangeInCasesByPHU-Display.pickle')

    changeInCasesByPHU.drop(['TotalToDate'], axis=1, inplace=True)
    changeInCasesByPHU = changeInCasesByPHU.sort_values(by=changeInCasesByPHU.columns[0],
                                                        ascending=False, inplace=False)
    changeInCasesByPHU = changeInCasesByPHU.fillna(0)
    changeInCasesByPHU.to_pickle('PickleNew/ChangeInCasesByPHU.pickle')
    del changeInCasesByPHU

    ############################################################################################
    ############################################################################################

    AgesByPHUYesterday = pd.pivot_table(MasterDataFrame[(MasterDataFrame['File_Date']
                                                         == MasterDataFrame['File_Date'].max() - pd.DateOffset(days=1))],
                                        values='Row_ID', index=['Reporting_PHU'], columns='Age_Group',
                                        aggfunc=np.count_nonzero, margins=False).fillna(0)
    AgesByPHUToday = pd.pivot_table(MasterDataFrame[(MasterDataFrame['File_Date']
                                                     == MasterDataFrame['File_Date'].max())],
                                    values='Row_ID', index=['Reporting_PHU'], columns='Age_Group',
                                    aggfunc=np.count_nonzero, margins=False).fillna(0)
    changeInAgesByPHUToday = AgesByPHUToday - AgesByPHUYesterday
    changeInAgesByPHUWeek = (pd.pivot_table(MasterDataFrame[(MasterDataFrame['File_Date']
                                                             == MasterDataFrame['File_Date'].max())],
                                            values='Row_ID', index=['Reporting_PHU'], columns='Age_Group',
                                            aggfunc=np.count_nonzero, margins=False).fillna(0)
                             - pd.pivot_table(MasterDataFrame[(MasterDataFrame['File_Date']
                                                               == MasterDataFrame['File_Date'].max()
                                                               - pd.DateOffset(days=7))], values='Row_ID',
                                              index=['Reporting_PHU'], columns='Age_Group',
                                              aggfunc=np.count_nonzero, margins=False).fillna(0)).fillna(0)
    changeInAgesByPHUWeek.rename(columns={'19 & under': '<20', 'Unknown': 'N/A'}, inplace=True)

    ############################################################################################
    ############################################################################################
    "Case count by Age"
    casesByAge = pd.pivot_table(MasterDataFrame, values='Row_ID', index=['Age_Group'], columns='File_Date', aggfunc=np.count_nonzero)
    casesByAge = casesByAge.reindex(columns=sorted(casesByAge.columns, reverse=True))
    casesByAge.sort_index(ascending=True, inplace=True)

    changeInCasesByAge = casesByAge - casesByAge.shift(-1, axis=1)
    if os.path.exists('PickleNew/ChangeInCasesByAge.pickle'):
        tempDF = pd.read_pickle('PickleNew/ChangeInCasesByAge.pickle')
        tempDF = tempDF.merge(changeInCasesByAge[TodaysDate], left_index=True, right_index=True, how='outer', suffixes=('drop', None))
        tempDF = tempDF.drop([col for col in tempDF.columns if 'drop' in str(col)], axis=1)
        changeInCasesByAge = tempDF

    changeInCasesByAge = changeInCasesByAge.fillna(0)
    changeInCasesByAge = changeInCasesByAge.drop('TotalToDate', axis=1)

    changeInCasesByAge = changeInCasesByAge.reindex(columns=sorted(changeInCasesByAge.columns, reverse=True))
    changeInCasesByAge = changeInCasesByAge.reindex(casesByAge.index)

    changeInCasesByAge.insert(0, 'TotalToDate', casesByAge[casesByAge.columns[0]])
    changeInCasesByAge.to_pickle('PickleNew/ChangeInCasesByAge.pickle')
    print(round((time.time() - starttime), 2), '- Case count by age done')
    del changeInCasesByAge
    ############################################################################################
    ############################################################################################
    #Done
    "death count by PHU"
    deathsByPHU = pd.pivot_table(MasterDataFrame[(MasterDataFrame['Outcome']=="Fatal")],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'File_Date',aggfunc=np.count_nonzero)
    deathsByPHU = deathsByPHU.reindex(columns=sorted(deathsByPHU.columns,reverse = True))
    deathsByPHU.sort_values(by=deathsByPHU.columns[0],ascending = False, inplace = True)

    "Change in death count by PHU"
    changeInDeathsByPHU = deathsByPHU-deathsByPHU.shift(-1,axis=1)
    if os.path.exists('PickleNew/changeInDeathsByPHU.pickle'):
        tempDF = pd.read_pickle('PickleNew/changeInDeathsByPHU.pickle')
        tempDF = tempDF.merge(changeInDeathsByPHU[TodaysDate],left_index = True, right_index = True, how = 'outer',suffixes= ('drop',None) )
        tempDF = tempDF.drop([col for col in tempDF.columns if 'drop' in str(col)],axis = 1)
        changeInDeathsByPHU = tempDF

    changeInDeathsByPHU = changeInDeathsByPHU.fillna(0)
    changeInDeathsByPHU = changeInDeathsByPHU.drop('TotalToDate',axis=1)

    changeInDeathsByPHU = changeInDeathsByPHU.reindex(columns=sorted(changeInDeathsByPHU.columns,reverse = True))
    changeInDeathsByPHU = changeInDeathsByPHU.reindex(deathsByPHU.index)

    changeInDeathsByPHU.insert(0,'TotalToDate',deathsByPHU[deathsByPHU.columns[0]])
    changeInDeathsByPHU.to_pickle('PickleNew/changeInDeathsByPHU.pickle')

    print(round((time.time()-starttime),2),'- Death count by PHU done')


    ############################################################################################
    ############################################################################################
    #Done
    "Case count by Outcome"
    casesByOutcome = pd.pivot_table(MasterDataFrame,values = 'Row_ID',index = ['Outcome'],columns = 'File_Date',aggfunc=np.count_nonzero)
    casesByOutcome = casesByOutcome.reindex(columns=sorted(casesByOutcome.columns,reverse = True))
    casesByOutcome.sort_index(ascending = True, inplace = True)
    casesByOutcome.fillna(0, inplace = True)

    "Change in Case count by Outcome"
    changeInCasesByOutcome = casesByOutcome-casesByOutcome.shift(-1,axis=1)
    if os.path.exists('PickleNew/ChangeInCasesByOutcome.pickle'):
        tempDF = pd.read_pickle('PickleNew/ChangeInCasesByOutcome.pickle')
        tempDF = tempDF.merge(changeInCasesByOutcome[TodaysDate],left_index = True, right_index = True, how = 'outer',suffixes= ('drop',None) )
        tempDF = tempDF.drop([col for col in tempDF.columns if 'drop' in str(col)],axis = 1)
        changeInCasesByOutcome = tempDF

    changeInCasesByOutcome = changeInCasesByOutcome.fillna(0)
    changeInCasesByOutcome = changeInCasesByOutcome.drop('TotalToDate',axis=1)
    changeInCasesByOutcome = changeInCasesByOutcome.reindex(columns=sorted(changeInCasesByOutcome.columns,reverse = True))
    changeInCasesByOutcome = changeInCasesByOutcome.reindex(casesByOutcome.index)


    # changeInCasesByOutcome.to_csv('CSVNew/ChangeInCasesByOutcome.csv')
    changeInCasesByOutcome.insert(0,'TotalToDate',casesByOutcome[casesByOutcome.columns[0]])
    changeInCasesByOutcome.to_pickle('PickleNew/ChangeInCasesByOutcome.pickle')

    print(round((time.time()-starttime),2),'- Change in cases by outcome done')

    ############################################################################################
    ############################################################################################
    #Done
    "ActiveCaseByPHU"
    activeCasesByPHU = pd.pivot_table(MasterDataFrame[(MasterDataFrame['Outcome']=="Not Resolved")],
                                      values = 'Row_ID',index = ['Reporting_PHU'],columns = 'File_Date',
                                      aggfunc=np.count_nonzero)
    activeCasesByPHU = activeCasesByPHU.reindex(columns=sorted(activeCasesByPHU.columns,reverse = True))
    activeCasesByPHU = activeCasesByPHU.fillna(0)

    if os.path.exists('PickleNew/ActiveCasesByPHU.pickle'):
        tempDF = pd.read_pickle('PickleNew/ActiveCasesByPHU.pickle')
        tempDF = tempDF.merge(activeCasesByPHU[TodaysDate],left_index = True, right_index = True, how = 'outer',suffixes= ('drop',None) )
        tempDF = tempDF.drop([col for col in tempDF.columns if 'drop' in str(col)],axis = 1)
        activeCasesByPHU = tempDF
    activeCasesByPHU =activeCasesByPHU.fillna(0)

    activeCasesByPHU = activeCasesByPHU.reindex(columns=sorted(activeCasesByPHU.columns, reverse=True))
    activeCasesByPHU.sort_values(by=activeCasesByPHU.columns[0], ascending=False, inplace=True)
    activeCasesByPHU = activeCasesByPHU.drop(datetime.datetime(2022, 3, 11), axis=1,
                                             errors='ignore')


    activeCasesByPHU.to_pickle('PickleNew/ActiveCasesByPHU.pickle')




    "ChangeActiveCaseByPHU"
    changeInActiveCasesByPHU = activeCasesByPHU - activeCasesByPHU.shift(-1,axis=1)
    if (os.path.exists('PickleNew/ChangeInActiveCasesByPHU.pickle')
        and not (TodaysDate in [datetime.datetime(2022, 3, 11), datetime.datetime(2022, 3, 12)])):
        tempDF = pd.read_pickle('PickleNew/ChangeInActiveCasesByPHU.pickle')
        tempDF = tempDF.merge(changeInActiveCasesByPHU[TodaysDate],left_index = True, right_index = True, how = 'outer',suffixes= ('drop',None) )
        tempDF = tempDF.drop([col for col in tempDF.columns if 'drop' in str(col)],axis = 1)
        changeInActiveCasesByPHU = tempDF
    changeInActiveCasesByPHU =changeInActiveCasesByPHU.fillna(0)
    if 'TotalToDate' in changeInActiveCasesByPHU.columns:
        changeInActiveCasesByPHU = changeInActiveCasesByPHU.drop('TotalToDate',axis=1)


    changeInActiveCasesByPHU = changeInActiveCasesByPHU.reindex(columns=sorted(changeInActiveCasesByPHU.columns,reverse = True))
    changeInActiveCasesByPHU = changeInActiveCasesByPHU.reindex(activeCasesByPHU.index)
    #changeInActiveCasesByPHU.to_csv('CSVNew/ChangeInActiveCasesByPHU.csv')

    changeInActiveCasesByPHU.insert(0,'TotalToDate',activeCasesByPHU[activeCasesByPHU.columns[0]])
    changeInActiveCasesByPHU.to_pickle('PickleNew/ChangeInActiveCasesByPHU.pickle')

    print(round((time.time()-starttime),2),'- Active cases by PHU done')

    ############################################################################################
    ############################################################################################
    #Done
    "Case count by Outcome by Age"
    #casesByOutcomeByAge = pd.pivot_table(MasterDataFrame,values = 'Row_ID',index = ['Outcome','Age_Group'],columns = 'File_Date',aggfunc=np.count_nonzero)
    casesByOutcomeByAge = pd.pivot_table(MasterDataFrame,values = 'Row_ID',
                                         index = ['Outbreak_Related','Outcome','Age_Group'],
                                         columns='File_Date', aggfunc=np.count_nonzero)
    casesByOutcomeByAge = casesByOutcomeByAge.reindex(columns=sorted(casesByOutcomeByAge.columns,
                                                                     reverse=True))
    casesByOutcomeByAge.sort_index(ascending=True, inplace=True)
    casesByOutcomeByAge.fillna(0, inplace=True)

    if os.path.exists('PickleNew/casesByOutcomeByAge.pickle'):
        tempDF = pd.read_pickle('PickleNew/casesByOutcomeByAge.pickle')
        tempDF = tempDF.merge(casesByOutcomeByAge[TodaysDate],left_index = True,
                              right_index = True, how = 'outer',suffixes= ('drop',None) )
        tempDF = tempDF.drop([col for col in tempDF.columns if 'drop' in str(col)],axis = 1)
        casesByOutcomeByAge = tempDF
    casesByOutcomeByAge =casesByOutcomeByAge.fillna(0)
    casesByOutcomeByAge = casesByOutcomeByAge.reindex(columns=sorted(casesByOutcomeByAge.columns,reverse = True))

    casesByOutcomeByAge = casesByOutcomeByAge.drop(datetime.datetime(2022,3,11), axis=1,
                                                   errors='ignore')
    casesByOutcomeByAge.to_pickle('PickleNew/casesByOutcomeByAge.pickle')


    print(round((time.time()-starttime),2),'- Cases by outcome by age done')


    "Case count by Outcome by Age"
    changeInCasesByOutcomeByAge = casesByOutcomeByAge - casesByOutcomeByAge.shift(-1,axis=1)
    #changeInCasesByOutcomeByAge.to_csv('CSVNew/changeInCasesByOutcomeByAge.csv')
    changeInCasesByOutcomeByAge.insert(0,'TotalToDate',casesByOutcomeByAge[casesByOutcomeByAge.columns[0]])

    changeInCasesByOutcomeByAge.to_pickle('PickleNew/changeInCasesByOutcomeByAge.pickle')

    if TodaysDate >= datetime.datetime(2020,6,17):

        casesByOutcomeByAgeByOutbreakStatus = pd.pivot_table(MasterDataFrame[(MasterDataFrame['Outcome']=='Not Resolved') & (MasterDataFrame['File_Date']>= datetime.datetime(2020,6,17))],values = 'Row_ID',index = ['Outcome','Age_Group','Case_AcquisitionInfo'],columns = 'File_Date',aggfunc=np.count_nonzero)

        activeCases70PlusOutbreakStatus = pd.DataFrame([casesByOutcomeByAgeByOutbreakStatus.loc[('Not Resolved',(['70s','80s','90s']),['Travel','Community','Close contact'])].sum(),casesByOutcomeByAgeByOutbreakStatus.loc[('Not Resolved',(['70s','80s','90s']),'Outbreak')].sum()],index=['Other','Outbreak'])
        activeCases70PlusOutbreakStatus = activeCases70PlusOutbreakStatus.reindex(columns=sorted(activeCases70PlusOutbreakStatus.columns,reverse = True))


        if os.path.exists('PickleNew/Active70PlusOutbreakStatus.pickle'):
            tempDF = pd.read_pickle('PickleNew/Active70PlusOutbreakStatus.pickle')
            tempDF = tempDF.merge(activeCases70PlusOutbreakStatus[TodaysDate],left_index = True, right_index = True, how = 'outer',suffixes= ('drop',None) )
            tempDF = tempDF.drop([col for col in tempDF.columns if 'drop' in str(col)],axis = 1)
            activeCases70PlusOutbreakStatus = tempDF
        activeCases70PlusOutbreakStatus =activeCases70PlusOutbreakStatus.fillna(0)
        activeCases70PlusOutbreakStatus = activeCases70PlusOutbreakStatus.reindex(columns=sorted(activeCases70PlusOutbreakStatus.columns,reverse = True))
        activeCases70PlusOutbreakStatus.to_pickle('PickleNew/Active70PlusOutbreakStatus.pickle')


    print(round((time.time()-starttime),2),'- Case count by outcome by age done')

    ############################################################################################
    ############################################################################################
    #Done
    "Case count by Source"
    "MasterDataFrame = pd.MultiIndex.from_frame(MasterDataFrame)"
    casesBySource = pd.pivot_table(MasterDataFrame,values = 'Row_ID',index = ['Case_AcquisitionInfo'],columns = 'File_Date',aggfunc=np.count_nonzero, margins = False)
    casesBySource.sort_values(by=['Case_AcquisitionInfo'],ascending =[True],inplace = True)
    casesBySource = casesBySource.reindex(columns=sorted(casesBySource.columns,reverse = True))
    casesBySource.fillna(0,inplace=True)

    changeInCasesBySource = casesBySource - casesBySource.shift(-1,axis=1)

    if os.path.exists('PickleNew/changeInCasesBySource.pickle'):
        tempDF = pd.read_pickle('PickleNew/changeInCasesBySource.pickle')
        tempDF = tempDF.merge(changeInCasesBySource[TodaysDate],left_index = True, right_index = True, how = 'outer',suffixes= ('drop',None) )
        tempDF = tempDF.drop([col for col in tempDF.columns if 'drop' in str(col)],axis = 1)
        changeInCasesBySource = tempDF
    changeInCasesBySource =changeInCasesBySource.fillna(0)
    changeInCasesBySource = changeInCasesBySource.drop('TotalToDate',axis=1)

    changeInCasesBySource = changeInCasesBySource.reindex(columns=sorted(changeInCasesBySource.columns,reverse = True))


    #changeInCasesBySource.to_csv('CSVNew/changeInCasesBySource.csv')

    changeInCasesBySource.insert(0,'TotalToDate',casesBySource[casesBySource.columns[0]])
    changeInCasesBySource.to_pickle('PickleNew/changeInCasesBySource.pickle')

    print(round((time.time()-starttime),2),'- Case count by source done')


    ############################################################################################
    ############################################################################################
    "Fatality rate tables"
    TableFileName = 'TextOutput/FatalityRateTable.txt'


    df = pd.read_pickle('PickleNew/changeInCasesByOutcomeByAge.pickle')
    df = df.drop('TotalToDate',axis=1)
    Rolling30DayOutbreak_Fatal = df.loc['Yes'].loc['Fatal'].T[::-1].rolling(30).sum().fillna(0)[::-1].T
    Rolling30DayOutbreak_Resolved = df.loc['Yes'].loc['Resolved'].T[::-1].rolling(30).sum().fillna(0)[::-1].T
    Rolling30DayNonOutbreak_Fatal = df.loc['No'].loc['Fatal'].T[::-1].rolling(30).sum().fillna(0)[::-1].T
    Rolling30DayNonOutbreak_Resolved = df.loc['No'].loc['Resolved'].T[::-1].rolling(30).sum().fillna(0)[::-1].T
    Rolling30DayOutbreak_FatalRate = Rolling30DayOutbreak_Fatal/(Rolling30DayOutbreak_Resolved+Rolling30DayOutbreak_Fatal)
    Rolling30DayNonOutbreak_FatalRate = Rolling30DayNonOutbreak_Fatal/(Rolling30DayNonOutbreak_Resolved+Rolling30DayNonOutbreak_Fatal)

    df = pd.DataFrame([Rolling30DayOutbreak_FatalRate.iloc[:,0],Rolling30DayOutbreak_Fatal.iloc[:,0],Rolling30DayNonOutbreak_FatalRate.iloc[:,0],Rolling30DayNonOutbreak_Fatal.iloc[:,0]]).T
    df.columns = ['CFR%','Deaths','CFR%','Deaths']
    df = df.drop('Unknown')
    df.insert(0,column='Outbreak-->',value='')
    df.insert(3,column='Non-Outbreak-->',value='')
    df['CFR%'] = df['CFR%'].apply(lambda series: series.apply(lambda x: f"{x:.2%}"))
    df['Deaths'] = df['Deaths'].astype(int)

    Rolling30DayNonOutbreak_FatalRate.to_pickle('PickleNew/Rolling30DayNonOutbreak_FatalRate.pickle')
    Rolling30DayOutbreak_FatalRate.to_pickle('PickleNew/Rolling30DayOutbreak_FatalRate.pickle')

    with open(TableFileName,'w',newline='') as f:
        f.write('**Case fatality rates by age group (last 30 days):**')
        f.write('\n\n')
        f.write('Age Group|Outbreak-->|CFR %|Deaths|Non-outbreak-->|CFR%|Deaths|\n')
        f.write(':-:|:--|--:|--:|:--|--:|--:|\n')
        df.to_csv(f,sep = '|',header = False)

    print(round((time.time()-starttime),2),'- Fatality rate tables done')


    ############################################################################################
    ############################################################################################
    #Done
    "Count by Episode Date"

    ReportingLagPivot = pd.pivot_table(MasterDataFrame,values = 'Row_ID',index = ['Reporting_Lag'],columns = 'File_Date',aggfunc=np.count_nonzero)
    ReportingLagPivot.fillna(0,inplace=True)
    ReportingLagPivot = ReportingLagPivot.reindex(columns=sorted(ReportingLagPivot.columns,reverse = True))
    changeInReportingLag = ReportingLagPivot - ReportingLagPivot.shift(-1,axis=1).fillna(0).shift(1,axis=0).fillna(0)

    if os.path.exists('PickleNew/ReportingLagPivot.pickle'):
        tempDF = pd.read_pickle('PickleNew/ReportingLagPivot.pickle')
        tempDF = tempDF.merge(ReportingLagPivot[TodaysDate],left_index = True, right_index = True, how = 'outer',suffixes= ('drop',None) )
        tempDF = tempDF.drop([col for col in tempDF.columns if 'drop' in str(col)],axis = 1)
        ReportingLagPivot = tempDF
    ReportingLagPivot = ReportingLagPivot.fillna(0)
    ReportingLagPivot = ReportingLagPivot.reindex(columns=sorted(ReportingLagPivot.columns,reverse = True))
    ReportingLagPivot.to_pickle('PickleNew/ReportingLagPivot.pickle')
    #ReportingLagPivot.to_csv('CSVNew/ReportingLagPivot.csv')

    if os.path.exists('PickleNew/changeInReportingLag.pickle'):
        tempDF = pd.read_pickle('PickleNew/changeInReportingLag.pickle')
        tempDF = tempDF.merge(changeInReportingLag[TodaysDate],left_index = True, right_index = True, how = 'outer',suffixes= ('drop',None) )
        tempDF = tempDF.drop([col for col in tempDF.columns if 'drop' in str(col)],axis = 1)
        changeInReportingLag = tempDF
    changeInReportingLag = changeInReportingLag.fillna(0)
    changeInReportingLag = changeInReportingLag.reindex(columns=sorted(changeInReportingLag.columns, reverse=True))
    changeInReportingLag.to_pickle('PickleNew/changeInReportingLag.pickle')

    tempDataFrame = pd.DataFrame()
    for x in range(1, 31):
        b = ReportingLagPivot.loc[datetime.timedelta(days=1):datetime.timedelta(days=x)].sum(axis=0)
        tempDataFrame = pd.concat([tempDataFrame,b.to_frame(name=x).T])

    changeInReportingLag = changeInReportingLag.fillna(0)
    changeInReportingLag.to_pickle('PickleNew/Tab2Table3CasesByEpisodeDate.pickle')

    ############################################################################################
    ############################################################################################
    #Done but numbers are wrong


    casesByEpisodeDate = pd.pivot_table(MasterDataFrame,values = 'Row_ID',index = ['Episode_Date'],columns = 'File_Date',aggfunc=np.count_nonzero, margins = False)
    casesByEpisodeDate.fillna(0, inplace = True)
    casesByEpisodeDate.sort_values(by=['Episode_Date'],ascending =[False],inplace = True)
    casesByEpisodeDate = casesByEpisodeDate.reindex(columns=sorted(casesByEpisodeDate.columns,reverse = True))
    casesByEpisodeDate.to_pickle('PickleNew/casesByEpisodeDate.pickle')
    changeInCasesByEpisodeDate = casesByEpisodeDate - casesByEpisodeDate.shift(-1,axis=1)
    changeInCasesByEpisodeDate.insert(0,'TotalToDate',casesByEpisodeDate[casesByEpisodeDate.columns[0]])


    CumulativeCaseEpisodeDates = pd.pivot_table(MasterDataFrame[MasterDataFrame['File_Date']==TodaysDate],values = 'Row_ID',index = ['Episode_Date','File_Date'],columns = 'Reporting_Lag',aggfunc=np.count_nonzero)
    CumulativeCaseEpisodeDates.sort_values(by=['Episode_Date'],ascending =[False],inplace = True)
    #print(CumulativeCaseEpisodeDates.index)
    # CumulativeCaseEpisodeDates.to_csv('aaa.csv')

    CumulativeCaseEpisodeDates.fillna(0, inplace=True)

    CumulativeCaseFilePath = config.get('file_location', 'cumulative_case_filepath')
    if os.path.exists(CumulativeCaseFilePath):
        tempDF = pd.read_pickle(CumulativeCaseFilePath)
        CumulativeCaseEpisodeDates = pd.concat([tempDF, CumulativeCaseEpisodeDates], axis=0)
    CumulativeCaseEpisodeDates = CumulativeCaseEpisodeDates.fillna(0)
    if 'TotalToDate' in CumulativeCaseEpisodeDates.columns:
        CumulativeCaseEpisodeDates = CumulativeCaseEpisodeDates.drop('TotalToDate', axis=1)

    CumulativeCaseEpisodeDates = CumulativeCaseEpisodeDates.reset_index()
    # CumulativeCaseEpisodeDates =CumulativeCaseEpisodeDates.rename(columns = {'index':'Episode_Date'})

    CumulativeCaseEpisodeDates = CumulativeCaseEpisodeDates.sort_values(by='Episode_Date', ascending=[False])
    CumulativeCaseEpisodeDates = CumulativeCaseEpisodeDates.drop_duplicates(keep='first')
    CumulativeCaseEpisodeDates = CumulativeCaseEpisodeDates.set_index(['Episode_Date', 'File_Date'])

    CumulativeCaseEpisodeDates.insert(0, 'TotalToDate', casesByEpisodeDate[casesByEpisodeDate.columns[0]])
    CumulativeCaseEpisodeDates.to_pickle(CumulativeCaseFilePath)

    print(round((time.time() - starttime), 2), '- Case count by episode date done')

    ############################################################################################
    ############################################################################################

    "Case count by PHU/Source"
    "MasterDataFrame = pd.MultiIndex.from_frame(MasterDataFrame)"
    # casesByPHUAndSource = pd.pivot_table(MasterDataFrame,values = 'Row_ID',index = ['Reporting_PHU','Case_AcquisitionInfo'],columns = 'File_Date',aggfunc=np.count_nonzero, margins = False)
    # casesByPHUAndSource.sort_values(by=['Reporting_PHU','Case_AcquisitionInfo'],ascending =[True,True],inplace = True)
    # casesByPHUAndSource = casesByPHUAndSource.reindex(columns=sorted(casesByPHUAndSource.columns,reverse = True))
    # for x in range(casesByPHUAndSource.shape[1]-1):
    #     casesByPHUAndSource[casesByPHUAndSource.columns[x]] = casesByPHUAndSource.iloc[:,x] - casesByPHUAndSource.iloc[:,x+1]
    # with open('FFF.csv', 'a',newline='') as f:
    #     f.write('Case count by PHU/source \n')
    #     casesByPHUAndSource.to_csv(f,header = True)
    #     f.write('\n')

    ############################################################################################
    ############################################################################################

    "Deaths detail"
    MasterDataFrame = MasterDataFrame.dropna(axis='columns', how ='all')
    if 'Case_Reported_Date' in MasterDataFrame.columns:
        DeathsByPHUDetail = pd.pivot_table(MasterDataFrame[(MasterDataFrame['Outcome']=="Fatal")],values = 'Row_ID',index = ['Reporting_PHU','Age_Group','Client_Gender','Case_AcquisitionInfo','Case_Reported_Date','Episode_Date'],columns = 'File_Date',aggfunc=np.count_nonzero)
        DeathsByPHUDetail = DeathsByPHUDetail.reindex(columns=sorted(DeathsByPHUDetail.columns,reverse = True))
        DeathsByPHUDetail.sort_values(by=DeathsByPHUDetail.columns[0],ascending = False, inplace = True)
        DeathsByPHUDetail = DeathsByPHUDetail.fillna(0)
        changeInDeathsByPHUDetail = (DeathsByPHUDetail - DeathsByPHUDetail.shift(-1,axis = 1))
        changeInDeathsByPHUDetail = changeInDeathsByPHUDetail.fillna(0).astype(int)

        changeInDeathsByPHUDetail = changeInDeathsByPHUDetail.sort_values(by=['Age_Group','Reporting_PHU','Client_Gender','Case_Reported_Date'],ascending = [True,True,False,False])
        changeInDeathsByPHUDetail.to_pickle('PickleNew/changeInDeathsByPHUDetails.pickle')

        with open('FFF_Indiv.csv', 'a',newline='') as f:
            f.write('Change in Deaths by PHU \n')
            changeInDeathsByPHUDetail[abs(changeInDeathsByPHUDetail[changeInDeathsByPHUDetail.columns[0]])>0][changeInDeathsByPHUDetail.columns[0]].to_csv(f,header = True, sep=',')
            f.write('\n')

        with open(DeathDetailFileName, 'w',newline='') as f:
            f.write('|Reporting_PHU|Age_Group|Client_Gender|Case_AcquisitionInfo|Case_Reported_Date|Episode_Date|Count| \n')
            f.write(':--|:--|:--|:--|:--|:--|--:|\n')
            changeInDeathsByPHUDetail[abs(changeInDeathsByPHUDetail[changeInDeathsByPHUDetail.columns[0]])>0][changeInDeathsByPHUDetail.columns[0]].to_csv(f,header = False, sep='|')

        # print("* *"+format((changeInCasesByAge.iloc[0:3,:].sum()/changeInCasesByAge.iloc[:,:].sum())[1],".1%")+" of today's cases are in people under the age of 40.* /s")
        # print("* "+format((changeInCasesByAge.iloc[6:9,1].sum()/changeInCasesByAge.iloc[:,1].sum()),".1%")+" or "+format(int(changeInCasesByAge.iloc[6:9,1].sum()),",d")+" of today's cases are in people aged 70+ - [Chart of active 70+ cases](https://docs.google.com/spreadsheets/d/e/2PACX-1vQ7fegCALd11ElozUYcMi-e9Dj69YaiNQhvEpk81JHsyTACl0UXkWK5zfMNFe49Tq3VuN9Av-fuEZqV/pubchart?oid=365228609&format=interactive)")

        print(round((time.time()-starttime),2),'- Deaths detail done')

    #########################################################################
    #------------------------------------------------------------------------
    #Pivot table showing episode dates by PHU for current day's reports

    EpisodeDateByPHUToday = pd.pivot_table(MasterDataFrame[MasterDataFrame['File_Date']==MasterDataFrame['File_Date'].max()],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'Episode_Date',fill_value = 0,aggfunc = np.count_nonzero,margins = False).sort_index(axis = 1,ascending = False)
    EpisodeDateByPHUYesterday = pd.pivot_table(MasterDataFrame[MasterDataFrame['File_Date']==(MasterDataFrame['File_Date'].max()-datetime.timedelta(days = 1))],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'Episode_Date',fill_value = 0,aggfunc = np.count_nonzero,margins = False).sort_index(axis = 1,ascending = False)

    if EpisodeDateByPHUToday.columns.size >= EpisodeDateByPHUYesterday.columns.size:
        for x in range(EpisodeDateByPHUYesterday.columns.size-1):
            if EpisodeDateByPHUYesterday.columns[x] not in EpisodeDateByPHUToday.columns:
                #print (EpisodeDateByPHUYesterday.columns[x])
                EpisodeDateByPHUToday.insert(EpisodeDateByPHUToday.columns.size-1, EpisodeDateByPHUYesterday.columns[x],0)

        for x in range(EpisodeDateByPHUToday.columns.size-1):
            if EpisodeDateByPHUToday.columns[x] not in EpisodeDateByPHUYesterday.columns:
                #print (EpisodeDateByPHUToday.columns[x])
                EpisodeDateByPHUYesterday.insert(EpisodeDateByPHUYesterday.columns.size-1, EpisodeDateByPHUToday.columns[x],0)

    else:
        for x in range(EpisodeDateByPHUToday.columns.size-1):
            if EpisodeDateByPHUToday.columns[x] not in EpisodeDateByPHUYesterday.columns:
                #print (EpisodeDateByPHUToday.columns[x])
                EpisodeDateByPHUYesterday.insert(EpisodeDateByPHUYesterday.columns.size-1, EpisodeDateByPHUToday.columns[x],0)

        for x in range(EpisodeDateByPHUYesterday.columns.size-1):
            if EpisodeDateByPHUYesterday.columns[x] not in EpisodeDateByPHUToday.columns:
                #print (EpisodeDateByPHUYesterday.columns[x])
                EpisodeDateByPHUToday.insert(EpisodeDateByPHUToday.columns.size-1, EpisodeDateByPHUYesterday.columns[x],0)

    EpisodeDateByPHUToday = EpisodeDateByPHUToday.sort_index(axis = 1,ascending = False)
    EpisodeDateByPHUYesterday = EpisodeDateByPHUYesterday.sort_index(axis = 1,ascending = False)
    TodaysEpisodeDatesByPHU = EpisodeDateByPHUToday-EpisodeDateByPHUYesterday
    TodaysEpisodeDatesByPHU = TodaysEpisodeDatesByPHU.sort_index(axis = 1,ascending = False)

    TodaysEpisodeDatesByPHU.insert(0,'Today',TodaysEpisodeDatesByPHU.sum(axis=1))
    TodaysEpisodeDatesByPHU = TodaysEpisodeDatesByPHU.sort_values(by = TodaysEpisodeDatesByPHU.columns[0],ascending = False)

    #TodaysEpisodeDatesByPHU.to_csv('CSVNew/TodaysEpisodeDatesByPHU.csv')
    TodaysEpisodeDatesByPHU.to_pickle('PickleNew/TodaysEpisodeDatesByPHU.pickle')

    print(round((time.time()-starttime),2),'- Episode dates by PHU for todays cases done')


    #########################################################################
    #########################################################################
    #How many days ago of episode dates do the day's new cases relate to?
    ReportingLagPivot_WithPHU = pd.pivot_table(MasterDataFrame,values = 'Row_ID',index = ['Reporting_PHU','Reporting_Lag'],columns = 'File_Date',aggfunc=np.count_nonzero)
    ReportingLagPivot_WithPHU.fillna(0,inplace=True)
    ReportingLagPivot_WithPHU = ReportingLagPivot_WithPHU.reindex(columns=sorted(ReportingLagPivot_WithPHU.columns,reverse = True))
    changeInReportingLag_WithPHU = (ReportingLagPivot_WithPHU - ReportingLagPivot_WithPHU.shift(-1,axis=1).shift(1,axis=0)).ffill()

    if os.path.exists('PickleNew/changeInReportingLag_WithPHU.pickle'):
        tempDF = pd.read_pickle('PickleNew/changeInReportingLag_WithPHU.pickle')
        tempDF = tempDF.merge(changeInReportingLag_WithPHU[TodaysDate],left_index = True, right_index = True, how = 'outer',suffixes= ('drop',None) )
        tempDF = tempDF.drop([col for col in tempDF.columns if 'drop' in str(col)],axis = 1)
        changeInReportingLag_WithPHU = tempDF

    changeInReportingLag_WithPHU = changeInReportingLag_WithPHU.fillna(0)
    changeInReportingLag_WithPHU = changeInReportingLag_WithPHU.reindex(columns=sorted(changeInReportingLag_WithPHU.columns,reverse = True))
    changeInReportingLag_WithPHU = changeInReportingLag_WithPHU.reindex(changeInReportingLag_WithPHU.index)


    changeInReportingLag_WithPHU.to_pickle('PickleNew/changeInReportingLag_WithPHU.pickle')

    changeInCasesByPHU = pd.read_pickle('PickleNew/ChangeInCasesByPHU.pickle')

    #########################################################################
    #########################################################################
    # Change by PHU by ages
    AgesByPHUYesterday = pd.pivot_table(MasterDataFrame[(MasterDataFrame['File_Date']
                                                         == MasterDataFrame['File_Date'].max() - pd.DateOffset(days=1))],
                                        values='Row_ID', index=['Reporting_PHU'], columns='Age_Group',
                                        aggfunc=np.count_nonzero, margins=False).fillna(0)
    AgesByPHUToday = pd.pivot_table(MasterDataFrame[(MasterDataFrame['File_Date'] == MasterDataFrame['File_Date'].max())],
                                    values='Row_ID', index=['Reporting_PHU'], columns='Age_Group',
                                    aggfunc=np.count_nonzero, margins=False).fillna(0)
    changeInAgesByPHUToday = AgesByPHUToday - AgesByPHUYesterday
    changeInAgesByPHUToday.rename(columns={'19 & under': '<20', 'Unknown': 'N/A'}, inplace=True)
    changeInAgesByPHUTodayGrouped = pd.DataFrame([changeInAgesByPHUToday['<20'],
                                                  changeInAgesByPHUToday['20s'],
                                                  changeInAgesByPHUToday[['30s', '40s']].sum(axis=1),
                                                  changeInAgesByPHUToday[['50s', '60s']].sum(axis=1),
                                                  changeInAgesByPHUToday[['70s', '80s', '90+']].sum(axis=1)]).T
    changeInAgesByPHUTodayGrouped.columns = ['<20', '20-29', '30-49', '50-69', '70+']
    changeInAgesByPHUTodayGrouped.to_pickle('Pickle/changeInAgesByPHUTodayGrouped.pickle')

    #########################################################################
    #########################################################################
    # Change by source by PHU

    changeInSourcesByPHUToday = (pd.pivot_table(MasterDataFrame[(MasterDataFrame['File_Date']==MasterDataFrame['File_Date'].max())],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'Case_AcquisitionInfo',aggfunc=np.count_nonzero, margins = False).fillna(0)
                                 - pd.pivot_table(MasterDataFrame[(MasterDataFrame['File_Date']==MasterDataFrame['File_Date'].max()-pd.DateOffset(days=1))],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'Case_AcquisitionInfo',aggfunc=np.count_nonzero, margins = False).fillna(0)).fillna(0)

    changeInSourcesByPHUToday.to_pickle('PickleNew/changeInSourcesByPHUToday.pickle')
    #########################################################################
    #########################################################################

    '''

    #Combine the data into the main table
    NewCasePHUDF = pd.read_pickle('PickleNew/NewCasePHUDF.pickle')
    #NewCasePHUDF = NewCasePHUDF.drop('Total',axis = 0)


    DisplayDF = pd.DataFrame()
    DisplayDF = DisplayDF.merge(changeInCasesByPHU.iloc[:,0],left_index = True,right_index = True,how = 'outer')

    DisplayDF = DisplayDF.merge(activeCasesByPHU.iloc[:,0],left_index = True,right_index = True,how = 'outer')
    DisplayDF.columns = ['TodayDF','Active/100k']

    DisplayDF = DisplayDF.merge(PHUPopulation(),on='Reporting_PHU')


    DisplayDF.insert(1,column='Averages-->',value='')
    DisplayDF.insert(1,column='Totals per 100k-->',value='')

    DisplayDF.insert(1,'Ages (day %)->>',"")
    DisplayDF = DisplayDF.merge(changeInAgesByPHUTodayGrouped,left_index = True,right_index = True,how = 'outer')
    DisplayDF.insert(4,'Source (day %)->>',"")
    DisplayDF = DisplayDF.merge(changeInSourcesByPHUToday,left_index = True,right_index = True,how = 'outer')
    DisplayDF = DisplayDF.fillna(0)




    totals = DisplayDF.sum()
    totals.name = 'Total'
    DisplayDF = DisplayDF.append(totals.transpose())

    restNewCaseStats = DisplayDF.loc[abs(DisplayDF['TodayDF'])==0].sum()
    restNewCaseStats.name = 'Regions of Zeroes'
    DisplayDF = DisplayDF.loc[abs(DisplayDF['TodayDF'])>=1]
    DisplayDF = DisplayDF.append(restNewCaseStats.transpose())


    # DisplayDF = DisplayDF.fillna(0)
    DisplayDF = DisplayDF.sort_values(by='TodayDF',ascending = False)

    for column in ['<20','20-29','30-49','50-69','70+','Close contact','Community','Outbreak','Travel']:
        DisplayDF[column] = ((DisplayDF[column]/DisplayDF['TodayDF'])*100).round(1)

    for column in ['Active/100k']:
        DisplayDF[column] = ((DisplayDF[column]/DisplayDF['Population'])*100000).round(1)

    #DisplayDF.replace([np.inf, -np.inf], '', inplace=True)
    DisplayDF = DisplayDF.merge(NewCasePHUDF[['Today','Yesterday','Last 7','Prev 7','Last 7/100k','Prev 7/100k']],left_index = True,right_index = True,how = 'outer')
    DisplayDF['Today'] = DisplayDF['Today'].map('{:.0f}'.format)
    DisplayDF['Yesterday'] = DisplayDF['Yesterday'].map('{:.0f}'.format)
    DisplayDF = DisplayDF.sort_values(by='TodayDF',ascending = False)

    DisplayDF = DisplayDF[['Today','Averages-->','Last 7','Prev 7','Totals per 100k-->','Last 7/100k','Prev 7/100k','Active/100k','Ages (day %)->>','<20','20-29','30-49','50-69','70+','Source (day %)->>','Close contact','Community','Outbreak','Travel']]


    with open('TextOutput/NewCasesTable.txt','w',newline = '') as f:
        header = 'PHU|'

        for column in range(0,DisplayDF.columns.size):
            header = header+DisplayDF.columns[column]+'|'
        f.write(header)
        f.write('\n')
        f.write(':-:|--:|:--|--:|--:|:--|--:|--:|--:|:--|--:|--:|--:|--:|--:|:--|--:|--:|--:|--:|')
        #Extra column above
        #f.write(':--|--:|:--|--:|--:|:--|--:|--:|--:|:--|--:|--:|--:|--:|--:|:--|--:|--:|--:|--:|')
        f.write('\n')
        DisplayDF.to_csv(f,header=False,sep = '|')
        f.write('\n')

    #########################################################################
    #########################################################################


    DisplayDF.to_pickle('PickleNew/DisplayDF.pickle')


    DisplayDF.to_csv('Display.csv')
    '''


    """
    for date in pd.date_range( start = '2021-09-12',end = '2021-09-12'):

    print(date)
    cp.DailyReports_Individual(date.strftime("%Y-%m-%d"))
    """


def DailyReports_Compile():
    starttime = datetime.datetime.now()
    print('------------------------------------------------------------------------')
    print(f'DailyReports_Compile \nStarted: {starttime:%Y-%m-%d %H:%M:%S}')

    PivotTableFileName = 'Pivot_New.csv'
    # DeathDetailFileName = 'TextOutput/DeathDetail.txt'
    f = open(PivotTableFileName, 'w')
    f.close()
    # DailyReports_Individual(FileNameIn)
    DailyReports_PHUChange()
    time.sleep(1)
    OntarioCaseStatus()

    OntarioCaseStatusDF = pd.read_pickle('Pickle/OntarioCaseStatus.pickle')
    OntarioCaseStatusDF = OntarioCaseStatusDF[['Total patients approved for testing as of Reporting Date',
                                               'Under Investigation', 'Total Positive LTC Resident Cases',
                                               'Total Positive LTC HCW Cases',
                                               'Number of patients hospitalized with COVID-19',
                                               'Number of patients in ICU due to COVID-19',
                                               'Number of patients in ICU on a ventilator due to COVID-19',
                                               'Day new cases', '7 day SMA', 'Day new deaths',
                                               'Day new resolved', 'CumHosp', 'CumICU']]
    OntarioCaseStatusDF = OntarioCaseStatusDF.T

    # changeInCasesByPHU = pd.read_pickle('PickleNew/ChangeInCasesByPHU-Display.pickle')
    changeInCasesByPHU = pd.read_pickle('PickleNew/ChangeInCasesByPHU-Display-1.pickle')
    # changeInCasesByPHU_2 = pd.read_pickle('PickleNew/ChangeInCasesByPHU.pickle')

    changeInCasesByAge = pd.read_pickle('PickleNew/ChangeInCasesByAge.pickle')
    changeInDeathsByPHU = pd.read_pickle('PickleNew/changeInDeathsByPHU.pickle')
    changeInCasesByOutcome = pd.read_pickle('PickleNew/ChangeInCasesByOutcome.pickle')
    activeCasesByPHU = pd.read_pickle('PickleNew/ActiveCasesByPHU.pickle')
    changeInActiveCasesByPHU = pd.read_pickle('PickleNew/ChangeInActiveCasesByPHU.pickle')
    casesByOutcomeByAge = pd.read_pickle('PickleNew/casesByOutcomeByAge.pickle')
    changeInCasesByOutcomeByAge = pd.read_pickle('PickleNew/changeInCasesByOutcomeByAge.pickle')
    activeCases70PlusOutbreakStatus = pd.read_pickle('PickleNew/Active70PlusOutbreakStatus.pickle')
    changeInCasesBySource = pd.read_pickle('PickleNew/changeInCasesBySource.pickle')
    Rolling30DayNonOutbreak_FatalRate = pd.read_pickle('PickleNew/Rolling30DayNonOutbreak_FatalRate.pickle')
    Rolling30DayOutbreak_FatalRate = pd.read_pickle('PickleNew/Rolling30DayOutbreak_FatalRate.pickle')
    ReportingLagPivot = pd.read_pickle('PickleNew/ReportingLagPivot.pickle')
    changeInReportingLag = pd.read_pickle('PickleNew/changeInReportingLag.pickle')
    changeInAgesByPHUTodayGrouped = pd.read_pickle('Pickle/changeInAgesByPHUTodayGrouped.pickle')
    changeInSourcesByPHUToday = pd.read_pickle('PickleNew/changeInSourcesByPHUToday.pickle')

    CumulativeCaseFilePath = config.get('file_location', 'cumulative_case_filepath')
    CumulativeCaseEpisodeDates = pd.read_pickle(CumulativeCaseFilePath)
    casesByEpisodeDate = pd.read_pickle('PickleNew/casesByEpisodeDate.pickle')

    tempCol = CumulativeCaseEpisodeDates.pop('TotalToDate')
    tempCol = tempCol.drop_duplicates()
    CumulativeCaseEpisodeDates = CumulativeCaseEpisodeDates.groupby(level=0).sum()
    CumulativeCaseEpisodeDates.insert(0, 'TotalToDate', casesByEpisodeDate[casesByEpisodeDate.columns[0]])
    CumulativeCaseEpisodeDates = CumulativeCaseEpisodeDates.sort_values(by='Episode_Date', ascending=False)

    # changeInDeathsByPHUDetail = pd.read_pickle('PickleNew/changeInDeathsByPHUDetails.pickle')
    TodaysEpisodeDatesByPHU = pd.read_pickle('PickleNew/TodaysEpisodeDatesByPHU.pickle')
    changeInReportingLag_WithPHU = pd.read_pickle('PickleNew/changeInReportingLag_WithPHU.pickle')
    FirstDosePivot = pd.read_pickle('PickleNew/FirstDosePivot-Display.pickle')
    SecondDosePivot = pd.read_pickle('PickleNew/SecondDosePivot-Display.pickle')
    FirstDoseCountPivot = pd.read_pickle('PickleNew/FirstDoseCountPivot-Display.pickle')
    SecondDoseCountPivot = pd.read_pickle('PickleNew/SecondDoseCountPivot-Display.pickle')
    dfCaseByVaxStatus = pd.read_pickle('PickleNew/dfCaseByVaxStatus-Display.pickle')
    dfICUHospByVaxStatus = pd.read_pickle('PickleNew/dfICUHospByVaxStatus-Display.pickle')
    dfCanadaData = pd.read_pickle('Pickle/dfCanadaData.pickle')

    with open(PivotTableFileName, 'w', newline='') as f:
        f.write('Ontario Case Status \n')
        OntarioCaseStatusDF.to_csv(f, header=True)
        f.write('\n')

        f.write('Change InCases by PHU \n')
        changeInCasesByPHU.to_csv(f, header=True)
        f.write('\n')
        f.write('Change in Cases by Age Group \n')
        changeInCasesByAge.to_csv(f, header=True)
        f.write('\n')
        f.write('Change In Deaths by PHU \n')
        changeInDeathsByPHU.to_csv(f, header=True)
        f.write('\n')
        f.write('changeInCases by Outcome \n')
        changeInCasesByOutcome.to_csv(f, header=True)
        f.write('\n')
        f.write('changeInActive by PHU \n')
        changeInActiveCasesByPHU.to_csv(f, header=True)
        f.write('\n')
        f.write('Active by PHU \n')
        activeCasesByPHU.to_csv(f, header=True)
        f.write('\n')
        f.write('Cases by Outcome by Age \n')
        # casesByOutcomeByAge.to_csv(f,header = True)
        casesByOutcomeByAge.loc["No"].add(casesByOutcomeByAge.loc["Yes"], fill_value=0).to_csv(f, header=True)
        f.write('\n')
        f.write('changeInCases by Outcome by Age \n')
        changeInCasesByOutcomeByAge.to_csv(f, header=True)
        f.write('\n')
        f.write('ActiveCases70PlusByOutbreak /n')
        activeCases70PlusOutbreakStatus.to_csv(f, header=True)
        f.write('\n')
        f.write('Change in Case count by source \n')
        changeInCasesBySource.to_csv(f, header=True)
        f.write('\n')
        f.write('FatalityRate - Outbreak')
        Rolling30DayOutbreak_FatalRate.to_csv(f, header=True)
        f.write('\n')
        f.write('FatalityRate - Non-outbreak')
        Rolling30DayNonOutbreak_FatalRate.to_csv(f, header=True)
        f.write('\n')
        f.write('Episode Table 2 - Positive cases with episode dates X days before report date \n')
        TempReportingLagPivot = ReportingLagPivot.copy()
        TempReportingLagPivot.index = TempReportingLagPivot.index.days
        TempReportingLagPivot.iloc[1:31, :].to_csv(f, header=True)
        # ReportingLagPivot.loc['1 days': '30 days'].to_csv(f,header = True)
        f.write('\n')

        tempDataFrame = pd.DataFrame()
        for x in range(1, 31):
            b = ReportingLagPivot.loc[datetime.timedelta(days=1):datetime.timedelta(days=x)].sum(axis=0)
            tempDataFrame = pd.concat([tempDataFrame, b.to_frame(name=x).T])
        f.write('Episode Table 1 - Cumulative Positive cases with episode dates X days before report date \n')
        # tempDataFrame.loc['0 days': '30 days'].to_csv(f,header = True)
        tempDataFrame.to_csv(f, header=True)
        f.write('\n')
        f.write("Table 3 - How long ago do  the day's new cases relate to? \n")
        changeInReportingLag.iloc[0:30, :].to_csv(f, header=True)
        changeInReportingLag.iloc[30:].sum(axis=0).to_frame(name='Over 30 days').transpose().to_csv(f, header=False)
        f.write('\n')

        f.write('Cumulative Case count - days since Episode Date \n')
        CumulativeCaseEpisodeDates.iloc[0:240, 0:300].to_csv(f, header=True)
        f.write('\n')
        f.write("Current day's cases by episode date by PHU \n")
        TodaysEpisodeDatesByPHU.to_csv(f, header=True)
        f.write('\n')
        changeInCasesByPHU = changeInCasesByPHU.sort_values(by=changeInCasesByPHU.columns[1],
                                                            ascending=False,
                                                            inplace=False)

        for x in list(changeInCasesByPHU.index[0:15]):
            f.write("Table X - How long ago do  the day's new cases relate to? - ,")
            f.write(x)
            f.write('\n')

            changeInReportingLag_WithPHU.loc[x].loc['1 days':'10 days'].to_csv(f, header=True)
            changeInReportingLag_WithPHU.loc[x].loc['11 days':].sum(axis=0).to_frame(name='Over 10 days').T.to_csv(f, header=False)

        f.write('\n')
        f.write('Vaccines - 1st dose by day \n')
        FirstDosePivot.to_csv(f, header=True)
        f.write('\n')

        f.write('Vaccines - 2nd dose by day \n')
        SecondDosePivot.to_csv(f, header=True)
        f.write('\n')

        f.write('Vaccines - 1st dose count by day \n')
        FirstDoseCountPivot.to_csv(f, header=True)
        f.write('\n')

        f.write('Vaccines - 2nd dose count by day \n')
        SecondDoseCountPivot.to_csv(f, header=True)
        f.write('\n')

        f.write('Cases By Vax Status \n')
        dfCaseByVaxStatus.T.to_csv(f)
        f.write('\n')

        f.write('ICU By Vax Status \n')
        dfICUHospByVaxStatus.T.to_csv(f)
        f.write('\n')

        f.write('CanadaData - Last 7/100 \n')
        dfCanadaData.T.to_csv(f)
        f.write('\n')

    NewCasePHUDF = pd.read_pickle('PickleNew/NewCasePHUDF.pickle')
    DisplayDF = pd.DataFrame()
    DisplayDF = DisplayDF.merge(changeInCasesByPHU.iloc[:, 1], left_index=True,
                                right_index=True, how='outer')
    DisplayDF = DisplayDF.merge(activeCasesByPHU.iloc[:, 0], left_index=True,
                                right_index=True, how='outer')
    DisplayDF.columns = ['TodayDF', 'Active/100k']
    DisplayDF = DisplayDF.merge(PHUPopulation(), on='Reporting_PHU')

    DisplayDF.insert(1, column='Averages-->', value='')
    DisplayDF.insert(1, column='Totals per 100k-->', value='')

    DisplayDF.insert(1, 'Ages (day %)->>', "")
    DisplayDF = DisplayDF.merge(changeInAgesByPHUTodayGrouped, left_index=True, right_index=True, how='outer')
    DisplayDF.insert(4, 'Source (day %)->>', "")
    DisplayDF = DisplayDF.merge(changeInSourcesByPHUToday, left_index=True, right_index=True, how='outer')
    DisplayDF = DisplayDF.fillna(0)

    totals = DisplayDF.sum()
    totals.name = 'Total'
    DisplayDF = DisplayDF.append(totals.transpose())

    restNewCaseStats = DisplayDF.loc[abs(DisplayDF['TodayDF']) == 0].sum()
    restNewCaseStats.name = 'Regions of Zeroes'
    DisplayDF = DisplayDF.loc[abs(DisplayDF['TodayDF']) >= 1]
    DisplayDF = DisplayDF.append(restNewCaseStats.transpose())

    # DisplayDF = DisplayDF.fillna(0)
    DisplayDF = DisplayDF.sort_values(by='TodayDF', ascending=False)

    for column in ['<20', '20-29', '30-49', '50-69', '70+',
                   'Close contact', 'Community', 'Outbreak', 'Travel']:
        DisplayDF[column] = ((DisplayDF[column] / DisplayDF['TodayDF']) * 100).round(1)

    for column in ['Active/100k']:
        DisplayDF[column] = ((DisplayDF[column] / DisplayDF['Population']) * 100000).round(1)

    # DisplayDF.replace([np.inf, -np.inf], '', inplace=True)
    DisplayDF = DisplayDF.merge(NewCasePHUDF[['Today', 'Yesterday', 'Last 7',
                                              'Prev 7', 'Last 7/100k', 'Prev 7/100k']],
                                left_index=True, right_index=True, how='outer')
    DisplayDF['Today'] = DisplayDF['Today'].map('{:.0f}'.format)
    DisplayDF['Yesterday'] = DisplayDF['Yesterday'].map('{:.0f}'.format)
    DisplayDF = DisplayDF.sort_values(by='TodayDF', ascending=False)

    DisplayDF = DisplayDF[['Today', 'Averages-->', 'Last 7', 'Prev 7',
                           'Totals per 100k-->', 'Last 7/100k', 'Prev 7/100k', 'Active/100k',
                           'Ages (day %)->>', '<20', '20-29', '30-49', '50-69', '70+',
                           'Source (day %)->>', 'Close contact', 'Community', 'Outbreak', 'Travel']]

    with open('TextOutput/NewCasesTable.txt', 'w', newline='') as f:
        header = 'PHU|'

        for column in range(0, DisplayDF.columns.size):
            header = header + DisplayDF.columns[column] + '|'
        f.write(header)
        f.write('\n')
        f.write(':-:|--:|--:|--:|--:|:--|--:|--:|--:|:--|--:|--:|--:|--:|--:|:--|--:|--:|--:|--:|')
        # Extra column above
        # f.write(':--|--:|:--|--:|--:|:--|--:|--:|--:|:--|--:|--:|--:|--:|--:|:--|--:|--:|--:|--:|')
        f.write('\n')
        DisplayDF.to_csv(f, header=False, sep='|')
        f.write('\n')

    DisplayDF.to_csv('Pickle/Display.csv')

    #########################################################################
    #########################################################################
    # Put together the Text Output file

    GlobalDataFileName = 'TextOutput/GlobalData.txt'
    ChildCareDataFileName = 'TextOutput/ChildCareData.txt'
    DeathProjectionFileName = 'TextOutput/DeathProjectionText.txt'
    FatalityRateFileName = 'TextOutput/FatalityRateTable.txt'
    VaccineDataFileName = 'TextOutput/VaccineDataText.txt'
    AppDataFileName = 'TextOutput/COVIDAppDataText.txt'
    NewCasesDataFileName = 'TextOutput/NewCasesTable.txt'
    CanadaDataFileName = 'TextOutput/CanadaData.txt'
    JailDataFileName = 'TextOutput/JailData.txt'
    OutbreakDataFileName = 'TextOutput/OutbreakData.txt'
    LTCDataFileName = 'TextOutput/LTCDataText.txt'
    VaccineTableFileName = 'TextOutput/VaccineAgeTable.txt'
    PostalCodeFileName = 'TextOutput/PostalCodeData.txt'
    VaccinePHUTableFileName = 'TextOutput/VaccineAgePHU.txt'
    DeathDetailFileName = 'TextOutput/DeathDetail.txt'
    OntarioCaseStatusFileName = 'TextOutput/OntarioCaseStatusText.txt'
    OntarioThrowbackFileName = 'TextOutput/OntariThrowbackText.txt'
    SchoolsFileName = 'TextOutput/SchoolsText.txt'
    icu_capacity_filename = 'TextOutput/ICU_capacity.txt'
    school_closuresfilename = 'TextOutput/school_closures.txt'
    school_absenteeism_filename = 'TextOutput/school_absenteeism.txt'

    with open(GlobalDataFileName, 'r') as f:
        GlobalLines = f.readlines()
    with open(ChildCareDataFileName, 'r') as f:
        ChildCareLines = f.readlines()
    # with open(DeathProjectionFileName, 'r') as f:
    #     DeathProjectionLines = f.readlines()
    with open(FatalityRateFileName, 'r') as f:
        FatalityRateLines = f.readlines()
    with open(OntarioCaseStatusFileName, 'r') as f:
        OntarioCaseStatusLines = f.readlines()
    with open(OntarioThrowbackFileName, 'r') as f:
        OntarioThrowbackLines = f.readlines()

    with open(SchoolsFileName, 'r') as f:
        SchoolsLines = f.readlines()

    with open(VaccineDataFileName, 'r') as f:
        VaccineDataLines = f.readlines()
    with open(AppDataFileName, 'r') as f:
        AppDataLines = f.readlines()
    with open(NewCasesDataFileName, 'r') as f:
        NewCasesDataLines = f.readlines()
    with open(CanadaDataFileName, 'r') as f:
        CanadaDataLines = f.readlines()
    with open(JailDataFileName, 'r') as f:
        JailDataLines = f.readlines()
    with open(OutbreakDataFileName, 'r') as f:
        OutbreakDataLines = f.readlines()
    with open(LTCDataFileName, 'r') as f:
        LTCDataLines = f.readlines()

    with open(VaccineTableFileName, 'r') as f:
        VaccineTableLines = f.readlines()

    with open(PostalCodeFileName, 'r') as f:
        PostalCodeLines = f.readlines()

    with open(VaccinePHUTableFileName, 'r') as f:
        VaccinePHULines = f.readlines()

    with open(DeathDetailFileName, 'r') as f:
        DeathDetailLines = f.readlines()

    with open(icu_capacity_filename, 'r') as f:
        icu_capacity_lines = f.readlines()

    with open(school_closuresfilename, 'r') as f:
        school_closures_lines = f.readlines()

    with open(school_absenteeism_filename, 'r') as f:
        school_absenteeism_lines = f.readlines()


    with open('TextFileOutput.txt', 'w', encoding='utf-8') as f:
        ############################################################################################
        # Not published anymore
        # f.write('Link to report: https://files.ontario.ca/moh-covid-19-report-en-' + str(pd.Timestamp.today().strftime('%Y-%m-%d')) + '.pdf')
        # f.write('\n\n')
        ############################################################################################
        f.write('Detailed tables: [Google Sheets mode](https://docs.google.com/spreadsheets/d/1E28C0ylUQ0hHgFySFpXtdjX_LkdY5tlhl-nt0SGhCDg) and [some TLDR charts](https://imgur.com/a/qI0P0X5)')
        f.write('\n\n')
        f.write('------------------------------------------------------------\n')
        f.write('\n\n')
        f.writelines(OntarioThrowbackLines)
        f.write('\n\n')
        f.write('------------------------------------------------------------\n')
        f.write('\n')
        f.writelines(OntarioCaseStatusLines)
        f.write('\n')
        f.writelines(icu_capacity_lines)
        f.write('\n')
        f.writelines(VaccineDataLines)
        f.write('\n')
        f.writelines(VaccineTableLines)
        f.write('\n')
        f.write('**Schools Data:**\n\n')
        f.writelines(school_closures_lines)
        f.writelines(school_absenteeism_lines)
        f.write('\n')
        # f.writelines(SchoolsLines)
        # f.write('\n')
        # f.writelines(ChildCareLines)
        # f.write('\n')
        f.writelines(OutbreakDataLines)
        f.write('\n')
        # f.writelines(PostalCodeLines)
        f.write('\n\n')

        f.writelines(GlobalLines)
        f.write('\n')
        f.write('\n')
        f.writelines(JailDataLines)
        f.write('\n')
        f.writelines(AppDataLines)

        f.write('\n')
        f.writelines(FatalityRateLines)
        f.write('\n\n')
        f.write('------------------------------------------------------------\n')
        f.write('\n')
        f.write('**Main data table:**\n\n')
        f.writelines(NewCasesDataLines)
        f.write('\n\n')
        f.write('------------------------------------------------------------\n')
        f.write('\n')
        f.writelines(VaccinePHULines)
        f.write('\n\n')
        f.write('------------------------------------------------------------\n')
        f.write('\n\n')
        # f.write('**Canada comparison** - [Source](https://www.canada.ca/en/public-health/services/diseases/coronavirus-disease-covid-19/epidemiological-economic-research-data.html) - not updated Sunday/Monday')
        f.write('\n\n')
        f.writelines(CanadaDataLines)
        f.write('\n\n')
        f.write('------------------------------------------------------------\n')
        f.write('\n\n')
        f.writelines(LTCDataLines)
        f.write('\n\n')
        f.write("**Today's deaths:**\n\n")
        f.writelines(DeathDetailLines)

    #########################################################################
    #########################################################################

    import subprocess

    child = subprocess.Popen(['notepad', 'TextFileOutput.txt'])
    child = subprocess.Popen(['notepad', 'TextOutput/PostTitle.txt'])

    # child.kill()
    endTime = datetime.datetime.now()
    print(f'DailyReports_Compile Ended:   {endTime:%Y-%m-%d %H:%M:%S} {(endTime - starttime).total_seconds():.2f} seconds')
    print('------------------------------------------------------------------------')


def DailyReports_PHUChange():
    '''
    Takes the MoH's cases by PHU report and calculates various stats from it

    Returns
    -------
    None.

    '''
    starttime = datetime.datetime.now()
    print('------------------------------------------------------------------------')
    print(f'DailyReports_PHUChange \nStarted: {starttime:%Y-%m-%d %H:%M:%S}')
    NewCasePHUDF = pd.read_csv('https://data.ontario.ca/dataset/f4f86e54-872d-43f8-8a86-3892fd3cb5e6/resource/8a88fe6d-d8fb-41a3-9d04-f0550a44999f/download/daily_change_in_cases_by_phu.csv')
    # NewCasePHUDF = pd.read_csv('daily_change_in_cases_by_phu.csv')
    NewCasePHUDF.to_csv('SourceFiles/daily_change_in_cases_by_phu.csv')
    NewCasePHUDF = NewCasePHUDF.set_index('Date')
    NewCasePHUDF = NewCasePHUDF.T
    NewCasePHUDF['Reporting_PHU'] = NewCasePHUDF.index
    NewCasePHUDF = PHUNameReplacements(NewCasePHUDF)
    NewCasePHUDF = NewCasePHUDF.set_index('Reporting_PHU')
    NewCasePHUDF = NewCasePHUDF.fillna(0)
    NewCasePHUDF = NewCasePHUDF.astype(int)
    NewCasePHUDF = NewCasePHUDF.reindex(columns=sorted(NewCasePHUDF.columns,
                                                       reverse=True))
    NewCasePHUDF = NewCasePHUDF.drop('Total', axis=0)
    NewCasePHUDF_Pivot = NewCasePHUDF.copy()
    NewCasePHUDF_Pivot.insert(0, 'TotalToDate', NewCasePHUDF.sum(axis=1))
    NewCasePHUDF_Pivot = NewCasePHUDF_Pivot.sort_values(by='TotalToDate', ascending=False)
    NewCasePHUDF_Pivot.to_pickle('PickleNew/ChangeInCasesByPHU-Display-1.pickle')

    NewCasePHUDF['Today'] = NewCasePHUDF[NewCasePHUDF.columns[0]]
    NewCasePHUDF['Yesterday'] = NewCasePHUDF[NewCasePHUDF.columns[1]]

    NewCasePHUDF = pd.merge(NewCasePHUDF, PHUPopulation(), on='Reporting_PHU')

    totals = NewCasePHUDF.sum()
    totals.name = 'Total'
    NewCasePHUDF = NewCasePHUDF.append(totals.transpose())

    restNewCaseStats = NewCasePHUDF.loc[abs(NewCasePHUDF['Today']) == 0].sum()
    restNewCaseStats.name = 'Regions of Zeroes'
    NewCasePHUDF = NewCasePHUDF.loc[abs(NewCasePHUDF['Today']) >= 1]
    NewCasePHUDF = NewCasePHUDF.append(restNewCaseStats.transpose())

    NewCasePHUDF['Last 7'] = NewCasePHUDF.iloc[:, 0:7].mean(axis=1).round(1)
    NewCasePHUDF['Prev 7'] = NewCasePHUDF.iloc[:, 7:14].mean(axis=1).round(1)
    NewCasePHUDF['Last 7/100k'] = (NewCasePHUDF.iloc[:, 0:7].sum(axis=1)
                                   / NewCasePHUDF['Population'] * 100000
                                   ).round(1)
    NewCasePHUDF['Prev 7/100k'] = (NewCasePHUDF.iloc[:, 7:14].sum(axis=1)
                                   / NewCasePHUDF['Population'] * 100000
                                   ).round(1)
    NewCasePHUDF = NewCasePHUDF[['Today', 'Yesterday', 'Last 7', 'Prev 7',
                                 'Last 7/100k', 'Prev 7/100k']]
    # NewCasePHUDF = NewCasePHUDF[['Today','Yesterday','Last 7/100k','Prev 7/100k']]

    NewCasePHUDF = NewCasePHUDF.sort_values(by='Today', ascending=False)

    NewCasePHUDF.to_csv('Pickle/NewCasesPHU.csv')
    NewCasePHUDF.to_pickle('PickleNew/NewCasePHUDF.pickle')

    endTime = datetime.datetime.now()
    print(f'Ended:   {endTime:%Y-%m-%d %H:%M:%S} {(endTime - starttime).total_seconds():.2f} seconds')
    print('------------------------------------------------------------------------')


def DailyReports():
    '''
    Do not use this anymore - replaced by DailyReports_Individual

    Returns
    -------
    None.

    '''
    starttime = time.time()
    ConsoleOut = sys.stdout

    textfilename = 'OutputTextFile.txt'
    DeathDetailFileName = 'TextOutput/DeathDetail.txt'

    pd.set_option('display.max_rows', 100)
    pd.set_option('display.max_columns', 100)
    pd.options.display.float_format = '{:.3f}'.format

    global casesByPHU
    global MasterDataFrame
    global stats
    global changeInDeathsByPHU
    global changeInCasesByPHU

    config = configparser.ConfigParser()
    config.read(FOLDER_LOCATION_INI_FILE)
    master_df_location = config.get('file_location', 'master_dataframe')
    MasterDataFrame = pd.read_pickle(master_df_location)

    MasterDataFrame.drop(columns=['Test_Reported_Date','Specimen_Date','Reporting_PHU_ID'], inplace=True)
    print(round((time.time()-starttime),2),'- MasterDataFrame loaded')

    #MasterDataFrame = MasterDataFrame[MasterDataFrame['File_Date']>MasterDataFrame['File_Date'].max()-datetime.timedelta(days=7)]


    "https://stackoverflow.com/questions/30829748/multiple-pandas-dataframe-to-one-csv-file"


    ############################################################################################
    ############################################################################################
    "Active Case count by PHU"
    pivot_Active = pd.pivot_table(MasterDataFrame[MasterDataFrame['Outcome']=='Not Resolved'],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'File_Date',aggfunc=np.count_nonzero)
    pivot_Active = pivot_Active.reindex(columns=sorted(pivot_Active.columns,reverse = True))
    pivot_Active = pivot_Active[pivot_Active.columns[0]]
    pivot_Active.name = 'ActiveCases'

    "Case count by PHU"
    casesByPHU = pd.pivot_table(MasterDataFrame,values = 'Row_ID',index = ['Reporting_PHU'],columns = 'File_Date',aggfunc=np.count_nonzero)
    casesByPHU = casesByPHU.reindex(columns=sorted(casesByPHU.columns,reverse = True))
    casesByPHU.sort_values(by=casesByPHU.columns[0],ascending = False, inplace = True)

    "Change in Case count by PHU"
    changeInCasesByPHU = (casesByPHU - casesByPHU.shift(-1,axis = 1))
    changeInCasesByPHU.insert(0,'TotalToDate',casesByPHU[casesByPHU.columns[0]])
    with open('PivotTable.csv', 'a',newline='') as f:
        f.write('Change InCases by PHU \n')
        changeInCasesByPHU.to_csv(f,header = True)
        f.write('\n')

    changeInCasesByPHU.drop(['TotalToDate'],axis = 1,inplace = True)
    changeInCasesByPHU = changeInCasesByPHU.sort_values(by=changeInCasesByPHU.columns[0],ascending = False, inplace = False)
    changeInCasesByPHU = changeInCasesByPHU.fillna(0)
    changeInCasesByPHU.to_pickle('Pickle/ChangeInCasesByPHU.pickle')


    changeInSourcesByPHUToday = (pd.pivot_table(MasterDataFrame[(MasterDataFrame['File_Date']==MasterDataFrame['File_Date'].max())],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'Case_AcquisitionInfo',aggfunc=np.count_nonzero, margins = False).fillna(0) - pd.pivot_table(MasterDataFrame[(MasterDataFrame['File_Date']==MasterDataFrame['File_Date'].max()-pd.DateOffset(days=1))],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'Case_AcquisitionInfo',aggfunc=np.count_nonzero, margins = False).fillna(0)).fillna(0)
    changeInSourcesByPHUWeek = (pd.pivot_table(MasterDataFrame[(MasterDataFrame['File_Date']==MasterDataFrame['File_Date'].max())],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'Case_AcquisitionInfo',aggfunc=np.count_nonzero, margins = False).fillna(0) - pd.pivot_table(MasterDataFrame[(MasterDataFrame['File_Date']==MasterDataFrame['File_Date'].max()-pd.DateOffset(days=7))],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'Case_AcquisitionInfo',aggfunc=np.count_nonzero, margins = False).fillna(0)).fillna(0)

    SourcesByPHUYesterday = pd.pivot_table(MasterDataFrame[(MasterDataFrame['File_Date']==MasterDataFrame['File_Date'].max()-pd.DateOffset(days=1))],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'Case_AcquisitionInfo',aggfunc=np.count_nonzero, margins = False).fillna(0)
    SourcesByPHUToday = pd.pivot_table(MasterDataFrame[(MasterDataFrame['File_Date']==MasterDataFrame['File_Date'].max())],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'Case_AcquisitionInfo',aggfunc=np.count_nonzero, margins = False).fillna(0)
    changeInSourcesByPHUToday = SourcesByPHUToday - SourcesByPHUYesterday

    #changeInSourcesByPHUWeek = ((changeInSourcesByPHUWeek.T/changeInSourcesByPHUWeek.T.sum()*100).round(1)).T

    AgesByPHUYesterday = pd.pivot_table(MasterDataFrame[(MasterDataFrame['File_Date']==MasterDataFrame['File_Date'].max()-pd.DateOffset(days=1))],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'Age_Group',aggfunc=np.count_nonzero, margins = False).fillna(0)
    AgesByPHUToday = pd.pivot_table(MasterDataFrame[(MasterDataFrame['File_Date']==MasterDataFrame['File_Date'].max())],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'Age_Group',aggfunc=np.count_nonzero, margins = False).fillna(0)
    changeInAgesByPHUToday = AgesByPHUToday - AgesByPHUYesterday
    changeInAgesByPHUWeek = (pd.pivot_table(MasterDataFrame[(MasterDataFrame['File_Date']==MasterDataFrame['File_Date'].max())],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'Age_Group',aggfunc=np.count_nonzero, margins = False).fillna(0) - pd.pivot_table(MasterDataFrame[(MasterDataFrame['File_Date']==MasterDataFrame['File_Date'].max()-pd.DateOffset(days=7))],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'Age_Group',aggfunc=np.count_nonzero, margins = False).fillna(0)).fillna(0)
    changeInAgesByPHUWeek.rename(columns = {'19 & under':'<20','Unknown':'N/A'},inplace=True)

    changeInCasesByPHU = casesByPHU.copy()
    for x in range(changeInCasesByPHU.shape[1]-1):
        changeInCasesByPHU[changeInCasesByPHU.columns[x]] = changeInCasesByPHU.iloc[:,x] - changeInCasesByPHU.iloc[:,x+1]

    df = changeInCasesByPHU.astype(int).T
    df = df[df.index != datetime.datetime(2020,4,20)]


    stats = pd.DataFrame([df.iloc[0],df[0:7].mean(),df[7:14].mean().round(1),
                      df[0:7].sum().round(1),df[7:14].sum().round(1),
                      df[(df.index.month == 7) & (df.index.year == 2021)].mean().round(1),
                      df[(df.index.month == 6) & (df.index.year == 2021)].mean().round(1),
                      df[(df.index.month == 5) & (df.index.year == 2021)].mean().round(1),
                      df[(df.index.month == 4) & (df.index.year == 2021)].mean().round(1),
                      df[(df.index.month == 3) & (df.index.year == 2021)].mean().round(1),
                      df[(df.index.month == 2) & (df.index.year == 2021)].mean().round(1),
                      df[(df.index.month == 1) & (df.index.year == 2021)].mean().round(1),
                      df[df.index.month == 12].mean().round(1),
                      df[df.index.month == 11].mean().round(1),
                      df[df.index.month == 10].mean().round(1),
                      df[df.index.month == 9].mean().round(1),df[df.index.month == 8].mean().round(1),
                      df[df.index.month == 7].mean().round(1),df[df.index.month == 6].mean().round(1),
                      df[(df.index.month == 5) & (df.index.year == 2020)].mean().round(1),
                      df[df.index.dayofweek == 0].mean().round(1),df[df.index.dayofweek == 1].mean().round(1),df[df.index.dayofweek == 2].mean().round(1),df[df.index.dayofweek == 3].mean().round(1),df[df.index.dayofweek == 4].mean().round(1),df[df.index.dayofweek == 5].mean().round(1),df[df.index.dayofweek == 6].mean().round(1)]).transpose()
    stats.columns = ['Today','Last 7','Prev 7','Last 7Ã¢Ë†â€˜','Prev 7Ã¢Ë†â€˜','July','June','May','April','Mar','Feb','Jan','Dec','Nov','Oct','Sep','Aug','Jul', 'Jun','May 2020','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
    stats.sort_values(by=stats.columns[0],ascending = False, inplace = True)
    #stats = pd.merge(stats,changeInSourcesByPHUToday,on='Reporting_PHU')
    stats = pd.merge(stats,changeInSourcesByPHUWeek,on='Reporting_PHU')

    stats = pd.merge(stats,changeInAgesByPHUWeek,on='Reporting_PHU')
    stats = pd.merge(stats,PHUPopulation(),on='Reporting_PHU')
    stats = pd.merge(stats,pivot_Active,on='Reporting_PHU')

    totals = stats.sum()
    totals.name = 'Total'

    restNewCaseStats = stats.loc[abs(stats['Today'])==0].sum()
    restNewCaseStats.name = 'Regions of Zeroes'
    stats = stats.loc[abs(stats['Today'])>=1]
    stats = stats.append(totals.transpose())
    stats.sort_values(by='Today',ascending = False, inplace = True)
    stats = stats.append(restNewCaseStats.transpose())


    stats['Close contact'] = (stats['Close contact']/stats['Last 7Ã¢Ë†â€˜']*100).round(1)
    stats['Community'] = (stats['Community']/stats['Last 7Ã¢Ë†â€˜']*100).round(1)
    stats['Outbreak'] = (stats['Outbreak']/stats['Last 7Ã¢Ë†â€˜']*100).round(1)
    stats['Travel'] = (stats['Travel']/stats['Last 7Ã¢Ë†â€˜']*100).round(1)
    stats['Last 7/100k'] = (stats['Last 7Ã¢Ë†â€˜']/stats['Population']*100000).round(1)
    stats['Prev 7/100k'] = (stats['Prev 7Ã¢Ë†â€˜']/stats['Population']*100000).round(1)
    stats['Active/100k'] = (stats['ActiveCases']/stats['Population']*100000).round(1)

    for column in changeInAgesByPHUWeek.columns:
        stats[column] = ((stats[column]/stats['Last 7Ã¢Ë†â€˜'])*100).round(1)
    changeInAgesByPHUWeekGrouped = pd.DataFrame([ stats['20s'],stats[['30s','40s']].sum(axis=1),stats[['50s','60s']].sum(axis=1), stats[['70s','80s','90+']].sum(axis=1)]).T
    changeInAgesByPHUWeekGrouped.columns = ['20-29','30-49','50-69','70+']
    stats = pd.merge(stats,changeInAgesByPHUWeekGrouped,on='Reporting_PHU')
    stats = stats.round(1)
    stats['Today'] = stats['Today'].astype(int)


    stats.insert(1,'Averages->>',"")
    stats.insert(1,'Source (week %)->>',"")
    stats.insert(1,'Ages (week %)->>',"")
    stats.insert(1,'Totals Per 100k->>',"")
    stats.insert(1,'More Averages->>',"")
    stats.insert(1,'Day of Week->>',"")

    OntarioZonesDF = pd.read_pickle('Pickle/OntarioZones.pickle')
    stats = pd.merge(stats,OntarioZonesDF,on='Reporting_PHU',how = 'left')
    stats = stats.rename(columns={"Status_PHU":"Zone"})

    #stats = stats[['Today','Ages->>','19 & under','20s','30s','40s','50s','60s','70s','80s','90s','Unknown','Source->>','Close contact','Community','Outbreak','Travel','Averages->>','Last 7ÃŽÂ¼','Prev 7ÃŽÂ¼','Sep','Aug','Jul','Jun','May']]
    #stats = stats[['Today','Averages->>','Last 7','Prev 7','Per 100k->>','Last 7/100k','Prev 7/100k','Source (week %)->>','Close contact','Community','Outbreak','Travel','More Averages->>','Oct','Sep','Aug','Jul','Jun','May','Day of Week->>','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']]
    stats = stats[['Today','Averages->>','Last 7','Prev 7','Totals Per 100k->>','Last 7/100k','Prev 7/100k','Active/100k','Ages (week %)->>','<20','20-29','30-49','50-69','70+','Source (week %)->>','Close contact','Community','Outbreak','Travel','More Averages->>','July','June','May','April','Mar','Feb','Jan','Dec','Nov','Oct','Sep','Aug','Jul','Jun','May 2020','Day of Week->>','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']]
    stats['Last 7'] = stats['Last 7'].round(1)

    statsT = stats.T
    statsT.insert(1,' ',"")
    stats = (statsT.T).copy()
    stats = PHUWebsiteReplacements(stats).copy()

    stats.to_pickle('Pickle/NewCasesTable.pickle')

    with open('FFF.csv', 'w',newline='') as f:
        f.write('Summary PHU \n')
        stats.to_csv(f,header = True)
        f.write('\n')


    header = 'PHU|'
    with open('TextOutput/NewCasesTable.txt','w',newline = '') as f:
        stats = stats[['Today','Averages->>','Last 7','Prev 7','Totals Per 100k->>','Last 7/100k','Prev 7/100k','Ages (week %)->>','<20','20-29','30-49','50-69','70+','Source (week %)->>','Close contact','Community','Outbreak','Travel']]
        #stats = stats[['Today','Averages->>','Last 7','Prev 7','Totals Per 100k->>','Last 7/100k','Prev 7/100k','Active/100k','Source (week %)->>','Close contact','Community','Outbreak','Travel','Ages (week %)->>','<40','40-69','70+','Day of Week->>','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']]

        for column in range(0,stats.columns.size):
            header = header+stats.columns[column]+'|'
        f.write(header)
        f.write('\n')
        f.write(':--|--:|:-:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|')
        #f.write(':--|--:|:-:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|')
        #f.write(':--|--:|:-:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|')
        f.write('\n')
        stats.to_csv(f,header=False,sep = '|')
        f.write('\n')

    # with open(textfilename, 'w',newline='') as f:
    #     f.write('Summary PHU \n')
    #     stats.to_csv(f,header = False, sep = '|')
    #     f.write('\n')
    del stats
    print(round((time.time()-starttime),2),'- New Cases table done')

    ############################################################################################
    ############################################################################################
    "Case count by Age"
    casesByAge = pd.pivot_table(MasterDataFrame,values = 'Row_ID',index = ['Age_Group'],columns = 'File_Date',aggfunc=np.count_nonzero)
    casesByAge = casesByAge.reindex(columns=sorted(casesByAge.columns,reverse = True))
    casesByAge.sort_index(ascending = True, inplace = True)

    changeInCasesByAge = casesByAge-casesByAge.shift(-1,axis=1)

    changeInCasesByAge.to_pickle('Pickle/ChangeInCasesByAge.pickle')
    changeInCasesByAge.to_csv('CSV/ChangeInCasesByAge.csv')
    changeInCasesByAge.insert(0,'TotalToDate',casesByAge[casesByAge.columns[0]])
    with open('PivotTable.csv', 'a',newline='') as f:
        f.write('Change in Cases by Age Group \n')
        changeInCasesByAge.to_csv(f,header = True)
        f.write('\n')
    print(round((time.time()-starttime),2),'- Case count by age done')

    ############################################################################################
    ############################################################################################

    "death count by PHU"
    deathsByPHU = pd.pivot_table(MasterDataFrame[(MasterDataFrame['Outcome']=="Fatal")],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'File_Date',aggfunc=np.count_nonzero)
    deathsByPHU = deathsByPHU.reindex(columns=sorted(deathsByPHU.columns,reverse = True))
    deathsByPHU.sort_values(by=deathsByPHU.columns[0],ascending = False, inplace = True)

    "Change in death count by PHU"
    changeInDeathsByPHU = deathsByPHU-deathsByPHU.shift(-1,axis=1)
    changeInDeathsByPHU.to_pickle('Pickle/changeInDeathsByPHU.pickle')
    changeInDeathsByPHU.to_csv('CSV/changeInDeathsByPHU.csv')



    changeInDeathsByPHU.insert(0,'TotalToDate',deathsByPHU[deathsByPHU.columns[0]])
    with open('PivotTable.csv', 'a',newline='') as f:
        f.write('Change In Deaths by PHU \n')
        changeInDeathsByPHU.to_csv(f,header = True)
        f.write('\n')
    print(round((time.time()-starttime),2),'- Death count by PHU done')


    ############################################################################################
    ############################################################################################

    "Case count by Outcome"
    casesByOutcome = pd.pivot_table(MasterDataFrame,values = 'Row_ID',index = ['Outcome'],columns = 'File_Date',aggfunc=np.count_nonzero)
    casesByOutcome = casesByOutcome.reindex(columns=sorted(casesByOutcome.columns,reverse = True))
    casesByOutcome.sort_index(ascending = True, inplace = True)
    casesByOutcome.fillna(0, inplace = True)
    #casesByOutcome.to_pickle('Pickle/CasesByOutcome.pickle')

    "Change in Case count by Outcome"
    changeInCasesByOutcome = casesByOutcome-casesByOutcome.shift(-1,axis=1)
    changeInCasesByOutcome.to_pickle('Pickle/ChangeInCasesByOutcome.pickle')
    changeInCasesByOutcome.to_csv('CSV/ChangeInCasesByOutcome.csv')


    changeInCasesByOutcome.insert(0,'TotalToDate',casesByOutcome[casesByOutcome.columns[0]])
    with open('PivotTable.csv', 'a',newline='') as f:
        f.write('changeInCases by Outcome \n')
        changeInCasesByOutcome.to_csv(f,header = True)
        f.write('\n')

    print(round((time.time()-starttime),2),'- Change in cases by outcome done')

    ############################################################################################
    ############################################################################################

    "ActiveCaseByPHU"
    activeCasesByPHU = pd.pivot_table(MasterDataFrame[(MasterDataFrame['Outcome']=="Not Resolved")],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'File_Date',aggfunc=np.count_nonzero)
    activeCasesByPHU = activeCasesByPHU.reindex(columns=sorted(activeCasesByPHU.columns,reverse = True))
    activeCasesByPHU.sort_values(by=activeCasesByPHU.columns[0],ascending = False, inplace = True)
    activeCasesByPHU = activeCasesByPHU.fillna(0)

    "ChangeActiveCaseByPHU"
    changeInActiveCasesByPHU = activeCasesByPHU - activeCasesByPHU.shift(-1,axis=1)
    changeInActiveCasesByPHU.to_pickle('Pickle/ChangeInActiveCasesByPHU.pickle')
    changeInActiveCasesByPHU.to_csv('CSV/ChangeInActiveCasesByPHU.csv')


    changeInActiveCasesByPHU.insert(0,'TotalToDate',activeCasesByPHU[activeCasesByPHU.columns[0]])
    with open('PivotTable.csv', 'a',newline='') as f:
        f.write('changeInActive by PHU \n')
        changeInActiveCasesByPHU.to_csv(f,header = True)
        f.write('\n')

    with open('PivotTable.csv', 'a',newline='') as f:
        f.write('Active by PHU \n')
        activeCasesByPHU.to_csv(f,header = True)
        f.write('\n')

    print(round((time.time()-starttime),2),'- Active cases by PHU done')

    ############################################################################################
    ############################################################################################

    "Case count by Outcome by Age"
    #casesByOutcomeByAge = pd.pivot_table(MasterDataFrame,values = 'Row_ID',index = ['Outcome','Age_Group'],columns = 'File_Date',aggfunc=np.count_nonzero)
    casesByOutcomeByAge = pd.pivot_table(MasterDataFrame,values = 'Row_ID',index = ['Outbreak_Related','Outcome','Age_Group'],columns = 'File_Date',aggfunc=np.count_nonzero)
    casesByOutcomeByAge = casesByOutcomeByAge.reindex(columns=sorted(casesByOutcomeByAge.columns,reverse = True))
    casesByOutcomeByAge.sort_index(ascending = True, inplace = True)
    casesByOutcomeByAge.fillna(0, inplace = True)
    casesByOutcomeByAge.to_pickle('Pickle/casesByOutcomeByAge.pickle')


    with open('PivotTable.csv', 'a',newline='') as f:
        f.write('Cases by Outcome by Age \n')
        #casesByOutcomeByAge.to_csv(f,header = True)
        casesByOutcomeByAge.loc["No"].add(casesByOutcomeByAge.loc["Yes"],fill_value=0).to_csv(f,header = True)
        f.write('\n')

    print(round((time.time()-starttime),2),'- Cases by outcome by age done')


    "Case count by Outcome by Age"
    changeInCasesByOutcomeByAge = casesByOutcomeByAge - casesByOutcomeByAge.shift(-1,axis=1)
    changeInCasesByOutcomeByAge.to_pickle('Pickle/changeInCasesByOutcomeByAge.pickle')
    changeInCasesByOutcomeByAge.to_csv('CSV/changeInCasesByOutcomeByAge.csv')
    changeInCasesByOutcomeByAge.insert(0,'TotalToDate',casesByOutcomeByAge[casesByOutcomeByAge.columns[0]])

    casesByOutcomeByAgeByOutbreakStatus = pd.pivot_table(MasterDataFrame[(MasterDataFrame['Outcome']=='Not Resolved') & (MasterDataFrame['File_Date']>= datetime.datetime(2020,6,17))],values = 'Row_ID',index = ['Outcome','Age_Group','Case_AcquisitionInfo'],columns = 'File_Date',aggfunc=np.count_nonzero)
    activeCases70PlusOutbreakStatus = pd.DataFrame([casesByOutcomeByAgeByOutbreakStatus.loc[('Not Resolved',(['70s','80s','90s']),['Travel','Community','Close contact'])].sum(),casesByOutcomeByAgeByOutbreakStatus.loc[('Not Resolved',(['70s','80s','90s']),'Outbreak')].sum()],index=['Other','Outbreak'])
    activeCases70PlusOutbreakStatus = activeCases70PlusOutbreakStatus.reindex(columns=sorted(activeCases70PlusOutbreakStatus.columns,reverse = True))

    with open('PivotTable.csv', 'a',newline='') as f:
        f.write('changeInCases by Outcome by Age \n')
        changeInCasesByOutcomeByAge.to_csv(f,header = True)
        f.write('\n')
        f.write('ActiveCases70PlusByOutbreak /n')
        activeCases70PlusOutbreakStatus.to_csv(f,header = True)
        f.write('\n')

    print(round((time.time()-starttime),2),'- Case count by outcome by age done')

    ############################################################################################
    ############################################################################################

    "Case count by Source"
    "MasterDataFrame = pd.MultiIndex.from_frame(MasterDataFrame)"
    casesBySource = pd.pivot_table(MasterDataFrame,values = 'Row_ID',index = ['Case_AcquisitionInfo'],columns = 'File_Date',aggfunc=np.count_nonzero, margins = False)
    casesBySource.sort_values(by=['Case_AcquisitionInfo'],ascending =[True],inplace = True)
    casesBySource = casesBySource.reindex(columns=sorted(casesBySource.columns,reverse = True))
    casesBySource.fillna(0,inplace=True)

    changeInCasesBySource = casesBySource - casesBySource.shift(-1,axis=1)
    changeInCasesBySource.to_pickle('Pickle/changeInCasesBySource.pickle')
    changeInCasesBySource.to_csv('CSV/changeInCasesBySource.csv')

    changeInCasesBySource.insert(0,'TotalToDate',casesBySource[casesBySource.columns[0]])
    with open('PivotTable.csv', 'a',newline='') as f:
        f.write('Change in Case count by source \n')
        changeInCasesBySource.to_csv(f,header = True)
        f.write('\n')

    print(round((time.time()-starttime),2),'- Case count by source done')


    ############################################################################################
    ############################################################################################
    "Fatality rate tables"

    df = pd.read_pickle('Pickle/changeInCasesByOutcomeByAge.pickle')
    Rolling30DayOutbreak_Fatal = df.loc['Yes'].loc['Fatal'].T[::-1].rolling(30).sum().fillna(0)[::-1].T
    Rolling30DayOutbreak_Resolved = df.loc['Yes'].loc['Resolved'].T[::-1].rolling(30).sum().fillna(0)[::-1].T
    Rolling30DayNonOutbreak_Fatal = df.loc['No'].loc['Fatal'].T[::-1].rolling(30).sum().fillna(0)[::-1].T
    Rolling30DayNonOutbreak_Resolved = df.loc['No'].loc['Resolved'].T[::-1].rolling(30).sum().fillna(0)[::-1].T
    Rolling30DayOutbreak_FatalRate = Rolling30DayOutbreak_Fatal/(Rolling30DayOutbreak_Resolved+Rolling30DayOutbreak_Fatal)
    Rolling30DayNonOutbreak_FatalRate = Rolling30DayNonOutbreak_Fatal/(Rolling30DayNonOutbreak_Resolved+Rolling30DayNonOutbreak_Fatal)
    with open('PivotTable.csv', 'a',newline='') as f:
        f.write('FatalityRate - Outbreak')
        Rolling30DayOutbreak_FatalRate.to_csv(f,header = True)
        f.write('\n')
        f.write('FatalityRate - Non-outbreak')
        Rolling30DayNonOutbreak_FatalRate.to_csv(f,header = True)
        f.write('\n')

    print(round((time.time()-starttime),2),'- Fatality rate tables done')


    ############################################################################################
    ############################################################################################
    "Count by Episode Date"

    ReportingLagPivot = pd.pivot_table(MasterDataFrame,values = 'Row_ID',index = ['Reporting_Lag'],columns = 'File_Date',aggfunc=np.count_nonzero)
    ReportingLagPivot.fillna(0,inplace=True)
    ReportingLagPivot = ReportingLagPivot.reindex(columns=sorted(ReportingLagPivot.columns,reverse = True))
    changeInReportingLag = (ReportingLagPivot - ReportingLagPivot.shift(-1,axis=1).shift(1,axis=0)).ffill()

    with open('PivotTable.csv', 'a',newline='') as f:
        f.write('Episode Table 2 - Positive cases with episode dates X days before report date \n')
        TempReportingLagPivot = ReportingLagPivot.copy()
        TempReportingLagPivot.index = TempReportingLagPivot.index.days
        TempReportingLagPivot.iloc[1:31,:].to_csv(f,header = True)
        #ReportingLagPivot.loc['1 days': '30 days'].to_csv(f,header = True)
        f.write('\n')


    tempDataFrame = pd.DataFrame()
    for x in range(1,31):
        b = ReportingLagPivot.loc[datetime.timedelta(days=1):datetime.timedelta(days=x)].sum(axis=0)
        tempDataFrame = pd.concat([tempDataFrame,b.to_frame(name=x).T])
    with open('PivotTable.csv', 'a',newline='') as f:
        f.write('Episode Table 1 - Cumulative Positive cases with episode dates X days before report date \n')
        #tempDataFrame.loc['0 days': '30 days'].to_csv(f,header = True)
        tempDataFrame.to_csv(f,header = True)
        f.write('\n')

    with open('PivotTable.csv', 'a',newline='') as f:
        f.write("Table 3 - How long ago do  the day's new cases relate to? \n")
        changeInReportingLag.iloc[1:31,:].to_csv(f,header = True)
        changeInReportingLag.iloc[31:].sum(axis=0).to_frame(name='Over 30 days').transpose().to_csv(f,header = False)
        f.write('\n')

    changeInReportingLag = changeInReportingLag.fillna(0)
    changeInReportingLag.to_pickle('Pickle/Tab2Table3CasesByEpisodeDate.pickle')


    casesByEpisodeDate = pd.pivot_table(MasterDataFrame,values = 'Row_ID',index = ['Episode_Date'],columns = 'File_Date',aggfunc=np.count_nonzero, margins = False)
    casesByEpisodeDate.fillna(0, inplace = True)
    casesByEpisodeDate.sort_values(by=['Episode_Date'],ascending =[False],inplace = True)
    casesByEpisodeDate = casesByEpisodeDate.reindex(columns=sorted(casesByEpisodeDate.columns,reverse = True))
    # with open('PivotTable.csv', 'a',newline='') as f:
    #     f.write('Case count by Episode Date \n')
    #     casesByEpisodeDate.to_csv(f,header = True)
    #     f.write('\n')
    changeInCasesByEpisodeDate = casesByEpisodeDate - casesByEpisodeDate.shift(-1,axis=1)
    changeInCasesByEpisodeDate.insert(0,'TotalToDate',casesByEpisodeDate[casesByEpisodeDate.columns[0]])
    # with open('PivotTable.csv', 'a',newline='') as f:
    #     f.write('Change in Case count by Episode Date \n')
    #     changeInCasesByEpisodeDate.to_csv(f,header = True)
    #     f.write('\n')

    # ReportingLagPivot.loc[pd.to_timedelta('2 days')]
    # ReportingLagPivot.loc[datetime.timedelta(days=0):datetime.timedelta(days=6)]
    # ReportingLagPivot.loc[datetime.timedelta(days=0):datetime.timedelta(days=6)].sum(axis=0)

    ww = pd.pivot_table(MasterDataFrame,values = 'Row_ID',index = ['Episode_Date'],columns = 'Reporting_Lag',aggfunc=np.count_nonzero)
    ww.sort_values(by=['Episode_Date'],ascending =[False],inplace = True)
    ww.insert(0,'TotalToDate',casesByEpisodeDate[casesByEpisodeDate.columns[0]])
    ww.fillna(0, inplace = True)
    with open('PivotTable.csv', 'a',newline='') as f:
        f.write('Cumulative Case count - days since Episode Date \n')
        ww.iloc[0:240,0:300].to_csv(f,header = True)
        f.write('\n')

    print(round((time.time()-starttime),2),'- Case count by episode date done')

    ############################################################################################
    ############################################################################################

    "Case count by PHU/Source"
    "MasterDataFrame = pd.MultiIndex.from_frame(MasterDataFrame)"
    # casesByPHUAndSource = pd.pivot_table(MasterDataFrame,values = 'Row_ID',index = ['Reporting_PHU','Case_AcquisitionInfo'],columns = 'File_Date',aggfunc=np.count_nonzero, margins = False)
    # casesByPHUAndSource.sort_values(by=['Reporting_PHU','Case_AcquisitionInfo'],ascending =[True,True],inplace = True)
    # casesByPHUAndSource = casesByPHUAndSource.reindex(columns=sorted(casesByPHUAndSource.columns,reverse = True))
    # for x in range(casesByPHUAndSource.shape[1]-1):
    #     casesByPHUAndSource[casesByPHUAndSource.columns[x]] = casesByPHUAndSource.iloc[:,x] - casesByPHUAndSource.iloc[:,x+1]
    # with open('FFF.csv', 'a',newline='') as f:
    #     f.write('Case count by PHU/source \n')
    #     casesByPHUAndSource.to_csv(f,header = True)
    #     f.write('\n')

    ############################################################################################
    ############################################################################################

    "Deaths detail"
    RecentDF = MasterDataFrame[MasterDataFrame['File_Date'] >= MasterDataFrame['File_Date'].max()-datetime.timedelta(days=1)]
    """ Deaths detail for ALL dates, not just current:
    DeathsByPHUDetail = pd.pivot_table(MasterDataFrame[(MasterDataFrame['Outcome']=="Fatal")],values = 'Row_ID',index = ['Reporting_PHU','Age_Group','Client_Gender','Case_AcquisitionInfo','Case_Reported_Date','Episode_Date'],columns = 'File_Date',aggfunc=np.count_nonzero)
    """
    DeathsByPHUDetail = pd.pivot_table(RecentDF[(RecentDF['Outcome']=="Fatal")],values = 'Row_ID',index = ['Reporting_PHU','Age_Group','Client_Gender','Case_AcquisitionInfo','Case_Reported_Date','Episode_Date'],columns = 'File_Date',aggfunc=np.count_nonzero)

    DeathsByPHUDetail = DeathsByPHUDetail.reindex(columns=sorted(DeathsByPHUDetail.columns,reverse = True))
    DeathsByPHUDetail.sort_values(by=DeathsByPHUDetail.columns[0],ascending = False, inplace = True)
    DeathsByPHUDetail = DeathsByPHUDetail.fillna(0)
    changeInDeathsByPHUDetail = (DeathsByPHUDetail - DeathsByPHUDetail.shift(-1,axis = 1))
    changeInDeathsByPHUDetail = changeInDeathsByPHUDetail.fillna(0).astype(int)


    changeInDeathsByPHUDetail = changeInDeathsByPHUDetail.sort_values(by=['Age_Group','Reporting_PHU','Client_Gender','Case_Reported_Date'],ascending = [True,True,False,False])

    with open('FFF.csv', 'a',newline='') as f:
        f.write('Change in Deaths by PHU \n')
        changeInDeathsByPHUDetail[abs(changeInDeathsByPHUDetail[changeInDeathsByPHUDetail.columns[0]])>0].to_csv(f,header = True)
        f.write('\n')

    with open(DeathDetailFileName, 'w',newline='') as f:
        f.write('|Reporting_PHU|Age_Group|Client_Gender|Case_AcquisitionInfo|Case_Reported_Date|Episode_Date|Count| \n')
        f.write(':--|:--|:--|:--|:--|:--|--:|\n')
        changeInDeathsByPHUDetail[abs(changeInDeathsByPHUDetail[changeInDeathsByPHUDetail.columns[0]])>0][changeInDeathsByPHUDetail.columns[0]].to_csv(f,header = False, sep='|')



    print("* *"+format((changeInCasesByAge.iloc[0:3,:].sum()/changeInCasesByAge.iloc[:,:].sum())[1],".1%")+" of today's cases are in people under the age of 40.* /s")
    print("* "+format((changeInCasesByAge.iloc[6:9,1].sum()/changeInCasesByAge.iloc[:,1].sum()),".1%")+" or "+format(int(changeInCasesByAge.iloc[6:9,1].sum()),",d")+" of today's cases are in people aged 70+ - [Chart of active 70+ cases](https://docs.google.com/spreadsheets/d/e/2PACX-1vQ7fegCALd11ElozUYcMi-e9Dj69YaiNQhvEpk81JHsyTACl0UXkWK5zfMNFe49Tq3VuN9Av-fuEZqV/pubchart?oid=365228609&format=interactive)")

    print(round((time.time()-starttime),2),'- Deaths detail done')


    #########################################################################
    #------------------------------------------------------------------------
    #Pivot table showing episode dates by PHU for current day's reports

    EpisodeDateByPHUToday = pd.pivot_table(MasterDataFrame[MasterDataFrame['File_Date']==MasterDataFrame['File_Date'].max()],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'Episode_Date',fill_value = 0,aggfunc = np.count_nonzero,margins = False).sort_index(axis = 1,ascending = False)
    EpisodeDateByPHUYesterday = pd.pivot_table(MasterDataFrame[MasterDataFrame['File_Date']==(MasterDataFrame['File_Date'].max()-datetime.timedelta(days = 1))],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'Episode_Date',fill_value = 0,aggfunc = np.count_nonzero,margins = False).sort_index(axis = 1,ascending = False)

    if EpisodeDateByPHUToday.columns.size >= EpisodeDateByPHUYesterday.columns.size:
        for x in range(EpisodeDateByPHUYesterday.columns.size-1):
            if EpisodeDateByPHUYesterday.columns[x] not in EpisodeDateByPHUToday.columns:
                #print (EpisodeDateByPHUYesterday.columns[x])
                EpisodeDateByPHUToday.insert(EpisodeDateByPHUToday.columns.size-1, EpisodeDateByPHUYesterday.columns[x],0)

        for x in range(EpisodeDateByPHUToday.columns.size-1):
            if EpisodeDateByPHUToday.columns[x] not in EpisodeDateByPHUYesterday.columns:
                #print (EpisodeDateByPHUToday.columns[x])
                EpisodeDateByPHUYesterday.insert(EpisodeDateByPHUYesterday.columns.size-1, EpisodeDateByPHUToday.columns[x],0)

    else:
        for x in range(EpisodeDateByPHUToday.columns.size-1):
            if EpisodeDateByPHUToday.columns[x] not in EpisodeDateByPHUYesterday.columns:
                #print (EpisodeDateByPHUToday.columns[x])
                EpisodeDateByPHUYesterday.insert(EpisodeDateByPHUYesterday.columns.size-1, EpisodeDateByPHUToday.columns[x],0)

        for x in range(EpisodeDateByPHUYesterday.columns.size-1):
            if EpisodeDateByPHUYesterday.columns[x] not in EpisodeDateByPHUToday.columns:
                #print (EpisodeDateByPHUYesterday.columns[x])
                EpisodeDateByPHUToday.insert(EpisodeDateByPHUToday.columns.size-1, EpisodeDateByPHUYesterday.columns[x],0)

    EpisodeDateByPHUToday = EpisodeDateByPHUToday.sort_index(axis = 1,ascending = False)
    EpisodeDateByPHUYesterday = EpisodeDateByPHUYesterday.sort_index(axis = 1,ascending = False)
    TodaysEpisodeDatesByPHU = EpisodeDateByPHUToday-EpisodeDateByPHUYesterday
    TodaysEpisodeDatesByPHU = TodaysEpisodeDatesByPHU.sort_index(axis = 1,ascending = False)

    TodaysEpisodeDatesByPHU.insert(0,'Today',TodaysEpisodeDatesByPHU.sum(axis=1))
    TodaysEpisodeDatesByPHU = TodaysEpisodeDatesByPHU.sort_values(by = TodaysEpisodeDatesByPHU.columns[0],ascending = False)

    TodaysEpisodeDatesByPHU.to_csv('CSV/TodaysEpisodeDatesByPHU.csv')
    TodaysEpisodeDatesByPHU.to_pickle('Pickle/TodaysEpisodeDatesByPHU.pickle')

    with open('PivotTable.csv', 'a',newline='') as f:
        f.write("Current day's cases by episode date by PHU \n")
        TodaysEpisodeDatesByPHU.to_csv(f,header = True)
        f.write('\n')

    print(round((time.time()-starttime),2),'- Eppisode dates by PHU for todays cases done')


    #########################################################################
    #########################################################################
    #How many days ago of episode dates do the day's new cases relate to?
    ReportingLagPivot_WithPHU = pd.pivot_table(MasterDataFrame,values = 'Row_ID',index = ['Reporting_PHU','Reporting_Lag'],columns = 'File_Date',aggfunc=np.count_nonzero)
    ReportingLagPivot_WithPHU.fillna(0,inplace=True)
    ReportingLagPivot_WithPHU = ReportingLagPivot_WithPHU.reindex(columns=sorted(ReportingLagPivot_WithPHU.columns,reverse = True))
    changeInReportingLag_WithPHU = (ReportingLagPivot_WithPHU - ReportingLagPivot_WithPHU.shift(-1,axis=1).shift(1,axis=0)).ffill()
    changeInCasesByPHU = pd.read_pickle('Pickle/ChangeInCasesByPHU.pickle')

    with open('PivotTable.csv', 'a',newline='') as f:


        for x in list(changeInCasesByPHU.index[0:15]):
            f.write("Table X - How long ago do  the day's new cases relate to? - ,")

            f.write(x)
            f.write('\n')

            changeInReportingLag_WithPHU.loc[x].loc['1 days':'10 days'].to_csv(f,header = True)
            changeInReportingLag_WithPHU.loc[x].loc['11 days':].sum(axis=0).to_frame(name = 'Over 10 days').transpose().to_csv(f,header = False)
            #changeInReportingLag_PHU.loc['0 day' :'10 days',:].to_csv(f,header = True)
            #changeInReportingLag_PHU.loc['11 days':].sum(axis=0).to_frame(name='Over 10 days').transpose().to_csv(f,header = False)
            #f.write('\n')
    #########################################################################
    #########################################################################

    del MasterDataFrame
    print('Daily Procedures: ', round(time.time()-starttime,2),'seconds')
    print('------------------------------------------------------------------------')


def TorontoCOVID():
    print('------------------------------------------------------------------------')
    print(f'TorontoCOVID \nStarted: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}')
    starttime = time.time()
    dateRange = pd.date_range(start='2020-02-01', end='2022-01-01', freq="m")
    dateRange = dateRange[::-1]

    filename = 'FilesToUpload/TorontoCOVID.csv'
    f = open(filename, 'w')
    f.close()

    df = pd.read_csv('https://ckan0.cf.opendata.inter.prod-toronto.ca/download_resource/e5bf35bc-e681-43da-b2ce-0242d00922ad?format=csv')
    df.to_csv('SourceFiles/TorontoCOVID_Data.csv')
    df['Episode Date'] = pd.to_datetime(df['Episode Date'], format='%Y-%m-%d')
    # TorontoPivot = pd.pivot_table(df, values='Assigned_ID', index=['Age Group'],
    #                               columns=['Ever Hospitalized', 'Ever in ICU'],
    #                               aggfunc=np.count_nonzero, margins=True)
    df['Reported Date'] = pd.to_datetime(df['Reported Date'], format='%Y-%m-%d')
    # CompletedCasesToronto = pd.pivot_table(df, values='Assigned_ID', index=['Outcome'],
    #                                        aggfunc=np.count_nonzero, margins=True)
    abc = pd.pivot_table(df, values='Assigned_ID',
                         index=['Age Group', 'Ever Hospitalized', 'Ever in ICU'],
                         aggfunc=np.count_nonzero, margins=True)
    abc = (abc.Assigned_ID / abc.groupby(level=0).Assigned_ID.transform(sum) * 100).round(2)

    # return df

    # temp = pd.pivot_table(df,values = 'Assigned_ID',index = ['Age Group','Ever Hospitalized','Ever in ICU'],aggfunc=np.count_nonzero, margins=True)
    # for months in range(11,1,-1):
    #     abc = pd.pivot_table(df[df['Reported Date'].dt.month == months],values = 'Assigned_ID',index = ['Age Group','Ever Hospitalized','Ever in ICU'],aggfunc=np.count_nonzero, margins=True)
    #     abc = (abc.Assigned_ID / abc.groupby(level=0).Assigned_ID.transform(sum) * 100).round(2)
    #     abc.name = months
    #     #abc.set_index(['Age Group', 'Ever Hospitalized', 'Ever in ICU'],inplace = True)
    #     temp = pd.merge(temp,abc, how = 'left', on = ['Age Group','Ever Hospitalized','Ever in ICU'])
    #     #print(abc)

    #     print(temp)
    # temp.fillna(0,inplace = True)
    # temp.to_csv('aaa.csv')

    temp = pd.pivot_table(df[df['Outcome'] == 'RESOLVED'],
                          values='Assigned_ID',
                          index=['Age Group', 'Ever Hospitalized', 'Ever in ICU'],
                          aggfunc=np.count_nonzero, margins=True)

    # for months in range(11,1,-1):
    #     abc = pd.pivot_table(df[(df['Outcome'] == 'RESOLVED') & (df['Reported Date'].dt.month == months)],values = 'Assigned_ID',index = ['Age Group','Ever Hospitalized','Ever in ICU'],aggfunc=np.count_nonzero, margins=True)
    #     #abc = (abc.Assigned_ID / abc.groupby(level=0).Assigned_ID.transform(sum) * 100).round(2)
    #     abc.name = months
    #     #abc.set_index(['Age Group', 'Ever Hospitalized', 'Ever in ICU'],inplace = True)
    #     temp = pd.merge(temp,abc, how = 'left', on = ['Age Group','Ever Hospitalized','Ever in ICU'])
    #     #print(abc)

    #     print(temp)
    # temp.fillna(0,inplace = True)
    # temp.to_csv('aab.csv')

    #######################################################
    # This section takes all the Toronto data and filters to show the percent of people that were admitted to
    # the ICU based on the month of the Reporting date.
    # temp = pd.pivot_table(df,values = 'Assigned_ID',index = ['Age Group','Ever Hospitalized','Ever in ICU'],aggfunc=np.count_nonzero, margins=True)
    # temp2 = temp.copy()
    # temp = (temp.Assigned_ID / temp.groupby(level=0).Assigned_ID.transform(sum) * 1).round(4)
    # #abc2 = ''
    # for months in range(12,1,-1):
    #     abc = pd.pivot_table(df[df['Reported Date'].dt.month == months],values = 'Assigned_ID',index = ['Age Group','Ever Hospitalized','Ever in ICU'],aggfunc=np.count_nonzero, margins=True)
    #     abc2 = abc.copy()
    #     abc = (abc.Assigned_ID / abc.groupby(level=0).Assigned_ID.transform(sum) * 1).round(4)
    #     abc.name = months
    #     abc2.columns = [months]
    #     #abc.set_index(['Age Group', 'Ever Hospitalized', 'Ever in ICU'],inplace = True)
    #     temp = pd.merge(temp,abc, how = 'left', on = ['Age Group','Ever Hospitalized','Ever in ICU'])
    #     temp2 = pd.merge(temp2,abc2, how = 'left', on = ['Age Group','Ever Hospitalized','Ever in ICU'])
    # temp.fillna(0,inplace = True)
    # temp2.fillna(0,inplace = True)
    # idx = pd.IndexSlice
    # Pivot_TorontoICUPctByReportMonth = temp.loc[idx[:,'Yes','Yes'],:]
    # Pivot_TorontoICUCountByReportMonth = temp2.loc[idx[:,'Yes','Yes'],:]

    temp = pd.pivot_table(df, values='Assigned_ID',
                          index=['Age Group', 'Ever Hospitalized', 'Ever in ICU'],
                          aggfunc=np.count_nonzero, margins=True)
    temp2 = temp.copy()
    temp = (temp.Assigned_ID / temp.groupby(level=0).Assigned_ID.transform(sum) * 1).round(4)

    for dates in list(dateRange):
        # abc = pd.pivot_table(df[df['Reported Date'].dt.month == dates.month ],values = 'Assigned_ID',index = ['Age Group','Ever Hospitalized','Ever in ICU'],aggfunc=np.count_nonzero, margins=True)
        abc = pd.pivot_table(df[(df['Reported Date'] >= datetime.datetime(dates.year, dates.month, 1))
                                & (df['Reported Date'] <= dates)],
                             values='Assigned_ID',
                             index=['Age Group', 'Ever Hospitalized', 'Ever in ICU'],
                             aggfunc=np.count_nonzero, margins=True)

        abc2 = abc.copy()
        abc = (abc.Assigned_ID / abc.groupby(level=0).Assigned_ID.transform(sum) * 1).round(4)
        abc.name = dates.month
        abc2.columns = [dates.month]
        # abc.set_index(['Age Group', 'Ever Hospitalized', 'Ever in ICU'],inplace = True)
        temp = pd.merge(temp, abc, how='left',
                        on=['Age Group', 'Ever Hospitalized', 'Ever in ICU'])
        temp2 = pd.merge(temp2, abc2, how='left',
                         on=['Age Group', 'Ever Hospitalized', 'Ever in ICU'])
    temp.fillna(0, inplace=True)
    temp2.fillna(0, inplace=True)
    idx = pd.IndexSlice
    Pivot_TorontoICUPctByReportMonth = temp.loc[idx[:, 'Yes', 'Yes'], :]
    Pivot_TorontoICUCountByReportMonth = temp2.loc[idx[:, 'Yes', 'Yes'], :]

    #######################################################
    # Hospitalized counts by reporting month and percentages
    # temp = pd.pivot_table(df,values = 'Assigned_ID',index = ['Age Group','Ever Hospitalized'],aggfunc=np.count_nonzero, margins=True)
    # temp2 = temp.copy()
    # temp = (temp.Assigned_ID / temp.groupby(level=0).Assigned_ID.transform(sum) * 1).round(4)
    # for months in range(12,1,-1):
    #     abc = pd.pivot_table(df[df['Reported Date'].dt.month == months],values = 'Assigned_ID',index = ['Age Group','Ever Hospitalized'],aggfunc=np.count_nonzero, margins=True)
    #     abc2 = abc.copy()
    #     abc = (abc.Assigned_ID / abc.groupby(level=0).Assigned_ID.transform(sum) * 1).round(4)
    #     abc.name = months
    #     abc2.columns = [months]
    #     #abc.set_index(['Age Group', 'Ever Hospitalized', 'Ever in ICU'],inplace = True)
    #     temp = pd.merge(temp,abc, how = 'left', on = ['Age Group','Ever Hospitalized'])
    #     temp2 = pd.merge(temp2,abc2, how = 'left', on = ['Age Group','Ever Hospitalized'])
    # temp.fillna(0,inplace = True)
    # temp2.fillna(0,inplace = True)
    # idx = pd.IndexSlice
    # Pivot_TorontoHospPctByReportMonth = temp.loc[idx[:,'Yes'],:]
    # Pivot_TorontoHospCountByReportMonth = temp2.loc[idx[:,'Yes'],:]

    temp = pd.pivot_table(df, values='Assigned_ID',
                          index=['Age Group', 'Ever Hospitalized'],
                          aggfunc=np.count_nonzero, margins=True)
    temp2 = temp.copy()
    temp = (temp.Assigned_ID / temp.groupby(level=0).Assigned_ID.transform(sum) * 1).round(4)
    for dates in list(dateRange):
        # abc = pd.pivot_table(df[df['Reported Date'].dt.month == dates.month],values = 'Assigned_ID',index = ['Age Group','Ever Hospitalized'],aggfunc=np.count_nonzero, margins=True)
        abc = pd.pivot_table(df[(df['Reported Date'] >= datetime.datetime(dates.year, dates.month, 1)) & (df['Reported Date'] <= dates)],
                             values='Assigned_ID',
                             index=['Age Group', 'Ever Hospitalized'],
                             aggfunc=np.count_nonzero, margins=True)

        abc2 = abc.copy()
        abc = (abc.Assigned_ID / abc.groupby(level=0).Assigned_ID.transform(sum) * 1).round(4)
        abc.name = dates.month
        abc2.columns = [dates.month]
        # abc.set_index(['Age Group', 'Ever Hospitalized', 'Ever in ICU'],inplace = True)
        temp = pd.merge(temp, abc, how='left',
                        on=['Age Group', 'Ever Hospitalized'])
        temp2 = pd.merge(temp2, abc2, how='left',
                         on=['Age Group', 'Ever Hospitalized'])
    temp.fillna(0, inplace=True)
    temp2.fillna(0, inplace=True)
    idx = pd.IndexSlice
    Pivot_TorontoHospPctByReportMonth = temp.loc[idx[:, 'Yes'], :]
    Pivot_TorontoHospCountByReportMonth = temp2.loc[idx[:, 'Yes'], :]

    #######################################################
    # Stats on Hosp (all) patients
    Pivot_Hosp = pd.pivot_table(df[df['Outcome'] != 'ACTIVE'], values='Assigned_ID',
                                index=['Age Group'],
                                columns=['Ever Hospitalized', 'Outcome'],
                                aggfunc=np.count_nonzero, margins=True).fillna(0)
    Pivot_Hosp_Fatal = Pivot_Hosp['Yes']['FATAL']
    Pivot_Hosp_NotActive = Pivot_Hosp['Yes']['RESOLVED'] + Pivot_Hosp['Yes']['FATAL']
    Pivot_Hosp_NotActive.name = 'Hosp Yes, Not Active (Dead or resolved)'
    Pivot_NotHosp = Pivot_Hosp['No'].sum(axis=1)
    Pivot_NotHosp.name = 'Not Hosp'
    Pivot_HospFatalityRate = (Pivot_Hosp_Fatal / Pivot_Hosp_NotActive).round(4)
    Pivot_HospFatalityRate.name = 'Hosp fatality %'
    Pivot_HospRate = (Pivot_Hosp['Yes'].sum(axis=1) / Pivot_Hosp.sum(axis=1)).round(4)
    Pivot_HospRate.name = 'Ever Hosp %'
    Pivot_HospDF = pd.DataFrame([Pivot_Hosp_Fatal, Pivot_Hosp_NotActive,
                                 Pivot_HospFatalityRate, Pivot_NotHosp,
                                 Pivot_HospRate]
                                ).T
    #######################################################
    # Stats on Ventilated patients
    Pivot_Vent = pd.pivot_table(df[df['Outcome'] != 'ACTIVE'],
                                values='Assigned_ID', index=['Age Group'],
                                columns=['Ever Intubated', 'Outcome'],
                                aggfunc=np.count_nonzero, margins=True).fillna(0)
    Pivot_Vent_Fatal = Pivot_Vent['Yes']['FATAL']
    Pivot_Vent_NotActive = Pivot_Vent['Yes']['RESOLVED'] + Pivot_Vent['Yes']['FATAL']
    Pivot_Vent_NotActive.name = 'Vent Yes, Not Active'
    Pivot_NotVent = Pivot_Vent['No'].sum(axis=1)
    Pivot_NotVent.name = 'Not vent'
    Pivot_VentFatalityRate = (Pivot_Vent_Fatal / Pivot_Vent_NotActive).round(4)
    Pivot_VentFatalityRate.name = 'Vent fatality %'
    Pivot_VentRate = (Pivot_Vent['Yes'].sum(axis=1) / Pivot_Vent.sum(axis=1)).round(4)
    Pivot_VentRate.name = 'Ever Vent %'
    Pivot_VentDF = pd.DataFrame([Pivot_Vent_Fatal, Pivot_Vent_NotActive,
                                 Pivot_VentFatalityRate, Pivot_NotVent,
                                 Pivot_VentRate]).T
    #######################################################
    # Stats on ICU (all) patients
    Pivot_ICU = pd.pivot_table(df[df['Outcome'] != 'ACTIVE'],
                               values='Assigned_ID', index=['Age Group'],
                               columns=['Ever in ICU', 'Outcome'],
                               aggfunc=np.count_nonzero, margins=True).fillna(0)
    Pivot_ICU_Fatal = Pivot_ICU['Yes']['FATAL']
    Pivot_ICU_NotActive = Pivot_ICU['Yes']['RESOLVED'] + Pivot_ICU['Yes']['FATAL']
    Pivot_ICU_NotActive.name = 'ICU Yes, Not Active (Dead or resolved)'
    Pivot_NotICU = Pivot_ICU['No'].sum(axis=1)
    Pivot_NotICU.name = 'Not ICU'
    Pivot_ICUFatalityRate = (Pivot_ICU_Fatal / Pivot_ICU_NotActive).round(4)
    Pivot_ICUFatalityRate.name = 'ICU fatality %'
    Pivot_ICURate = (Pivot_ICU['Yes'].sum(axis=1) / Pivot_ICU.sum(axis=1)).round(4)
    Pivot_ICURate.name = 'Ever ICU %'
    Pivot_ICUDF = pd.DataFrame([Pivot_ICU_Fatal, Pivot_ICU_NotActive, Pivot_ICUFatalityRate,
                                Pivot_NotICU, Pivot_ICURate]).T
    #######################################################
    # Stats on ICU only patients
    # Pivot_ICUNotVent = pd.pivot_table(df[df['Ever Intubated'] == 'No'], values='Assigned_ID',
    #                                   index=['Age Group'], columns=['Ever in ICU', 'Outcome'],
    #                                   aggfunc=np.count_nonzero, margins=True).fillna(0)
    # Pivot_ICUNotVent_Fatal = Pivot_ICU['Yes']['FATAL']
    # Pivot_ICUNotVent_NotActive = Pivot_ICU['Yes']['RESOLVED'] + Pivot_ICU['Yes']['FATAL']
    # Pivot_ICUNotVent_NotActive.name = 'ICU Yes, Not Active'
    # Pivot_NotICU = Pivot_ICU['No'].sum(axis=1)
    # Pivot_NotICU.name = 'Not ICU'
    # Pivot_ICUNotVentFatalityRate = (Pivot_ICU_Fatal / Pivot_ICU_NotActive).round(4)
    # Pivot_ICUNotVentFatalityRate.name = 'ICU fatality %'
    # Pivot_ICUNotVentRate = (Pivot_ICU['Yes'].sum(axis=1) / Pivot_ICU.sum(axis=1)).round(4)
    # Pivot_ICUNotVentRate.name = 'Ever ICU %'
    # Pivot_ICUNotVentDF = pd.DataFrame([Pivot_ICUNotVent_Fatal,Pivot_ICUNotVent_NotActive, Pivot_ICUNotVentFatalityRate, Pivot_NotICU,Pivot_ICUNotVentRate]).T
    #######################################################

    with open(filename, 'a', newline='') as f:
        f.write('Toronto Hosp% by month \n')
        Pivot_TorontoHospPctByReportMonth.to_csv(f, header=True)
        f.write('\n')
        f.write('Toronto Hosp count by month \n')
        Pivot_TorontoHospCountByReportMonth.to_csv(f, header=True)
        f.write('\n')
        f.write('Toronto ICU% by month \n')
        Pivot_TorontoICUPctByReportMonth.to_csv(f, header=True)
        f.write('\n')
        f.write('Toronto ICU count by month \n')
        Pivot_TorontoICUCountByReportMonth.to_csv(f, header=True)
        f.write('\n')
        f.write('Toronto Hospitalization stats \n')
        Pivot_HospDF.to_csv(f, header=True)
        f.write('\n')
        f.write('Toronto ICU stats \n')
        Pivot_ICUDF.to_csv(f, header=True)
        f.write('\n')
        f.write('Toronto ventilated stats \n')
        Pivot_VentDF.to_csv(f, header=True)

    print(f'Ended:   {datetime.datetime.now():%Y-%m-%d %H:%M:%S} {round(time.time() - starttime, 2)} seconds')
    print('------------------------------------------------------------------------')


def OntarioCaseStatus():
    starttime = datetime.datetime.now()
    print('------------------------------------------------------------------------')
    print(f'OntarioCaseStatus \nStarted: {starttime:%Y-%m-%d %H:%M:%S}')

    pd.options.display.float_format = '{:.2f}'.format
    ConsoleOut = sys.stdout
    TodaysDate = datetime.date.today()
    YesterdayDate = TodaysDate - datetime.timedelta(days=1)
    LastYearDate = datetime.date(TodaysDate.year - 1, TodaysDate.month, TodaysDate.day)
    # DailyReports_PHUChange()
    OntarioCaseStatusFileName = 'TextOutput/OntarioCaseStatusText.txt'
    SchoolsFileName = 'TextOutput/SchoolsText.txt'
    RTReportFileName = 'Pickle/RTData.pickle'
    hospitalizations_with_for_covid()
    deaths_with_for_covid()
    # school_closures()
    school_absenteeism()

    # HospitalMetrics()
    # VaccineData()
    print()
    pickleFileName = ('Pickle/OntarioCaseStatus.pickle')

    HospitalDF = pd.read_pickle('Pickle/HospitalData.pickle')
    # PositiveCaseDF = pd.read_pickle('Pickle/ChangeInCasesByOutcome.pickle')
    RTReportDF = pd.read_pickle(RTReportFileName)
    CumulativeHospitalizations_filePath = 'Pickle/CumulativeHospitalizations.csv'
    VariantCounts_filePath = 'Pickle/VariantCounts.csv'

    df = pd.read_csv('https://data.ontario.ca/dataset/f4f86e54-872d-43f8-8a86-3892fd3cb5e6/resource/ed270bb8-340b-41f9-a7c6-e8ef587e6d11/download/covidtesting.csv',
                     parse_dates=[0], infer_datetime_format=False, index_col=False)
    df.to_csv('SourceFiles/OntarioCaseStatus.csv')
    # df = pd.read_csv('covidtesting.csv', parse_dates=[0], infer_datetime_format=False,
    #                   index_col=False)

    df = df.drop_duplicates(ignore_index=True)
    df['Deaths'] = df['Deaths'].fillna(df['Deaths_New_Methodology'])
    for column in ['Total Cases', 'Deaths', 'Resolved']:
        df[column] = df[column].fillna(0)

    df['Number of patients hospitalized with COVID-19'] = df['Number of patients hospitalized with COVID-19'].fillna(method="ffill")
    # df['Reported Date'] = pd.to_datetime(df['Reported Date'],format='%Y-%m-%d')
    # df['Reported Date'] = pd.to_datetime(df['Reported Date'],format='%m/%d/%Y')
    df['Day new cases'] = (df['Total Cases'] - df['Total Cases'].shift(1))
    df['Day new deaths'] = df['Deaths'] - df['Deaths'].shift(1)
    df['Day new resolved'] = df['Resolved'] - df['Resolved'].shift(1)
    df['Cases vs. last week'] = (df['Day new cases'] - df['Day new cases'].shift(7)).fillna(0)
    df['Cases vs. last week - 7 day SMA'] = df['Cases vs. last week'].rolling(7).mean()
    df['doy'] = (df['Reported Date'].dt.dayofyear).astype(int)
    df['Year'] = df['Reported Date'].dt.year
    df = df.sort_values(by='Reported Date', ascending=True)

    CumulativeHospitalizationsDF = pd.read_csv(CumulativeHospitalizations_filePath)
    CumulativeHospitalizationsDF = CumulativeHospitalizationsDF.fillna(method='bfill')
    CumulativeHospitalizationsDF['Date'] = pd.to_datetime(CumulativeHospitalizationsDF['Date'])

    VariantCountDF = pd.read_csv(VariantCounts_filePath)
    VariantCountDF['Date'] = pd.to_datetime(VariantCountDF['Date'])

    df = df.merge(CumulativeHospitalizationsDF, left_on='Reported Date', right_on='Date', how='outer')
    df = df.merge(RTReportDF, left_on='Reported Date', right_index=True, how='outer')
    df = df.merge(VariantCountDF, left_on='Reported Date', right_on='Date', how='outer')

    df = df.set_index('Reported Date')
    df['7 day SMA'] = df['Day new cases'].rolling(7).mean().round(2)
    df['Deaths - 7 day SMA'] = df['Day new deaths'].rolling(7).mean().round(0)
    df['Rt estimate-5 day'] = df['7 day SMA'] / df['7 day SMA'].shift(5)
    df['Rt estimate'] = df['7 day SMA'] / df['7 day SMA'].shift(4)
    df['Re'] = df['Re'].fillna(df['Rt estimate'])
    df['Rt estimate'] = df['Re']
    df.pop('Re')

    df['Number of patients in ICU due to COVID-19'] = \
        df['Number of patients in ICU due to COVID-19'].fillna(df['Number of patients in ICU, testing positive for COVID-19'])
    df['Change in ICUs'] = (df['Number of patients in ICU due to COVID-19']
                            - df['Number of patients in ICU due to COVID-19'].shift(1))
    df['Number of patients in ICU on a ventilator due to COVID-19'] = \
        df['Number of patients in ICU on a ventilator due to COVID-19'].fillna(df['Num. of patients in ICU on a ventilator testing positive'])

    for column in ['Number of patients in ICU due to COVID-19',
                   'Number of patients in ICU on a ventilator due to COVID-19']:
        df[column] = df[column].fillna(method='ffill')
    df = df.fillna(0)

    df['Total LTC Deaths'] = (df['Total LTC Resident Deaths'] + df['Total LTC HCW Deaths'])
    df['New LTC Deaths'] = (df['Total LTC Deaths']
                            - df['Total LTC Deaths'].shift(1)).fillna(0).astype(int)

    df = df.sort_index(ascending=False)
    df['Day new cases vs. Trailing 7SMA'] = (df['Day new cases'] / df.shift(1)['7 day SMA'])
    # for dayweek in range(7):
    #     print(calendar.day_name[dayweek])
    #     print (df[df.index.dayofweek ==dayweek]['Day new cases vs. Trailing 7SMA'].quantile([.25, .5,.75]))
    # for dayweek in range(7):
    #     print(calendar.day_name[dayweek])
    #     print (df[df.index.dayofweek ==dayweek]['Day new cases'].median()/df['Day new cases'].median())
    df['doy'] = df['doy'].astype(int)
    df.to_pickle(pickleFileName)

    NewCases = int(df['Day new cases'][0])
    # NewCases = int(PositiveCaseDF.iloc[:,0].sum())

    NewCasesWeek = int(df['Day new cases'][0:7].sum())
    NewCasesPrevWeek = int(df['Day new cases'][7:14].sum())
    NewVariants_UK = int(df['Alpha'][0])
    NewVariants_RSA = int(df['Beta'][0])
    NewVariants_BRA = int(df['Gamma'][0])
    NewVariants_Delta = int(df['Delta'][0])
    NewVariants_Omicron = int(df['Omicron'][0])

    Backlog = int(df['Under Investigation'][0])
    ChangeInBacklog = int(df['Under Investigation'][0] - df['Under Investigation'][1])

    TestsCompleted = int(df['Total tests completed in the last day'][0])
    # if TodaysDate != df.iloc[0].name:
    if pd.Timestamp(TodaysDate) != df.iloc[0].name:
        TestsCompleted = 0

    TotalTestsCompleted = int(df['Total patients approved for testing as of Reporting Date'][0])
    TotalTestsCompletedWeek = int(df['Total tests completed in the last day'][0:7].sum())
    TotalTestsCompletedPrevWeek = int(df['Total tests completed in the last day'][7:14].sum())
    TestsCompletedPer100k = TestsCompleted / (PHUPopulation().sum() / 2) * 100000
    TestsCompletedWeekPer100k = TotalTestsCompletedWeek / (PHUPopulation().sum() / 2) * 100000

    TotalSwabbed = TestsCompleted + ChangeInBacklog
    # TodaysDate = df.iloc[0].name

    CurrentHospitalizations = int(df['Number of patients hospitalized with COVID-19'][0])
    ChangeInHospitalizations = int(df['Number of patients hospitalized with COVID-19'][0]) - int(df['Number of patients hospitalized with COVID-19'][1])
    ChangeInHospitalizationsLast7 = int(df['Number of patients hospitalized with COVID-19'][0]) - int(df['Number of patients hospitalized with COVID-19'][7])

    # CurrentICUs = int(df['Number of patients in ICU with COVID-19'][0])
    # CurrentICUs = int(df['Number of patients in ICU due to COVID-19'][0])
    # CurrentICUs = HospitalDF['icu_crci_total'].sum()
    CurrentICUs = HospitalDF[HospitalDF['date'] == HospitalDF['date'].max()]['icu_crci_total'].sum()
    # WeekAgoICU = int(df['Number of patients in ICU due to COVID-19'][7])
    WeekAgoICU = HospitalDF[HospitalDF['date']
                            == (HospitalDF['date'].max() - pd.Timedelta(days=7))]['icu_crci_total'].sum()

    YesterdayICU = HospitalDF[HospitalDF['date']
                              == (HospitalDF['date'].max() - pd.Timedelta(days=1))]['icu_crci_total'].sum()
    # ChangeInICUs = CurrentICUs - int(df['Number of patients in ICU with COVID-19'][1])
    # ChangeInICUs = CurrentICUs - int(df['Number of patients in ICU due to COVID-19'][1])
    ChangeInICUs = CurrentICUs - YesterdayICU

    # ChangeInICUsLast7 = CurrentICUs - int(df['Number of patients in ICU with COVID-19'][7])
    ChangeInICUsLast7 = CurrentICUs - WeekAgoICU

    # CurrentVent = int(df['Number of patients in ICU on a ventilator with COVID-19'][0])
    # ChangeInVent = CurrentVent - int(df['Number of patients in ICU on a ventilator with COVID-19'][1])
    # ChangeInVentLast7 = CurrentVent - int(df['Number of patients in ICU on a ventilator with COVID-19'][1])

    CurrentVent = int(df['Number of patients in ICU on a ventilator due to COVID-19'][0])
    ChangeInVent = CurrentVent - int(df['Number of patients in ICU on a ventilator due to COVID-19'][1])
    ChangeInVentLast7 = CurrentVent - int(df['Number of patients in ICU on a ventilator due to COVID-19'][7])

    TotalLTCCases = (df['Total Positive LTC Resident Cases'][0]
                     + df['Total Positive LTC HCW Cases'][0])
    NewLTCResidentDeaths = (df['Total LTC Resident Deaths'][0]
                            - df['Total LTC Resident Deaths'][1])

    if TestsCompleted > 0:
        PositiveRateDay = (NewCases / TestsCompleted)
        PositiveRateWeek = NewCasesWeek / (df['Total tests completed in the last day'][0:7].sum())
        PositiveRatePrevWeek = NewCasesPrevWeek / TotalTestsCompletedPrevWeek
    else:
        PositiveRateDay = 9000
        PositiveRateWeek = 9000
        PositiveRatePrevWeek = 9000

    print('Case status done')

    # dfschools = pd.read_csv('https://data.ontario.ca/dataset/b1fef838-8784-4338-8ef9-ae7cfd405b41/resource/7fbdbb48-d074-45d9-93cb-f7de58950418/download/schoolcovidsummary.csv')
    dfschools = pd.read_csv('https://data.ontario.ca/dataset/b1fef838-8784-4338-8ef9-ae7cfd405b41/resource/7e644a48-6040-4ee0-9216-1f88121b21ba/download/schoolcovidsummary2021_2022.csv')

    # dfschools['reported_date'] = pd.to_datetime(dfschools['reported_date'],format='%Y-%m-%d')
    # dfschools['reported_date'] = pd.to_datetime(dfschools['reported_date'],format='%m/%d/%Y')
    dfschools['reported_date'] = pd.to_datetime(dfschools['reported_date'])

    NewSchoolCases = int(dfschools.loc[:, 'new_total_school_related_cases'][dfschools.shape[0] - 1])
    SchoolsWithCases = int(dfschools.loc[:, 'current_schools_w_cases'][dfschools.shape[0] - 1])
    PctSchoolWithCases = (SchoolsWithCases
                          / int(dfschools.loc[:, 'current_total_number_schools'][dfschools.shape[0] - 1]))
    NewStudentCases = int(dfschools.loc[:, 'new_school_related_student_cases'][dfschools.shape[0] - 1])
    NewStaffCases = NewSchoolCases - NewStudentCases
    SchoolsClosed = int(dfschools.loc[:, 'current_schools_closed'][dfschools.shape[0] - 1])
    # NewSchoolsClosed = SchoolsClosed
    NewSchoolsClosed = SchoolsClosed - int(dfschools.loc[:, 'current_schools_closed'][dfschools.shape[0] - 2])
    print('Schools file #1 done')

    dfSchoolsWithActive = pd.read_csv('https://data.ontario.ca/dataset/b1fef838-8784-4338-8ef9-ae7cfd405b41/resource/dc5c8788-792f-4f91-a400-036cdf28cfe8/download/schoolrecentcovid2021_2022.csv')
    dfSchoolsWithActive.to_csv('SourceFiles/schoolrecentcovid2021_2022.csv')
    dfSchoolsWithActive['reported_date'] = pd.to_datetime(dfSchoolsWithActive['reported_date'],
                                                          format='%Y-%m-%d')
    dfSchoolsWithActive = dfSchoolsWithActive[dfSchoolsWithActive['reported_date']
                                              == dfSchoolsWithActive['reported_date'].max()]
    dfSchoolsWithActive = dfSchoolsWithActive.sort_values(by='total_confirmed_cases', ascending=False)

    dfSchoolsWithActivePivot = pd.pivot_table(dfSchoolsWithActive,
                                              index='municipality',
                                              aggfunc=[np.count_nonzero, sum],
                                              values='total_confirmed_cases')

    dfSchoolsWithActivePivot = dfSchoolsWithActivePivot.sort_values(by=dfSchoolsWithActivePivot.columns[0],
                                                                    ascending=False)

    print('Schools file #2 done')

    dfVaccine = pd.read_pickle('Pickle/VaccinesData.pickle')
    dfCaseByVaxStatus = pd.read_pickle('Pickle/CasesByVaxStatus.pickle')
    # dfVaxAge = pd.read_pickle('Pickle/VaccineAgeAll.pickle')
    # dfCaseByVaxStatus['Unvax_Per100k_Day'] = dfCaseByVaxStatus['Unvax_Per100k_Day'].map('{:,.2f}'.format)
    # dfCaseByVaxStatus['Partial_Per100k_Day'] = dfCaseByVaxStatus['Partial_Per100k_Day'].map('{:,.2f}'.format)
    # dfCaseByVaxStatus['Fully_Per100k_Day'] = dfCaseByVaxStatus['Fully_Per100k_Day'].map('{:,.2f}'.format)
    # dfCaseByVaxStatus['All_Per100k_Day'] = dfCaseByVaxStatus['All_Per100k_Day'].map('{:,.2f}'.format)

    TodaysDFVaxAge = pd.read_pickle('Pickle/VaccineAgeData.pickle')
    # Total_AtLeastOne_18Plus = (TodaysDFVaxAge['At least one dose_cumulative'].sum()
    #                            - TodaysDFVaxAge.loc['12-17yrs']['At least one dose_cumulative']
    #                            - TodaysDFVaxAge.loc['Unknown']['At least one dose_cumulative'])
    Total_AtLeastOne_12Plus = TodaysDFVaxAge.loc['Total - eligible 12+']['At least one dose_cumulative']
    Total_BothDose_12Plus = TodaysDFVaxAge.loc['Total - eligible 12+']['Second_dose_cumulative']
    # Total_BothDose_18Plus = (TodaysDFVaxAge['Second_dose_cumulative'].sum()
    #                          - TodaysDFVaxAge.loc['12-17yrs']['Second_dose_cumulative']
    #                          - TodaysDFVaxAge.loc['Unknown']['Second_dose_cumulative'])
    OntarioAdultPopulation = (TodaysDFVaxAge['Total population'].sum()
                              - TodaysDFVaxAge.loc['12-17yrs']['Total population'])
    OntarioEligiblePopulation = TodaysDFVaxAge.loc['Total - eligible 12+']['Total population'].sum()
    # PercentVax_18Plus_AtLeastOne = Total_AtLeastOne_18Plus/OntarioAdultPopulation
    PercentVax_18Plus_AtLeastOne = TodaysDFVaxAge.loc['Adults_18plus']['Percent_at_least_one_dose']
    PercentVax_18Plus_BothDose = TodaysDFVaxAge.loc['Adults_18plus']['Percent_fully_vaccinated']
    Percent_Eligible_AtLeastOne = Total_AtLeastOne_12Plus / OntarioEligiblePopulation
    Percent_Eligible_Both = Total_BothDose_12Plus / OntarioEligiblePopulation
    # PercentVax_18Plus_BothDose = Total_BothDose_18Plus/OntarioAdultPopulation

    # dfVaccine = pd.read_json('https://api.covid19tracker.ca/reports/province/on',orient='records',dtype = {'province' : str})
    # dfVaccine = pd.json_normalize(dfVaccine['data'])
    # dfVaccine = dfVaccine.fillna(0)

    # dfVaccine['date'] = pd.to_datetime(dfVaccine['date'])
    # dfVaccine = dfVaccine.set_index('date')
    # dfVaccine = dfVaccine.sort_index(ascending = False)
    # dfVaccine['total_vaccinations'] = dfVaccine['total_vaccinations'].astype(int)
    # dfVaccine['change_vaccinations'] = dfVaccine['change_vaccinations'].astype(int)

    OntarioThrowbackFileName = 'TextOutput/OntariThrowbackText.txt'
    sys.stdout = open(OntarioThrowbackFileName, 'w')

    for years in range(2, 0, -1):
        LastYearDate = datetime.date(TodaysDate.year - years, TodaysDate.month, TodaysDate.day)

        NewCases_YearAgo = int(df.loc[LastYearDate.strftime("%Y-%m-%d")]['Day new cases'])
        # NewCasesWeek = int(df['Day new cases'][0:7].sum())
        # NewCasesPrevWeek = int(df['Day new cases'][7:14].sum())
        NewRecoveries_YearAgo = (int(df.loc[LastYearDate.strftime("%Y-%m-%d")]['Resolved'])
                                 - int(df.loc[(LastYearDate - datetime.timedelta(days=1)).strftime("%Y-%m-%d")]['Resolved']))
        NewDeaths_YearAgo = (int(df.loc[LastYearDate.strftime("%Y-%m-%d")]['Deaths'])
                             - int(df.loc[(LastYearDate - datetime.timedelta(days=1)).strftime("%Y-%m-%d")]['Deaths']))

        CurrentICU_YearAgo = int(df.loc[LastYearDate.strftime("%Y-%m-%d")]['Number of patients in ICU due to COVID-19'])
        ChangeInICUs_YearAgo = (CurrentICU_YearAgo
                                - int(df.loc[(LastYearDate - datetime.timedelta(days=1)).strftime("%Y-%m-%d")]['Number of patients in ICU due to COVID-19']))
        ChangeInICUsLast7_YearAgo = (CurrentICU_YearAgo
                                     - int(df.loc[(LastYearDate - datetime.timedelta(days=7)).strftime("%Y-%m-%d")]['Number of patients in ICU due to COVID-19']))

        TestsCompleted_YearAgo = (int(df.loc[LastYearDate.strftime("%Y-%m-%d")]['Total patients approved for testing as of Reporting Date'])
                                  - int(df.loc[(LastYearDate - datetime.timedelta(days=1)).strftime("%Y-%m-%d")]['Total patients approved for testing as of Reporting Date']))
        PositiveRateDay_YearAgo = NewCases_YearAgo / TestsCompleted_YearAgo

        print('* **Throwback** Ontario ' + LastYearDate.strftime('%B %#d, %Y') + ' update: '
              + str(NewCases_YearAgo) + ' New Cases, ' + str(NewRecoveries_YearAgo) + ' Recoveries, '
              + str(NewDeaths_YearAgo) + ' Deaths, ' + format(TestsCompleted_YearAgo, ",d"), ' tests ('
              + format(PositiveRateDay_YearAgo, ".2%") + ' positive), Current ICUs: '
              + format(CurrentICU_YearAgo, ",d") + ' (' + format(ChangeInICUs_YearAgo, "+,d") + ' vs. yesterday)  ('
              + format(ChangeInICUsLast7_YearAgo, "+,d") + ' vs. last week)'
              )

    sys.stdout = ConsoleOut

    # print('Ontario '+TodaysDate.strftime('%B %d').lstrip("0").replace(" 0", " ")+ ' update: '+
    #       (str(NewCases)+' New Cases, '+str(NewRecoveries)+' Recoveries, '+str(NewDeaths)+'('+str(NewLTCResidentDeaths)+' LTC) Deaths, X New Hospitalizations, X New ICU')
    #       )
    print()

    # print('Ontario '+custom_strftime('%B {S}',TodaysDate)+ ' update: '+
    #       str(NewCases)+' New Cases, '+str(NewRecoveries)+' Recoveries, '+str(NewDeaths)+' Deaths, '
    #       + format(TestsCompleted,",d"),' tests ('+format(PositiveRateDay,".2%")+' positive), Current ICUs: '
    #       + format(CurrentICUs,",d")+' ('+format(ChangeInICUs,"+,d")+' vs. yesterday)  ('+format(ChangeInICUsLast7,"+,d")+' vs. last week)'
    #       +'. Ã°Å¸â€™â€°Ã°Å¸â€™â€°'+"{:,}".format(dfVaccine['previous_day_total_doses_administered'][0])+' administered, '"{:.2%}".format(Percent_Eligible_AtLeastOne),'/',"{:.2%}".format(Percent_Eligible_Both),'('+"{:+.2%}".format(TodaysDFVaxAge.loc['Total - eligible 12+']['FirstDose - in last day %']),'/',"{:+.2%}".format(TodaysDFVaxAge.loc['Total - eligible 12+']['SecondDose - in last day %'])+') of 12+ at least one/two dosed')

    # print('Ontario '+custom_strftime('%B {S}',TodaysDate)+ ' update: '+
    #       str(NewCases)+' Cases, '+str(NewDeaths)+' Deaths, '
    #       + format(TestsCompleted,",d"),' tests ('+format(PositiveRateDay,".2%")+' pos.), Ã°Å¸ÂÂ¥ ICUs: '
    #       + format(CurrentICUs,",d")+' ('+format(ChangeInICUs,"+,d")+' vs. yest.)  ('+format(ChangeInICUsLast7,"+,d")+' vs. last week)'
    #       +'. Ã°Å¸â€™â€°'+"{:,}".format(dfVaccine['previous_day_total_doses_administered'][0])+' admin, '"{:.2%}".format(Percent_Eligible_AtLeastOne),'/',"{:.2%}".format(Percent_Eligible_Both),'('+"{:+.2%}".format(TodaysDFVaxAge.loc['Total - eligible 12+']['FirstDose - in last day %']),'/',"{:+.2%}".format(TodaysDFVaxAge.loc['Total - eligible 12+']['SecondDose - in last day %'])+') of 12+ at least one/two dosed'
    #       +', Ã°Å¸â€ºÂ¡Ã¯Â¸Â 12+ Cases by Vax (un/part/full): '+"{:.2f}".format(dfCaseByVaxStatus['Unvax_Per100k_Day_12Plus'][0])+' / '+dfCaseByVaxStatus['Partial_Per100k_Day'][0]+' / '+dfCaseByVaxStatus['Fully_Per100k_Day'][0]+' (All: '+dfCaseByVaxStatus['All_Per100k_Day'][0]+') per 100k')

    # print('Ontario '+custom_strftime('%B {S}',TodaysDate)+ ' update: '+
    #       str(NewCases)+' Cases, '+str(NewDeaths)+' Deaths, '
    #       + format(TestsCompleted,",d"),' tests ('+format(PositiveRateDay,".2%")+' pos.), ICUs: '
    #       + format(CurrentICUs,",d")+' ('+format(ChangeInICUs,"+,d")+' vs. yest.)  ('+format(ChangeInICUsLast7,"+,d")+' vs. last week)'
    #       +'. Vax: '+"{:,}".format(dfVaccine['previous_day_total_doses_administered'][0])+' admin, '"{:.2%}".format(Percent_Eligible_AtLeastOne),'/',"{:.2%}".format(Percent_Eligible_Both),'('+"{:+.2%}".format(TodaysDFVaxAge.loc['Total - eligible 12+']['FirstDose - in last day %']),'/',"{:+.2%}".format(TodaysDFVaxAge.loc['Total - eligible 12+']['SecondDose - in last day %'])+') of 12+ at least one/two dosed'
    #       +', 12+ Cases by Vax (un/part/full): '+"{:.2f}".format(dfCaseByVaxStatus['Unvax_Per100k_Day_12Plus'][0])+' / '+dfCaseByVaxStatus['Partial_Per100k_Day'][0]+' / '+dfCaseByVaxStatus['Fully_Per100k_Day'][0]+' (All: '+dfCaseByVaxStatus['All_Per100k_Day'][0]+') per 100k')

    PostTitleFileName = 'TextOutput/PostTitle.txt'
    ###############################################################################################
    # If case status data is not posted today, update the headline to reflect this
    if dfCaseByVaxStatus.index.max() == EVHelper.todays_date():
        cases_by_vax = (f"🛡 5+ Cases by Vax (<2/2/3): {dfCaseByVaxStatus['NotFullVax_Per100k_Day_5Plus'][0]:.1f} / "
                        + f"{dfCaseByVaxStatus['Fully_Per100k_Day'][0]:.1f} / {dfCaseByVaxStatus['Boosted_Per100k_Day'][0]:.1f} "
                        + f"(All: {dfCaseByVaxStatus['All_Per100k_Day'][0]:.1f}) per 100k")
    else:
        cases_by_vax = "🛡 5+ Cases by Vax (<2/2/3): ???"
    ###############################################################################################

    sys.stdout = open(PostTitleFileName, 'w', encoding='utf-8')

    # f"Ontario {custom_strftime('%b {S}',TodaysDate)}
    PostTitle = f"Ontario {TodaysDate:%b %d}: {NewCases:,.0f} Cases, " \
        + f"{df['newly_reported_deaths'][0]:.0f} new " \
        + f"{df['deaths_data_cleaning'][0]:.0f} old Deaths, " \
        + f"{TestsCompleted:,.0f} tests ({PositiveRateDay:.1%} to {df['Percent positive tests in last day'][0]:.1f}% pos.) " \
        + f"🏥 ICUs: {CurrentICUs:,.0f} ({ChangeInICUs:+.0f} vs. yest.) ({ChangeInICUsLast7:+.0f} vs. last wk) " \
        + f"💉 {dfVaccine['previous_day_total_doses_administered'][0]:,.0f} admin, " \
        + f"{TodaysDFVaxAge.loc['Ontario_5plus']['Percent_at_least_one_dose']:.2%} / {TodaysDFVaxAge.loc['Ontario_5plus']['Percent_fully_vaccinated']:.2%} / "\
        + f"{TodaysDFVaxAge.loc['Ontario_5plus']['Percent_3doses']:.2%} " \
        + f"({TodaysDFVaxAge.loc['Ontario_5plus']['FirstDose - in last day %']:+.2%},"\
        + f" / {TodaysDFVaxAge.loc['Ontario_5plus']['SecondDose - in last day %']:+.2%} / "\
        + f" {dfVaccine['ThreeDosedPopulation_5Plus_Day'][0]:.2%}) of 5+ at least 1/2/3 dosed"\
        # + cases_by_vax

    PostTitle_2 = f"Ontario {TodaysDate:%b %d}: {NewCases:,.0f} Cases, " \
        + f"{df['Day new deaths'][0]:.0f} Deaths, " \
        + f"{TestsCompleted:,.0f} tests ({PositiveRateDay:.1%} to {df['Percent positive tests in last day'][0]:.1f}% pos.) " \
        + f"🏥 ICUs: {CurrentICUs:,.0f} ({ChangeInICUs:+.0f} vs. yest.) ({ChangeInICUsLast7:+.0f} vs. last wk) " \
        + f"💉 {dfVaccine['previous_day_total_doses_administered'][0]:,.0f} admin, " \
        + f"{TodaysDFVaxAge.loc['Ontario_5plus']['Percent_at_least_one_dose']:.2%} / {TodaysDFVaxAge.loc['Ontario_5plus']['Percent_fully_vaccinated']:.2%} / "\
        + f"{TodaysDFVaxAge.loc['Ontario_5plus']['Percent_3doses']:.2%} " \
        + f"({TodaysDFVaxAge.loc['Ontario_5plus']['FirstDose - in last day %']:+.2%},"\
        + f" / {TodaysDFVaxAge.loc['Ontario_5plus']['SecondDose - in last day %']:+.2%} / "\
        + f" {dfVaccine['ThreeDosedPopulation_5Plus_Day'][0]:.2%}) of 5+ at least 1/2/3 dosed"\
        # + cases_by_vax


    # if (df['deaths_data_cleaning'][0] != 0):
    #     PostTitle = PostTitle
    # else:
    #     PostTitle = PostTitle_2
    print(PostTitle_2)

    sys.stdout = ConsoleOut
    print(PostTitle_2)

    # print(f"Ontario {YesterdayDate:%b %d}+{TodaysDate:%d} update: {df['Day new cases'][1]:,.0f}+{df['Day new cases'][0]:,.0f} Cases, {df['Day new deaths'][1]:,.0f}+{df['Day new deaths'][0]:,.0f} Deaths, {df['Total tests completed in the last day'][0:2].sum():,.0f} tests, ({df['Day new cases'][0:2].sum()/df['Total tests completed in the last day'][0:2].sum():,.2%} pos.), Ã°Å¸ÂÂ¥ICUs: {df['Number of patients in ICU due to COVID-19'][0]:.0f}({df['Change in ICUs'][0:2].sum():+.0f} vs. {TodaysDate-datetime.timedelta(days=2):%a}) "
    #       + f"({df['Change in ICUs'][0:7].sum():+.0f} vs. last week), Ã°Å¸â€™â€°{dfVaccine['previous_day_total_doses_administered'][1]:,d}+{dfVaccine['previous_day_total_doses_administered'][0]:,d} admin, {Percent_Eligible_AtLeastOne:.2%} / {Percent_Eligible_Both:.2%} ({dfVaxAge.loc['Ontario_12plus']['FirstDose - in last day %'][0:2].sum():.2%} / {dfVaxAge.loc['Ontario_12plus']['SecondDose - in last day %'][0:2].sum():.2%}) of 12+ at least one/two dosed"
    #       + ', Ã°Å¸â€ºÂ¡Ã¯Â¸Â 12+ Cases by Vax (un/part/full): '
    #       + "{:.2f}".format(dfCaseByVaxStatus['Unvax_Per100k_Day_12Plus'][0])
    #       + ' / ' + dfCaseByVaxStatus['Partial_Per100k_Day'][0] + ' / ' + dfCaseByVaxStatus['Fully_Per100k_Day'][0]
    #       + ' (All: ' + dfCaseByVaxStatus['All_Per100k_Day'][0] + ') per 100k')

    sys.stdout = open(OntarioCaseStatusFileName, 'w')

    print('**Testing data:** - [Source](https://data.ontario.ca/dataset/status-of-covid-19-cases-in-ontario)')
    print()
    print('* Backlog: ' + format(Backlog, ",d") + ' (' + format(ChangeInBacklog, "+,d") + ')'
          + ', ' + format(TestsCompleted, ",d") + ' tests completed ('
          + "{:,.1f}".format(float(TestsCompletedWeekPer100k))
          + ' per 100k in week) --> ' + format(TotalSwabbed, ",d") + ' swabbed')
    print(f"* MoH positive rate: {df['Percent positive tests in last day'][0]:.1f}% - differs from the cases/tests calc.")
    print('* Positive rate (Day/Week/Prev Week): ' + format(PositiveRateDay, ".2%")
          + ' / ' + " {:,.2%}".format(PositiveRateWeek) + ' / '
          + " {:,.2%}".format(PositiveRatePrevWeek)
          + ' - [Chart](https://docs.google.com/spreadsheets/d/e/2PACX-1vQ7fegCALd11ElozUYcMi-e9Dj69YaiNQhvEpk81JHsyTACl0UXkWK5zfMNFe49Tq3VuN9Av-fuEZqV/pubchart?oid=1660579214&format=interactive)')
    print()
    changeInReportingLag = pd.read_pickle('PickleNew/changeInReportingLag.pickle')
    print('**Episode date data (day/week/prev. week)** - [Cases by episode date](https://docs.google.com/spreadsheets/d/e/2PACX-1vQ7fegCALd11ElozUYcMi-e9Dj69YaiNQhvEpk81JHsyTACl0UXkWK5zfMNFe49Tq3VuN9Av-fuEZqV/pubchart?oid=1644344261&format=interactive) and [historical averages of episode date](https://docs.google.com/spreadsheets/d/e/2PACX-1vQ7fegCALd11ElozUYcMi-e9Dj69YaiNQhvEpk81JHsyTACl0UXkWK5zfMNFe49Tq3VuN9Av-fuEZqV/pubchart?oid=1644344261&format=interactive)')
    print()
    print('* New cases with episode dates in last 3 days: ',
          format(int(changeInReportingLag.iloc[0:4, 0].sum()), ",d"), ' / ',
          format(int(changeInReportingLag.iloc[0:4, 0:7].sum().mean()), ",d"), ' / ',
          format(int(changeInReportingLag.iloc[0:4, 7:14].sum().mean()), ",d"),
          ' (', format(int(changeInReportingLag.iloc[0:4, 0].sum().mean()
                           - changeInReportingLag.iloc[0:4, 1:8].sum().mean()), "+,d"),
          ' vs. yesterday week avg)', sep='')

    print('* New cases - episode dates in last 7 days: ', format(int(changeInReportingLag.iloc[0:8, 0].sum()), ",d"),
          ' / ', format(int(changeInReportingLag.iloc[0:8, 0:7].sum().mean()), ",d"),
          ' / ', format(int(changeInReportingLag.iloc[0:8, 7:14].sum().mean()), ",d"),
          ' (', format(int(changeInReportingLag.iloc[0:8, 0].sum().mean()
                           - changeInReportingLag.iloc[0:8, 1:8].sum().mean()), "+,d"), ' vs. yesterday week avg)', sep='')
    print('* New cases - episode dates in last 30 days: ', format(int(changeInReportingLag.iloc[0:31, 0].sum()), ",d"),
          ' / ', format(int(changeInReportingLag.iloc[0:31, 0:7].sum().mean()), ",d"),
          ' / ', format(int(changeInReportingLag.iloc[0:31, 7:14].sum().mean()), ",d"),
          ' (', format(int(changeInReportingLag.iloc[0:31, 0].sum().mean()
                           - changeInReportingLag.iloc[0:31, 1:8].sum().mean()), "+,d"), ' vs. yesterday week avg)', sep='')
    print('* New cases - ALL episode dates: ', format(int(changeInReportingLag.iloc[:, 0].sum()), ",d"),
          ' / ', format(int(changeInReportingLag.iloc[:, 0:7].sum().mean()), ",d"),
          ' / ', format(int(changeInReportingLag.iloc[:, 7:14].sum().mean()), ",d"),
          ' (', format(int(changeInReportingLag.iloc[:, 0].sum().mean()
                           - changeInReportingLag.iloc[:, 1:8].sum().mean()), "+,d"), ' vs. yesterday week avg)', sep='')
    print()
    print('**Other data:**')
    print()
    print('* [7 day average:](https://docs.google.com/spreadsheets/d/e/2PACX-1vQ7fegCALd11ElozUYcMi-e9Dj69YaiNQhvEpk81JHsyTACl0UXkWK5zfMNFe49Tq3VuN9Av-fuEZqV/pubchart?oid=60082987&format=interactive) '
          + format(int(df['7 day SMA'].iloc[0]), ",d"),
          ' (', format((df['7 day SMA'].iloc[0] - df['7 day SMA'].iloc[1]), "+,.1f"),
          ' vs. yesterday)', ' (', format(int(df['7 day SMA'].iloc[0] - df['7 day SMA'].iloc[7]), "+,d"),
          ' or ', format((df['7 day SMA'].iloc[0] / df['7 day SMA'].iloc[7]) - 1, "+.1%"),
          ' vs. last week)', ', (', format(int(df['7 day SMA'].iloc[0] - df['7 day SMA'].iloc[30]), "+,d"),
          ' or ', format((df['7 day SMA'].iloc[0] / df['7 day SMA'].iloc[30]) - 1, "+.1%"), ' vs. 30 days ago)',
          sep='')
    print(f"* Today's Rt estimate: {df['Rt estimate'][0]:.2f} - [Historical](https://data.ontario.ca/en/dataset/effective-reproduction-number-re-for-covid-19-in-ontario)")
    print('* Active cases: ' + format(int(df['Confirmed Positive'].iloc[0]), ",d"),
          ' (', format(int(df['Confirmed Positive'].iloc[0] - df['Confirmed Positive'].iloc[1]), "+,d"),
          ' vs. yesterday)', ' (', format(int(df['Confirmed Positive'].iloc[0] - df['Confirmed Positive'].iloc[7]), "+,d"),
          ' vs. last week) - [Chart](https://docs.google.com/spreadsheets/d/e/2PACX-1vQ7fegCALd11ElozUYcMi-e9Dj69YaiNQhvEpk81JHsyTACl0UXkWK5zfMNFe49Tq3VuN9Av-fuEZqV/pubchart?oid=1136841100&format=interactive)',
          sep='')

    print('* Current hospitalizations: ' + format(CurrentHospitalizations, ",d")
          + '(' + format(ChangeInHospitalizations, "+,d")
          + '), ICUs: ' + format(CurrentICUs, ",d") + '(' + format(ChangeInICUs, "+,d")
          + '), Ventilated: ' + format(CurrentVent, ",d") + '(' + format(ChangeInVent, "+,d")
          + '), [vs. last week: ' + format(ChangeInHospitalizationsLast7, "+,d")
          + ' / ' + format(ChangeInICUsLast7, "+,d") + ' / ' + format(ChangeInVentLast7, "+,d")
          + '] - [Chart](https://docs.google.com/spreadsheets/d/e/2PACX-1vQ7fegCALd11ElozUYcMi-e9Dj69YaiNQhvEpk81JHsyTACl0UXkWK5zfMNFe49Tq3VuN9Av-fuEZqV/pubchart?oid=1392680472&format=interactive)'
          )

    HospitalizationsDF = pd.read_csv('Pickle/CumulativeHospitalizations.csv')
    if pd.to_datetime(HospitalizationsDF['Date'].max()) == pd.Timestamp(datetime.date.today()):
        print(f"* New hospitalizations (Week/prev week avgs.): "
              + f"{HospitalizationsDF.iloc[0]['NewHosp']:,.0f} ({HospitalizationsDF['NewHosp'][0:7].mean():,.1f} / {HospitalizationsDF['NewHosp'][7:14].mean():,.1f}), "
              + f"ICUs: {HospitalizationsDF.iloc[0]['NewICU']:,.0f} ({HospitalizationsDF['NewICU'][0:7].mean():,.1f} / {HospitalizationsDF['NewICU'][7:14].mean():,.1f}), "
              )
    else:
        print("* New hospitalizations/ICU data not published today")
    print('* Total reported cases to date: ' + format(int(df['Total Cases'].iloc[0]), ",d"),
          # ' *(', format(df['Total Cases'].iloc[0] / 14936396, ".2%"), ' of the population)*',
          sep='')

    # # Not published anymore
    # print('* **New variant cases** (Alpha/Beta/Gamma/Delta/Omicron): ' + format(NewVariants_UK, "+,d")
    #       + ' / ' + format(NewVariants_RSA, "+,d") + ' / ' + format(NewVariants_BRA, "+,d")
    #       + ' / ' + format(NewVariants_Delta, "+,d"), f"/ {NewVariants_Omicron:+,d}"
    #       + ' - [This data lags quite a bit](https://www.reddit.com/r/ontario/comments/ls8ohl/ontario_february_25_update_1138_new_cases_1094/gopq2kb/)')
    HospitalMetrics()

    # with open('TextOutput/DeathProjectionText.txt', 'r') as ff:
    #     DeathProjectionLines = ff.read()
    # print(DeathProjectionLines,end='')

    print('* Rolling case fatality rates for [outbreak](https://docs.google.com/spreadsheets/d/e/2PACX-1vQ7fegCALd11ElozUYcMi-e9Dj69YaiNQhvEpk81JHsyTACl0UXkWK5zfMNFe49Tq3VuN9Av-fuEZqV/pubchart?oid=1944697683&format=interactive) and [non-outbreak](https://docs.google.com/spreadsheets/d/e/2PACX-1vQ7fegCALd11ElozUYcMi-e9Dj69YaiNQhvEpk81JHsyTACl0UXkWK5zfMNFe49Tq3VuN9Av-fuEZqV/pubchart?oid=904668388&format=interactive) cases')
    print('* [Chart showing the 7 day average of cases per 100k by age group](https://docs.google.com/spreadsheets/d/e/2PACX-1vQ7fegCALd11ElozUYcMi-e9Dj69YaiNQhvEpk81JHsyTACl0UXkWK5zfMNFe49Tq3VuN9Av-fuEZqV/pubchart?oid=1925334241&format=interactive)')
    print('* Cases and vaccinations by [postal codes (first 3 letters)](https://www.ices.on.ca/DAS/AHRQ/COVID-19-Dashboard)')
    print('* [Details on post-vaccination cases](https://www.publichealthontario.ca/-/media/documents/ncov/epi/covid-19-epi-confirmed-cases-post-vaccination.pdf)')
    print()
    print('**With/For COVID Data:**')
    print()
    WithForCOVIDHospDF = pd.read_pickle('Pickle/WithForCOVID_Hosp.pickle')
    print(f"* {WithForCOVIDHospDF['hosp_for_covid'][0]:.1%} / {WithForCOVIDHospDF['icu_for_covid'][0]:.1%}",
          f"of the hospitalizations/ICU numbers are people there FOR COVID.")

    WithForCOVIDDeathsDF = pd.read_pickle('Pickle/WithForCOVID_Deaths.pickle')
    total_deaths = WithForCOVIDDeathsDF['deaths_total'].iloc[0]
    total_deaths_caused = WithForCOVIDDeathsDF['death_covid'].iloc[0]
    total_deaths_contrib = WithForCOVIDDeathsDF['death_covid_contrib'].iloc[0]
    death_unknown_missing = WithForCOVIDDeathsDF['death_unknown_missing'].iloc[0]
    print(f'* Of the {total_deaths} deaths with details today, {total_deaths_caused} were caused '
          + f'by COVID, {total_deaths_contrib} were contributed to by COVID and '
          + f'{death_unknown_missing} were unknown')
    print()
    print('**LTC Data:**')
    print()
    print('*', int(df['Total Positive LTC Resident Cases'][0] - df['Total Positive LTC Resident Cases'][1]),
          '/', int(df['Total Positive LTC HCW Cases'][0] - df['Total Positive LTC HCW Cases'][1]),
          'new LTC resident/HCW cases - [Chart of active 70+ cases split by outbreak and non-outbreak cases](https://docs.google.com/spreadsheets/d/e/2PACX-1vQ7fegCALd11ElozUYcMi-e9Dj69YaiNQhvEpk81JHsyTACl0UXkWK5zfMNFe49Tq3VuN9Av-fuEZqV/pubchart?oid=2044830410&format=interactive)')
    print('* ', df['New LTC Deaths'][0], ' / ', df['New LTC Deaths'][0:7].sum(),
          ' / ', df['New LTC Deaths'][0:30].sum(), ' / ', df['New LTC Deaths'][0:100].sum(),
          ' / ', df['New LTC Deaths'].sum(), ' LTC deaths in last day / week / 30 / 100 days / all-time',
          sep='')

    sys.stdout = ConsoleOut

    sys.stdout = open(SchoolsFileName, 'w')
    print("**Schools data:** - *(latest data as of ",
          dfschools['reported_date'].max().strftime('%B %d'),
          ')* - [Source](https://data.ontario.ca/dataset/summary-of-cases-in-schools)',
          sep='')
    print()
    print('*', str(NewSchoolCases) + ' new cases (' + str(NewStudentCases)
          + '/' + str(NewStaffCases) + ' student/staff split). '
          + str(SchoolsWithCases) + ' (' + format(PctSchoolWithCases, ".1%")
          + ' of all) schools have active cases. ' + str(SchoolsClosed)
          + ' schools currently closed.')
    print('* Top 10 municipalities by number of schools with active cases (number of cases)): ')
    print('* ', end='')
    for x in range(10):
        print(dfSchoolsWithActivePivot.index[x], ': ', dfSchoolsWithActivePivot.iloc[x, 0],
              ' (', dfSchoolsWithActivePivot.iloc[x, 1], ')', end=', ', sep='')
    print()
    dfSchoolsWithActive = dfSchoolsWithActive[dfSchoolsWithActive['total_confirmed_cases'] >= 15]
    print('* **Schools with 10+ active cases:** ', end='')
    for x in range(len(dfSchoolsWithActive)):
        print(dfSchoolsWithActive.iloc[x]['school'], ' (',
              dfSchoolsWithActive.iloc[x]['total_confirmed_cases'], ')',
              ' (', dfSchoolsWithActive.iloc[x]['municipality'], ')',
              end=', ', sep='')
    print()

    sys.stdout = ConsoleOut
    ChildCareData()
    print()

    endTime = datetime.datetime.now()
    TimeTaken = (endTime - starttime).total_seconds()
    print(f'Ended:   {endTime:%Y-%m-%d %H:%M:%S} {TimeTaken:.2f} seconds')
    print('------------------------------------------------------------------------')


def ChildCareData():
    ConsoleOut = sys.stdout
    starttime = datetime.datetime.now()
    TextFileName = 'TextOutput/ChildCareData.txt'
    print('------------------------------------------------------------------------')
    print(f'ChildCareData() \nStarted: {starttime:%Y-%m-%d %H:%M:%S}')

    df = pd.read_csv('https://data.ontario.ca/dataset/5bf54477-6147-413f-bab0-312f06fcb388/resource/74f9ac9f-7ca8-4860-b2c3-189a2c25e30c/download/lcccovidsummary.csv')
    df.to_csv('SourceFiles/ChildCareData-summary.csv')
    df['reported_date'] = pd.to_datetime(df['reported_date'], format='%Y-%m-%d')
    df = df.set_index('reported_date')
    df = df.sort_index(ascending=False)

    TodaysData = df.iloc[0]
    NewCasesTotal = TodaysData['new_total_lcc_related_cases'].astype(int)
    NewChildCasesTotal = TodaysData['new_lcc_related_child_cases'].astype(int)
    # NewStaffCasesTotal = NewCasesTotal - NewChildCasesTotal
    NewCentresClosed = (TodaysData['new_lcc_centres_closed'].astype(int)
                        + TodaysData['new_lcc_homes_closed'].astype(int))
    CentresWithCases = TodaysData['current_lcc_centres_w_cases']
    CentresCurrentlyClosed = TodaysData['current_lcc_centres_closed']

    df2 = pd.read_csv('https://data.ontario.ca/dataset/5bf54477-6147-413f-bab0-312f06fcb388/resource/eee282d3-01e6-43ac-9159-4ba694757aea/download/lccactivecovid.csv',
                      encoding='cp1252')
    df2.to_csv('SourceFiles/ChildCareData-active.csv')
    df2['reported_date'] = pd.to_datetime(df2['reported_date'], format='%Y-%m-%d')
    df2 = df2.set_index('reported_date')
    df2 = df2.sort_index(ascending=False)
    df2 = df2.loc[df2.index.max()]
    df2 = df2.sort_values(by='total_confirmed_cases', ascending=False)
    df2 = df2[df2['total_confirmed_cases'] >= 5]

    sys.stdout = open(TextFileName, 'w')
    print("**Child care centre data:** - *(latest data as of ",
          (df.iloc[0].name + datetime.timedelta(days=0)).strftime('%B %d'),
          ')* - [Source](https://data.ontario.ca/dataset/summary-of-cases-in-licensed-child-care-settings)',
          sep='')
    print()
    print('*', NewCasesTotal, '/', df.iloc[0:7]['new_total_lcc_related_cases'].sum().astype(int).astype(str),
          'new cases in the last day/week')
    print('* There are currently ', CentresWithCases, ' centres with cases ', '(',
          "{:.2%}".format(CentresWithCases / TodaysData['current_total_number_lcc_centres']),
          ' of all)', sep='')
    print('* ', NewCentresClosed, ' centres closed in the last day. ', CentresCurrentlyClosed,
          ' centres are currently closed ', sep='')
    print('* **LCCs with 5+ active cases:** ', end='')
    for x in range(len(df2)):
        print(df2.iloc[x]['lcc_name'], ' (', df2.iloc[x]['total_confirmed_cases'], ')',
              ' *(', df2.iloc[x]['municipality'], ')*', end=', ', sep='')
    print()
    sys.stdout = ConsoleOut

    endTime = datetime.datetime.now()
    TimeTaken = (endTime - starttime).total_seconds()
    print(f'Ended:   {endTime:%Y-%m-%d %H:%M:%S} {TimeTaken:.2f} seconds')
    print('------------------------------------------------------------------------')


def HospitalMetrics(download=True):
    picklefilename = 'Pickle/HospitalData.pickle'
    if download:
        df = pd.read_csv('https://data.ontario.ca/dataset/8f3a449b-bde5-4631-ada6-8bd94dbc7d15/resource/e760480e-1f95-4634-a923-98161cfb02fa/download/region_hospital_icu_covid_data.csv')
        df.to_csv('SourceFiles/HospitalMetrics.csv')
        try:
            df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y')
        except ValueError:
            df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
        df.set_index('oh_region', inplace=True)

    else:
        df = pd.read_pickle(picklefilename)
    for region in set(df.index):
        df.loc[region] = df.loc[region].fillna(method='ffill')
    for column in df.columns[1:]:
        df[column] = df[column].astype(int)
    TodaysDate = pd.Timestamp(df['date'].max())
    WeekAgoDate = TodaysDate - datetime.timedelta(days=7)
    TodaysDF = df[df['date'] == TodaysDate]

    df.to_pickle('Pickle/HospitalData.pickle')
    print('* Hospitalizations / ICUs/ +veICU count by [Ontario Health Region](https://news.ontario.ca/en/release/54585/ontario-taking-next-steps-to-integrate-health-care-system) (ICUs vs. last week): ',
          end="")

    for x in set(df.index):
        ICUchangeOverLastWeek = (df[df['date'] == TodaysDate].loc[x]['icu_crci_total']
                                 - df[df['date'] == WeekAgoDate].loc[x]['icu_crci_total'])
        print(x.capitalize(), ': ', TodaysDF.loc[x]['hospitalizations'], '/',
              TodaysDF.loc[x]['icu_crci_total'], '/', TodaysDF.loc[x]['icu_current_covid'], '(',
              format(ICUchangeOverLastWeek, "+,d"), ')', end=", ", sep="")

    print('Total: ', TodaysDF['hospitalizations'].sum(), '/', TodaysDF['icu_crci_total'].sum(),
          '/', TodaysDF['icu_current_covid'].sum())
    print()


def JailData():
    ConsoleOut = sys.stdout
    starttime = time.time()
    TextFileName = 'TextOutput/JailData.txt'

    df = pd.read_csv('https://data.ontario.ca/dataset/c4022f0f-6f3d-4e16-bd28-5312333a4bac/resource/d0d6ccc7-fc60-4a18-ac96-7f9493e9f10e/download/inmatetesting.csv')
    df.to_csv('SourceFiles/JailData-inmatetesting.csv')
    # df['Cumulative_Number_of_Tests_as_of_Reported_Date'] = df['Cumulative_Number_of_Tests_as_of_Reported_Date'].str.replace(',', '').astype(int)
    # df['Cumulative_Number_of_Negative_Tests_as_of_Reported_Date'] = df['Cumulative_Number_of_Negative_Tests_as_of_Reported_Date'].str.replace(',', '').astype(int)

    pivotCompletedTest = pd.pivot_table(df, values='Cumulative_Number_of_Tests_as_of_Reported_Date',
                                        index='Region', columns='Reported_Date', aggfunc=np.sum,
                                        margins=False)
    # pivotCompletedTest = pd.pivot_table(df,values='Total Tests',index = 'Region',columns = 'Date',aggfunc=np.sum, margins=False)

    pivotCompletedTest = pivotCompletedTest.reindex(columns=sorted(pivotCompletedTest.columns,
                                                                   reverse=True))
    TotalTestsCompletedByDay = pivotCompletedTest.iloc[:, ].sum()
    pivotRefusedTest = pd.pivot_table(df, values='Total_Inmates_that_Refused_Swab_as_of_Reported_Date',
                                      index='Region', columns='Reported_Date', aggfunc=np.sum, margins=False)
    # pivotRefusedTest = pd.pivot_table(df,values='Refused Swab',index = 'Region',columns = 'Date',aggfunc=np.sum, margins=False)
    pivotRefusedTest = pivotRefusedTest.reindex(columns=sorted(pivotRefusedTest.columns, reverse=True))
    TotalTestsRefusedByDay = pivotRefusedTest.iloc[:, ].sum().astype(int)

    df2 = pd.read_csv('https://data.ontario.ca/dataset/ecb75ea0-8b72-4f46-a14a-9bd54841d6ab/resource/1f95eda9-53b5-448e-abe0-afc0b71581ed/download/correctionsinmatecases.csv')
    df2.to_csv('SourceFiles/JailData-cases.csv')
    # df2 = pd.read_csv('https://data.ontario.ca/dataset/ecb75ea0-8b72-4f46-a14a-9bd54841d6ab/resource/1f95eda9-53b5-448e-abe0-afc0b71581ed/download/correctionsinmatecases.csv')
    df2['Reported_Date'] = pd.to_datetime(df2['Reported_Date'], format='%Y-%m-%d')
    df2 = df2.drop_duplicates(keep='first')

    # df2 = df2.fillna(0)
    # df2['Date_of_Release'] = df2['Date_of_Release'].fillna(method = 'backfill')
    # df2['Date_of_Release'] = pd.to_datetime(df2['Date_of_Release'],format = '%Y-%m-%d')

    df2 = df2.fillna(0)
    # df2['TotalPositive']= df2['Positive COVID_Ongoing']+df2['Positive COVID_Resolved']+df2['Positive_Cases_Released_from_Custody']
    df2['TotalPositive'] = (df2['Total_Active_Inmate_Cases_As_Of_Reported_Date']
                            + df2['Cumul_Nr_Resolved_Inmate_Cases_As_Of_Reported_Date']
                            + df2['Cumul_Nr_Positive_Released_Inmate_Cases_As_Of_Reported_Date'])
    df2 = df2.drop_duplicates(keep='last', subset=['Reported_Date', 'Institution'])

    TotalInmateByJailByDatePivot = pd.pivot_table(df2, values='TotalPositive',
                                                  index='Institution', columns='Reported_Date',
                                                  aggfunc=np.sum, margins=False)

    TotalInmateByJailByDatePivot = TotalInmateByJailByDatePivot.reindex(columns=sorted(TotalInmateByJailByDatePivot.columns,
                                                                                       reverse=True))
    TotalInmateByJailByDatePivot = TotalInmateByJailByDatePivot.fillna(0)
    TotalInmateCasesByDay = TotalInmateByJailByDatePivot.iloc[:, ].sum().astype(int)
    DailyChangeInJails = TotalInmateByJailByDatePivot - TotalInmateByJailByDatePivot.shift(-1, axis=1)
    DailyChangeInJails = DailyChangeInJails.sort_values(by=DailyChangeInJails.columns.max(),
                                                        ascending=False)
    DailyChangeInJails.fillna(0, inplace=True)
    DailyChangeInJails = DailyChangeInJails.astype(int)

    sys.stdout = open(TextFileName, 'w')
    print('**Jail Data** - *(latest data as of ', (df2['Reported_Date'].max() + datetime.timedelta(days=0)).strftime('%B %d'),
          ')* [Source](https://data.ontario.ca/en/dataset/covid-19-testing-of-inmates-in-ontario-s-correctional-institutions)',
          sep='')
    print()
    print('* Total inmate cases in last day/week: ', TotalInmateCasesByDay[0] - TotalInmateCasesByDay[1],
          '/', TotalInmateCasesByDay[0] - TotalInmateCasesByDay[7], sep="")
    print('* Total inmate tests completed in last day/week (refused test in last day/week): ',
          TotalTestsCompletedByDay[0] - TotalTestsCompletedByDay[1], '/', TotalTestsCompletedByDay[0] - TotalTestsCompletedByDay[7],
          ' (', TotalTestsRefusedByDay[0] - TotalTestsRefusedByDay[1], '/', TotalTestsRefusedByDay[0] - TotalTestsRefusedByDay[7],
          ')', sep="")

    print('* Jails with 2+ cases yesterday: ', end=' ')
    dfa = DailyChangeInJails[DailyChangeInJails[DailyChangeInJails.columns[0]] >= 2]
    dfa = dfa[DailyChangeInJails.columns.max()]
    for x in range(dfa.shape[0]):
        print(dfa.index[x], ': ', dfa[x], end=', ', sep='')
    print()
    sys.stdout = ConsoleOut

    # with open('FFF.csv', 'a', newline='') as f:
    #     f.write('Jail Data \n')
    #     DailyChangeInJails.to_csv(f, header=True)
    #     f.write('\n')

    print('------------------------------------------------------------------------')
    print('JailData: (time) ', (round(time.time() - starttime, 2)))
    print('------------------------------------------------------------------------')


def LoadCOVIDDataSupport(currentDataFrame,filename):
    currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Reporting_PHU_Address').columns), axis = 1, inplace = True)
    currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Reporting_PHU_City').columns), axis = 1, inplace = True)
    currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Reporting_PHU_Postal_Code').columns), axis = 1, inplace = True)
    currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Reporting_PHU_Website').columns), axis = 1, inplace = True)
    currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Reporting_PHU_Latitude').columns), axis = 1, inplace = True)
    currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Reporting_PHU_Longitude').columns), axis = 1, inplace = True)
    currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Days to report').columns), axis = 1, inplace = True)

    filenameLength = len(filename)
    ################
    try: FileNameDate = datetime.datetime(int(filename[0:4]),int(filename[5:7]),int(filename[8:10]))

    except ValueError as ve:
        FileNameDate = datetime.datetime(2020,int(filename[filenameLength-14:filenameLength-12]),int(filename[filenameLength-11:filenameLength-9]))

    ################
    currentDataFrame['File_Date'] = FileNameDate
    """".strftime("%d-%b")"""

    currentDataFrame.rename(columns = {'Accurate_Episode_Date':'Episode_Date', 'CLIENT_GENDER':'Client_Gender',
                     'CASE_ACQUISITIONINFO': 'Case_AcquisitionInfo',
             'Outcome1':'Outcome'}, inplace = True)



    currentDataFrame['Episode_Date'] = pd.to_datetime(currentDataFrame['Episode_Date'],format = '%Y-%m-%d',errors ='coerce')
    currentDataFrame['Case_Reported_Date'] = pd.to_datetime(currentDataFrame['Case_Reported_Date'],format = '%Y-%m-%d',errors ='coerce')
    currentDataFrame['Test_Reported_Date'] = pd.to_datetime(currentDataFrame['Test_Reported_Date'],format = '%Y-%m-%d',errors ='coerce')
    currentDataFrame['Specimen_Date'] = pd.to_datetime(currentDataFrame['Specimen_Date'],format = '%Y-%m-%d',errors ='coerce')
    currentDataFrame['Reporting_Lag'] = currentDataFrame['File_Date'] - currentDataFrame['Episode_Date']
    #Essential to organize and sort columns in DataFrame PRIOR to concatenation.
    #currentDataFrame = currentDataFrame[['Row_ID','Episode_Date','Case_Reported_Date','Test_Reported_Date','Specimen_Date','Age_Group','Client_Gender','Case_AcquisitionInfo','Outcome','Outbreak_Related','Reporting_PHU','File_Date','Reporting_PHU_ID','Reporting_Lag']]

    currentDataFrame['Age_Group'] = currentDataFrame['Age_Group'].replace(['<20'],'19 & under')
    currentDataFrame['Age_Group'] = currentDataFrame['Age_Group'].replace(['90s'],'90+')
    currentDataFrame['Age_Group'] = currentDataFrame['Age_Group'].replace(['UNKNOWN'],'Unknown')
    currentDataFrame['Age_Group'] = currentDataFrame['Age_Group'].fillna('Unknown')
    currentDataFrame['Client_Gender'] = currentDataFrame['Client_Gender'].fillna('Unknown')

    currentDataFrame['Episode_Date'] = currentDataFrame['Episode_Date'].replace(['00:00:00'],'2000-01-01')
    currentDataFrame['Episode_Date'] = currentDataFrame['Episode_Date'].fillna('2000-01-01')
    currentDataFrame['Episode_Date'] = pd.to_datetime(currentDataFrame['Episode_Date'], format = '%Y-%m-%d')
    currentDataFrame['Case_AcquisitionInfo'] = currentDataFrame['Case_AcquisitionInfo'].replace(['CC','Contact of a confirmed case'],'Close contact')
    #currentDataFrame['Case_AcquisitionInfo'] = currentDataFrame['Case_AcquisitionInfo'].replace(['Contact of a confirmed case'],'Close contact')
    #currentDataFrame['Case_AcquisitionInfo'] = currentDataFrame['Case_AcquisitionInfo'].replace(['Information pending'],'No Info-Missing')
    #currentDataFrame['Case_AcquisitionInfo'] = currentDataFrame['Case_AcquisitionInfo'].replace(['Neither'],'No Info-Unk')
    currentDataFrame['Case_AcquisitionInfo'] = currentDataFrame['Case_AcquisitionInfo'].replace(['OB'],'Outbreak')
    currentDataFrame['Case_AcquisitionInfo'] = currentDataFrame['Case_AcquisitionInfo'].replace(['TRAVEL','Travel-Related'],'Travel')
    currentDataFrame['Case_AcquisitionInfo'].replace(['Neither','Information pending','No Info-Missing','No Info-Unk','Missing Information','No Epi-link','No known epi link','Unspecified epi link','NO KNOWN EPI LINK','MISSING INFORMATION','UNSPECIFIED EPI LINK'],'Community',inplace=True)
    #currentDataFrame['Case_AcquisitionInfo'].replace(['TRAVEL'],'Travel',inplace=True)


    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Toronto Public Health'],'Toronto PHU')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Peel Public Health'],'Peel')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['York Region Public Health Services'],'York')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Region of Waterloo, Public Health'],'Waterloo Region')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Durham Region Health Department'],'Durham')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Hamilton Public Health Services'],'Hamilton')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Middlesex-London Health Unit'],'London')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Halton Region Health Department'],'Halton')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Simcoe Muskoka District Health Unit'],'Simcoe-Muskoka')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Niagara Region Public Health Department'],'Niagara')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Windsor-Essex County Health Unit'],'Windsor')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Wellington-Dufferin-Guelph Public Health'],'Wellington-Guelph')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Kingston, Frontenac and Lennox & Addington Public Health'],'Kingston')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Southwestern Public Health'],'Southwestern')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Chatham-Kent Health Unit'],'Chatham-Kent')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Ottawa Public Health'],'Ottawa')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Algoma Public Health Unit'],'Algoma')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Thunder Bay District Health Unit'],'Thunder Bay')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Timiskaming Health Unit'],'Timiskaming')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Porcupine Health Unit'],'Porcupine')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Sudbury & District Health Unit'],'Sudbury')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Brant County Health Unit'],'Brant')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Eastern Ontario Health Unit'],'Eastern Ontario')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Leeds, Grenville and Lanark District Health Unit'],'Leeds, Grenville, Lanark')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Haldimand-Norfolk Health Unit'],'Haldimand-Norfolk')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Lambton Public Health'],'Lambton')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Haliburton, Kawartha, Pine Ridge District Health Unit'],'Haliburton, Kawartha')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Grey Bruce Health Unit'],'Grey Bruce')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Huron Perth District Health Unit'],'Huron Perth')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Peterborough Public Health'],'Peterborough')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Renfrew County and District Health Unit'],'Renfrew')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Hastings and Prince Edward Counties Health Unit'],'Hastings')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['Northwestern Health Unit'],'Northwestern')
    currentDataFrame['Reporting_PHU'] = currentDataFrame['Reporting_PHU'].replace(['North Bay Parry Sound District Health Unit'],'North Bay')

    currentDataFrame = currentDataFrame.sort_index(axis=1)

    return currentDataFrame


def LoadCOVIDData(filenameIn):
    # pickleFileName = config.get('file_location', 'master_dataframe')

    filename = filenameIn

    starttime = time.time()

    path = config.get('folder_location', 'confirmed_cases_file')

    currentDataFrame = pd.DataFrame()
    MasterDataFrame = pd.DataFrame()

    print(round((time.time()-starttime),2),'- before doing anything')

    IndivFilePath = config.get('folder_location', 'indiv_pickles')

    if filename == "":
        for filename in os.listdir(path):
            with open(os.path.join(path, filename), 'r') as f:
                #print(f)
                currentDataFrame = pd.read_csv(f)

                "currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Row_ID').columns), axis = 1, inplace = True)"
                currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Reporting_PHU_Address').columns), axis = 1, inplace = True)
                currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Reporting_PHU_City').columns), axis = 1, inplace = True)
                currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Reporting_PHU_Postal_Code').columns), axis = 1, inplace = True)
                currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Reporting_PHU_Website').columns), axis = 1, inplace = True)
                currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Reporting_PHU_Latitude').columns), axis = 1, inplace = True)
                currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Reporting_PHU_Longitude').columns), axis = 1, inplace = True)
                currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Days to report').columns), axis = 1, inplace = True)
                currentDataFrame['Outbreak_Related'] = currentDataFrame['Outbreak_Related'].replace('(blank)','No')
                filenameLength = len(filename)
                ################
                try: FileNameDate = datetime.datetime(int(filename[0:4]),int(filename[5:7]),int(filename[8:10]))

                except ValueError as ve:
                    FileNameDate = datetime.datetime(2020,int(filename[filenameLength-14:filenameLength-12]),int(filename[filenameLength-11:filenameLength-9]))


                ################
                currentDataFrame['File_Date'] = FileNameDate
                """".strftime("%d-%b")"""

                currentDataFrame.rename(columns = {'Accurate_Episode_Date':'Episode_Date', 'CLIENT_GENDER':'Client_Gender',
                                 'CASE_ACQUISITIONINFO': 'Case_AcquisitionInfo',
                         'Outcome1':'Outcome'}, inplace = True)


                PHU_Rename(currentDataFrame)
                currentDataFrame['Reporting_Lag'] = currentDataFrame['File_Date'] - currentDataFrame['Episode_Date']


                MasterDataFrame = pd.concat([MasterDataFrame,currentDataFrame],ignore_index=True)

                print(round((time.time()-starttime),2),FileNameDate)

                #print(filename)

    else:
        #MasterDataFrame = pd.read_pickle(pickleFileName)

        with open(os.path.join(path, (filename+' Data.csv')), 'r') as f:
               currentDataFrame = pd.read_csv(f)
               currentDataFrame['Accurate_Episode_Date'] = currentDataFrame['Accurate_Episode_Date'].fillna(currentDataFrame['Test_Reported_Date'])
               currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Reporting_PHU_Address').columns), axis = 1, inplace = True)
               currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Reporting_PHU_City').columns), axis = 1, inplace = True)
               currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Reporting_PHU_Postal_Code').columns), axis = 1, inplace = True)
               currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Reporting_PHU_Website').columns), axis = 1, inplace = True)
               currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Reporting_PHU_Latitude').columns), axis = 1, inplace = True)
               currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Reporting_PHU_Longitude').columns), axis = 1, inplace = True)
               currentDataFrame.drop(list(currentDataFrame.filter(regex = 'Days to report').columns), axis = 1, inplace = True)

               filenameLength = len(filename)

               ################
               try: FileNameDate = datetime.datetime(int(filename[0:4]),int(filename[5:7]),int(filename[8:10]))

               except ValueError as ve:
                   FileNameDate = datetime.datetime(2020,int(filename[filenameLength-14:filenameLength-12]),int(filename[filenameLength-11:filenameLength-9]))


               ################
               currentDataFrame['File_Date'] = FileNameDate


               currentDataFrame.rename(columns = {'Accurate_Episode_Date':'Episode_Date', 'CLIENT_GENDER':'Client_Gender',
                                'CASE_ACQUISITIONINFO': 'Case_AcquisitionInfo',
                        'Outcome1':'Outcome'}, inplace = True)

               currentDataFrame['Outbreak_Related'] = currentDataFrame['Outbreak_Related'].fillna('No')
               currentDataFrame['Episode_Date'] = pd.to_datetime(currentDataFrame['Episode_Date'],format = '%Y-%m-%d',errors ='coerce')
               currentDataFrame['Case_Reported_Date'] = pd.to_datetime(currentDataFrame['Case_Reported_Date'],format = '%Y-%m-%d',errors ='coerce')
               currentDataFrame['Test_Reported_Date'] = pd.to_datetime(currentDataFrame['Test_Reported_Date'],format = '%Y-%m-%d',errors ='coerce')
               currentDataFrame['Specimen_Date'] = pd.to_datetime(currentDataFrame['Specimen_Date'],format = '%Y-%m-%d',errors ='coerce')
               currentDataFrame['Reporting_Lag'] = currentDataFrame['File_Date'] - currentDataFrame['Episode_Date']

               #print(round((time.time()-starttime),2),'- before concatenation')
               #Essential to organize and sort columns in DataFrame PRIOR to concatenation.
               currentDataFrame = currentDataFrame[['Row_ID','Episode_Date','Case_Reported_Date','Test_Reported_Date','Specimen_Date','Age_Group','Client_Gender','Case_AcquisitionInfo','Outcome','Outbreak_Related','Reporting_PHU','File_Date','Reporting_PHU_ID','Reporting_Lag']]
               #print(round((time.time()-starttime),2),'- after reorg columns')
               PHU_Rename(currentDataFrame)


               #MasterDataFrame = pd.concat([MasterDataFrame,currentDataFrame],ignore_index=True)

               currentDataFrame.to_pickle(IndivFilePath+'\\'+FileNameDate.strftime("%Y-%m-%d")+' - Source.pickle')
               # currentDataFrame.to_pickle(r'D:\Pickle\\'+FileNameDate.strftime("%Y-%m-%d")+' - Source.pickle')


    print(round((time.time()-starttime),2),'- concatenation done')







    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Toronto Public Health'],'Toronto PHU')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Peel Public Health'],'Peel')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['York Region Public Health Services'],'York')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Region of Waterloo, Public Health'],'Waterloo Region')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Durham Region Health Department'],'Durham')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Hamilton Public Health Services'],'Hamilton')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Middlesex-London Health Unit'],'London')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Halton Region Health Department'],'Halton')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Simcoe Muskoka District Health Unit'],'Simcoe-Muskoka')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Niagara Region Public Health Department'],'Niagara')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Windsor-Essex County Health Unit'],'Windsor')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Wellington-Dufferin-Guelph Public Health'],'Wellington-Guelph')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Kingston, Frontenac and Lennox & Addington Public Health'],'Kingston')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Southwestern Public Health'],'Southwestern')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Chatham-Kent Health Unit'],'Chatham-Kent')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Ottawa Public Health'],'Ottawa')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Algoma Public Health Unit'],'Algoma')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Thunder Bay District Health Unit'],'Thunder Bay')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Timiskaming Health Unit'],'Timiskaming')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Porcupine Health Unit'],'Porcupine')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Sudbury & District Health Unit'],'Sudbury')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Brant County Health Unit'],'Brant')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Eastern Ontario Health Unit'],'Eastern Ontario')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Leeds, Grenville and Lanark District Health Unit'],'Leeds, Greenville, Lanark')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Haldimand-Norfolk Health Unit'],'Haldimand-Norfolk')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Lambton Public Health'],'Lambton')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Haliburton, Kawartha, Pine Ridge District Health Unit'],'Haliburton, Kawartha')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Grey Bruce Health Unit'],'Grey Bruce')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Huron Perth District Health Unit'],'Huron Perth')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Peterborough Public Health'],'Peterborough')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Renfrew County and District Health Unit'],'Renfrew')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Hastings and Prince Edward Counties Health Unit'],'Hastings')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Northwestern Health Unit'],'Northwestern')
    # MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['North Bay Parry Sound District Health Unit'],'North Bay')
    # print(round((time.time()-starttime),2),'- PHU Names redone')



    #MasterDataFrame['Reporting_Lag'] = MasterDataFrame['File_Date'] - MasterDataFrame['Episode_Date']
    print(round((time.time()-starttime),2),'- Reporting Lag calculated')

    # Don't think we need to sort it.
    # MasterDataFrame.sort_values(by='File_Date',ascending = True, inplace = True)
    # print(round((time.time()-starttime),2),'- DataFrame sorted')


    MasterDataFrame.rename(columns = {'Accurate_Episode_Date':'Episode_Date',
                                      'Outcome1':'Outcome'}, inplace = True)
    print(round((time.time()-starttime),2),'- Columns renamed')



    #indexNamesToDrop = MasterDataFrame[MasterDataFrame['File_Date'] == '2020-04-15'].index
    #MasterDataFrame.drop(indexNamesToDrop,inplace = True)
    #print(round((time.time()-starttime),2),'- Columns deleted')


    # MasterDataFrame.to_pickle(pickleFileName)
    del MasterDataFrame

    print('LoadCOVIDData: (time)',time.time()-starttime)
    print('------------------------------------------------------------------------')

def PHU_Rename(MasterDataFrame):
    starttime = time.time()

    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Toronto Public Health'],'Toronto PHU')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Peel Public Health'],'Peel')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['York Region Public Health Services'],'York')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Region of Waterloo, Public Health'],'Waterloo Region')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Durham Region Health Department'],'Durham')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Hamilton Public Health Services'],'Hamilton')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Middlesex-London Health Unit'],'London')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Halton Region Health Department'],'Halton')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Simcoe Muskoka District Health Unit'],'Simcoe-Muskoka')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Niagara Region Public Health Department'],'Niagara')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Windsor-Essex County Health Unit'],'Windsor')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Wellington-Dufferin-Guelph Public Health'],'Wellington-Guelph')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Kingston, Frontenac and Lennox & Addington Public Health'],'Kingston')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Southwestern Public Health'],'Southwestern')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Chatham-Kent Health Unit'],'Chatham-Kent')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Ottawa Public Health'],'Ottawa')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Algoma Public Health Unit'],'Algoma')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Thunder Bay District Health Unit'],'Thunder Bay')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Timiskaming Health Unit'],'Timiskaming')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Porcupine Health Unit'],'Porcupine')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Sudbury & District Health Unit'],'Sudbury')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Brant County Health Unit'],'Brant')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Eastern Ontario Health Unit'],'Eastern Ontario')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Leeds, Grenville and Lanark District Health Unit'],'Leeds, Grenville, Lanark')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Haldimand-Norfolk Health Unit'],'Haldimand-Norfolk')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Lambton Public Health'],'Lambton')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Haliburton, Kawartha, Pine Ridge District Health Unit'],'Haliburton, Kawartha')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Grey Bruce Health Unit'],'Grey Bruce')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Huron Perth District Health Unit'],'Huron Perth')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Peterborough Public Health'],'Peterborough')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Renfrew County and District Health Unit'],'Renfrew')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Hastings and Prince Edward Counties Health Unit'],'Hastings')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['Northwestern Health Unit'],'Northwestern')
    MasterDataFrame['Reporting_PHU'] = MasterDataFrame['Reporting_PHU'].replace(['North Bay Parry Sound District Health Unit'],'North Bay')

    print(round((time.time()-starttime),2),'- Name replacements done')

    MasterDataFrame['Episode_Date'] = pd.to_datetime(MasterDataFrame['Episode_Date'],format = '%Y-%m-%d',errors ='coerce')

    if set(['Case_Reported_Date','Test_Reported_Date','Specimen_Date']).issubset(MasterDataFrame.columns):
        MasterDataFrame['Case_Reported_Date'] = pd.to_datetime(MasterDataFrame['Case_Reported_Date'],format = '%Y-%m-%d',errors ='coerce')
        MasterDataFrame['Test_Reported_Date'] = pd.to_datetime(MasterDataFrame['Test_Reported_Date'],format = '%Y-%m-%d',errors ='coerce')
        MasterDataFrame['Specimen_Date'] = pd.to_datetime(MasterDataFrame['Specimen_Date'],format = '%Y-%m-%d',errors ='coerce')
    else:
        MasterDataFrame['Case_Reported_Date'] = datetime.datetime(2000,1,1)
        MasterDataFrame['Test_Reported_Date'] = datetime.datetime(2000,1,1)
        MasterDataFrame['Specimen_Date'] = datetime.datetime(2000,1,1)

    MasterDataFrame['Episode_Date'] = MasterDataFrame['Episode_Date'].replace(['00:00:00'],'2000-01-01')
    MasterDataFrame['Episode_Date'] = MasterDataFrame['Episode_Date'].fillna('2000-01-01')
    #MasterDataFrame['Episode_Date'] = pd.to_datetime(MasterDataFrame['Episode_Date'], format = '%Y-%m-%d')


    print(round((time.time()-starttime),2),'- Date columns done')


    MasterDataFrame['Age_Group'] = MasterDataFrame['Age_Group'].replace(['<20'],'19 & under')
    MasterDataFrame['Age_Group'] = MasterDataFrame['Age_Group'].replace(['90s'],'90+')
    MasterDataFrame['Age_Group'] = MasterDataFrame['Age_Group'].replace(['UNKNOWN'],'Unknown')
    MasterDataFrame['Age_Group'] = MasterDataFrame['Age_Group'].fillna('Unknown')
    MasterDataFrame['Client_Gender'] = MasterDataFrame['Client_Gender'].fillna('Unknown')
    print(round((time.time()-starttime),2),'- Age columns done')


    MasterDataFrame['Case_AcquisitionInfo'] = MasterDataFrame['Case_AcquisitionInfo'].replace(['CC','Contact of a confirmed case'],
                                                                                              'Close contact')
    #MasterDataFrame['Case_AcquisitionInfo'] = MasterDataFrame['Case_AcquisitionInfo'].replace(['Contact of a confirmed case'],'Close contact')
    #MasterDataFrame['Case_AcquisitionInfo'] = MasterDataFrame['Case_AcquisitionInfo'].replace(['Information pending'],'No Info-Missing')
    #MasterDataFrame['Case_AcquisitionInfo'] = MasterDataFrame['Case_AcquisitionInfo'].replace(['Neither'],'No Info-Unk')
    MasterDataFrame['Case_AcquisitionInfo'] = MasterDataFrame['Case_AcquisitionInfo'].replace(['OB'],'Outbreak')
    MasterDataFrame['Case_AcquisitionInfo'] = MasterDataFrame['Case_AcquisitionInfo'].replace(['TRAVEL','Travel-Related'],'Travel')
    MasterDataFrame['Case_AcquisitionInfo'].replace(
        ['Neither','Information pending','No Info-Missing','No Info-Unk','Missing Information','No Epi-link','No known epi link','Unspecified epi link','NO KNOWN EPI LINK','MISSING INFORMATION','UNSPECIFIED EPI LINK'],
        'Community',inplace=True)
    #MasterDataFrame['Case_AcquisitionInfo'].replace(['TRAVEL'],'Travel',inplace=True)
    print(round((time.time()-starttime),2),'- Case acquisition replacements done')


def PHUPopulation():
    # df = pd.DataFrame([['Toronto PHU',2731571],['Peel',1381744],['York',1109909],['Ottawa',934243],['Windsor',398953],
    #       ['Durham',645862],['Waterloo Region',535154],['Hamilton',536917],['Halton',548430],['Niagara',447888],
    #       ['Simcoe-Muskoka',540249],['London',455526],['Wellington-Guelph',284461],['Haldimand-Norfolk',109652],
    #       ['Leeds, Greenville, Lanark',169244],['Chatham-Kent',102042],['Lambton',126638],['Southwestern',88978+110862],
    #       ['Eastern Ontario',202762],['Haliburton, Kawartha',179083],['Brant',134943],['Grey Bruce',161977],['Huron Perth',59297+76796],
    #       ['Kingston',193363],['Peterborough',138236],['Thunder Bay',151884],['Sudbury',196448],
    #       ['Porcupine',84201],['Renfrew',103593],['Hastings',161180],['Northwestern',76455],
    #       ['North Bay',123820],['Algoma',113084],['Timiskaming',33049]
    #     ],
    #                   columns=['Reporting_PHU','Population'])

    df = pd.DataFrame([['Toronto PHU',141435 / 4532.7], ['Peel',48450/3016.4], ['York', 22431/1829.9],
                       ['Ottawa',11976/1135.5], ['Windsor',10685/2515.1], ['Durham',9560/1341.2],
                       ['Waterloo Region',7990/1367.3], ['Hamilton',7971/1346.1],
                       ['Halton',7085/1144.4], ['Niagara',6169/1305.6], ['Simcoe-Muskoka',4669/778.7],
                       ['London',4787/943.2], ['Wellington-Guelph',3434/1101.0],
                       ['Haldimand-Norfolk',1139/998.4], ['Leeds, Grenville, Lanark',754/435.4],
                       ['Chatham-Kent',974/916.1], ['Lambton',1527/1166.0],
                       ['Southwestern',1980/936.2], ['Eastern Ontario',2024/969.8],
                       ['Haliburton, Kawartha',698/369.4], ['Brant',1352/871.1],
                       ['Grey Bruce',590/347.3],['Huron Perth',984/704.1], ['Kingston',623/292.9],
                       ['Peterborough',461/311.5], ['Thunder Bay', 696/464.1], ['Sudbury',384/192.9],
                       ['Porcupine',167/200.1], ['Renfrew',290/267], ['Hastings',333/197.61],
                       ['Northwestern',228/260.1], ['North Bay',180/138.7],['Algoma',135/118],
                       ['Timiskaming',85/260]
                       ],
                      columns=['Reporting_PHU', 'Population'])
    df['Population'] = df['Population'] * 100000

    df.sort_values(by='Population', ascending=False, inplace=True)
    df.set_index('Reporting_PHU', inplace=True)
    return df


def OntarioPopulation():
    starttime = time.time()
    print('------------------------------------------------------------------------')
    print(f'OntarioPopulation \nStarted: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}')
    df = pd.read_excel('https://data.ontario.ca/dataset/f52a6457-fb37-4267-acde-11a1e57c4dc8/resource/31376797-1e4c-4426-ba75-0d93f4bb9f45/download/ministry_of_finance_population_projections_ontario_2020_t_2046.xlsx',
                       skiprows=4)
    df = df.drop(0)
    df = df.set_index(['SCENARIO', 'YEAR (JULY 1)', 'SEX'])
    df = df.iloc[:, 23:]
    # df.loc['REFERENCE',2021,'TOTAL'][0:5]
    df.to_pickle('Pickle/OntarioPopulation.pickle')

    New_row = {'0to4': df.loc['REFERENCE', 2021, 'TOTAL'][0:5].sum(),
               '05to11': df.loc['REFERENCE', 2021, 'TOTAL'][5:12].sum(),
               '12to17': df.loc['REFERENCE', 2021, 'TOTAL'][12:18].sum(),
               '18to29': df.loc['REFERENCE', 2021, 'TOTAL'][18:30].sum(),
               '30to39': df.loc['REFERENCE', 2021, 'TOTAL'][30:40].sum(),
               '40to49': df.loc['REFERENCE', 2021, 'TOTAL'][40:50].sum(),
               '50to59': df.loc['REFERENCE', 2021, 'TOTAL'][50:60].sum(),
               '60to69': df.loc['REFERENCE', 2021, 'TOTAL'][60:70].sum(),
               '70to79': df.loc['REFERENCE', 2021, 'TOTAL'][70:80].sum(),
               '80+ ': df.loc['REFERENCE', 2021, 'TOTAL'][80:].sum()
               }
    AgeGroupPopDF = pd.DataFrame([New_row], index=['Population']).T
    AgeGroupPopDF = AgeGroupPopDF.astype(int)
    AgeGroupPopDF.to_pickle('Pickle/AgeGroupPopDF.pickle')
    # AgeGroupDF = pd.read_pickle('Pickle/AgeGroupPopDF.pickle')

    print(f'Ended:   {datetime.datetime.now():%Y-%m-%d %H:%M:%S} {round(time.time() - starttime, 2)} seconds')
    print('------------------------------------------------------------------------')


def CanadaData():
    starttime = datetime.datetime.now()
    print('------------------------------------------------------------------------')
    print(f'CanadaData \nStarted: {starttime:%Y-%m-%d %H:%M:%S}')

    df = pd.read_csv('https://health-infobase.canada.ca/src/data/covidLive/covid19-download.csv')
    df.to_csv('SourceFiles/CanadaData.csv')
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df = df.sort_values(by='date', ascending=False, inplace=False).copy()
    TodaysDate = df['date'].max()

    df['prname'].replace(['Nunavut', 'Newfoundland and Labrador'],
                         ['Nunavut', 'Newfoundland'], inplace=True)
    df['Population'] = (df['numtotal_last14'].replace(',', '', regex=True))
    df = df.fillna(0)
    df['Population'] = (df['numtotal_last14'].astype(int)) / df['ratetotal_last14'] * 100000
    df = df.fillna(0)
    for column in ['numtests', 'Population']:
        if (df[column].dtype == 'O'):
            df[column] = df[column].str.replace(',', '').astype(int)

    df2 = pd.read_csv('https://health-infobase.canada.ca/src/data/covidLive/vaccination-administration.csv')
    df2.to_csv('SourceFiles/CanadaData-vaccineadmin.csv')
    df2['report_date'] = pd.to_datetime(df2['report_date'], format='%Y-%m-%d')
    df2 = df2.fillna(0)
    df2 = df2.sort_values(by='report_date', ascending=False)
    df2 = df2.set_index('prename')
    for prov in set(df2.index):
        df2.loc[prov, 'numdelta_all_administered'] = (df2.loc[prov]['numtotal_all_administered'].iloc[0]
                                                      - df2.loc[prov]['numtotal_all_administered'].iloc[1])
    df2 = df2.reset_index()
    df2['numdelta_all_administered'] = df2['numdelta_all_administered'].astype(int)
    df2 = df2[df2['report_date'] == df2['report_date'].max()]
    df2['prename'].replace(['Nunavut', 'Newfoundland and Labrador'],
                           ['Nunavut', 'Newfoundland'], inplace=True)
    # df2 = df2.set_index(['prename', 'report_date'])

    dfWeeklyCanadaVax = pd.read_csv('https://health-infobase.canada.ca/src/data/covidLive/vaccination-coverage-map.csv')
    dfWeeklyCanadaVax.to_csv('SourceFiles/CanadaData-vaccinecoverage.csv')
    dfWeeklyCanadaVax['week_end'] = pd.to_datetime(dfWeeklyCanadaVax['week_end'], format='%Y-%m-%d')
    dfWeeklyCanadaVax['prename'].replace(['Nunavut', 'Newfoundland and Labrador'],
                                         ['Nunavut', 'Newfoundland'], inplace=True)
    dfWeeklyCanadaVax['proptotal_fully'] = dfWeeklyCanadaVax['proptotal_fully'].replace('<0.01',
                                                                                        0.001)
    dfWeeklyCanadaVax['proptotal_fully'] = dfWeeklyCanadaVax['proptotal_fully'].astype(float)
    dfWeeklyCanadaVax.sort_values(by='week_end', ascending=False, inplace=True)

    xyz = pd.DataFrame(set(df.prname))
    xyz.columns = ['Province']
    xyz.set_index('Province', inplace=True)
    xyz.drop('Repatriated travellers', inplace=True)

    for province in (set(df2.prename).intersection(set(df.prname))):
        xyz.loc[province, 'Yesterday'] = (df.loc[df['prname'] == province].iloc[0].loc['numtoday'])
        xyz.loc[province, 'Last 7/100k'] = df.loc[df['prname'] == province].iloc[0].loc['ratetotal_last7']
        xyz.loc[province, 'Prev 7/100k'] = (df.loc[df['prname'] == province].iloc[0].loc['ratetotal_last14']
                                            - df.loc[df['prname'] == province].iloc[0].loc['ratetotal_last7'])
        xyz.loc[province, 'Last 7'] = df.loc[df['prname'] == province].iloc[0].loc['numtotal_last7'] / 7
        xyz.loc[province, 'Prev 7'] = df.loc[df['prname'] == province].iloc[7].loc['numtotal_last7'] / 7
        xyz.loc[province, 'Positive % - last 7'] = (df.loc[df['prname'] == province].iloc[0].loc['numtotal_last7']
                                                    / (df.loc[df['prname'] == province].iloc[0].loc['numtests'] - df.loc[df['prname'] == province].iloc[7].loc['numtests']) * 100)
        xyz.loc[province, 'Population'] = df.loc[df['prname'] == province].iloc[0].loc['Population'].astype(int)
        xyz.loc[province, 'Vax (day)'] = df2.loc[df2['prename'] == province].iloc[0].loc['numdelta_all_administered']
        xyz.loc[province, 'To date (per 100)'] = (df2.loc[df2['prename'] == province].iloc[0].loc['numtotal_all_administered']
                                                  / xyz.loc[province, 'Population'] * 100)
        # xyz['Yesterday'] = xyz['Yesterday'].fillna(0).astype(int).map('{:,.0f}'.format)
        xyz.loc[province, 'Yesterday'] = "{:,.0f}".format(xyz.loc[province, 'Yesterday'])
        xyz.loc[province, '% with 1+'] = dfWeeklyCanadaVax.loc[dfWeeklyCanadaVax['prename'] == province].iloc[0].loc['proptotal_atleast1dose']
        xyz.loc[province, '% with both'] = round(dfWeeklyCanadaVax.loc[dfWeeklyCanadaVax['prename'] == province].iloc[0].loc['proptotal_fully'], 1)

        if (df.loc[df['prname'] == province].iloc[0].loc['update'] == 0) & (province != 'Canada'):
            xyz.loc[province, 'Yesterday'] = 'N/R'

    xyz.sort_values(by='Last 7', ascending=False, inplace=True)
    xyz = xyz.drop(['Repatriated travellers'], errors='ignore')
    xyz.insert(1, 'Averages->>', "")
    xyz.insert(1, 'Per 100k->>', "")
    xyz.insert(1, 'Vaccines->>', "")
    xyz.insert(1, 'Weekly vax update->>', "")

    xyz = xyz[['Yesterday', 'Averages->>', 'Last 7', 'Prev 7', 'Per 100k->>', 'Last 7/100k',
               'Prev 7/100k', 'Positive % - last 7', 'Vaccines->>', 'Vax (day)', 'To date (per 100)',
               'Weekly vax update->>', '% with 1+', '% with both']].copy()
    for column in ['Last 7', 'Prev 7', 'Last 7/100k', 'Prev 7/100k']:
        xyz[column] = xyz[column].round(1).astype(str)
    # xyz['Yesterday'] = xyz['Yesterday'].astype(int)
    # xyz['Yesterday'] = xyz['Yesterday'].map('{:,.0f}'.format)

    xyz['Positive % - last 7'] = xyz['Positive % - last 7'].map('{:.1f}'.format)

    xyz['To date (per 100)'] = xyz['To date (per 100)'].map('{:.1f}'.format)
    xyz['Vax (day)'] = xyz['Vax (day)'].map('{:,.0f}'.format)

    xyzT = xyz.T
    xyzT.insert(1, ' ', "")
    xyz = (xyzT.T).copy()

    with open('TextOutput/CanadaData.txt', 'w', newline='') as f:
        f.write('')
        f.write(f'**Canada comparison** - [Source](https://www.canada.ca/en/public-health/services/diseases/coronavirus-disease-covid-19/epidemiological-economic-research-data.html) - data as of {TodaysDate:%B %d} ')
        f.write('\n\n')
        f.write('Province|Yesterday|Averages->>|Last 7|Prev 7|Per 100k->>|Last 7/100k|Prev 7/100k|Positive % - last 7|Vaccines->>|Vax(day)|To date (per 100)|Weekly vax update->>|% with 1+|% with both|\n')
        f.write(':--|--:|:--|--:|--:|:--|--:|--:|--:|:-:|:-:|:-:|--:|--:|--:|\n')
        f.write('')
        xyz.to_csv(f, header=False, sep='|')
        f.write('\n')

    abc = pd.pivot_table(df, values='ratetotal_last7', index='prname', columns='date',
                         aggfunc=np.sum, fill_value=0)
    d = abc.T.copy()
    d = d.sort_values(by='date', ascending=False)
    d.to_pickle('Pickle/dfCanadaData.pickle')

    endTime = datetime.datetime.now()
    TimeTaken = (endTime - starttime).total_seconds()
    print(f'Ended:   {endTime:%Y-%m-%d %H:%M:%S} {TimeTaken:.2f} seconds')
    print('------------------------------------------------------------------------')

    # #Cases by Day of week/month
    # pivotabc = pd.pivot_table(df,values = 'Day new cases',index = [df['Reported Date'].dt.month], columns = [df['Reported Date'].dt.weekday] , aggfunc=np.mean)
    # pivotabc = pivotabc.fillna(0)
    # pivotabc = pivotabc.round(1)
    # DaysDict = {0:'Mon',1:'Tue',2:'Wed',3:'Thu',4:'Fri',5:'Sat',6:'Sun'}
    # NewColumnList = []
    # for i in pivotabc.columns:
    #     NewColumnList.append(DaysDict[pivotabc.columns[i]])
    # pivotabc.columns = NewColumnList
    # pivotabc.index.name = 'Month'
    # pivotabc = pivotabc.sort_index(ascending = False)
    # pivotabc.T.to_csv('aaa.csv')


def GlobalData():
    starttime = datetime.datetime.now()
    print('------------------------------------------------------------------------')
    print(f'GlobalData \nStarted: {starttime:%Y-%m-%d %H:%M:%S}')

    filename = 'FilesToUpload/GlobalData.csv'
    ConsoleOut = sys.stdout
    locationSet_G20 = ['Argentina', 'Australia', 'Brazil', 'Canada', 'China', 'France',
                       'Germany', 'India', 'Indonesia', 'Italy', 'Japan', 'South Korea',
                       'Mexico', 'Russia', 'Saudi Arabia', 'South Africa', 'Turkey',
                       'United Kingdom', 'United States', 'European Union', 'Israel',
                       'Nigeria', 'Pakistan', 'Bangladesh', 'Vietnam', 'Sweden',
                       'Spain', 'Italy', 'Iran', 'Phillipines', 'Ethiopia',
                       'Chile', 'Egypt']

    # New case data

    df = pd.read_csv('https://github.com/owid/covid-19-data/blob/master/public/data/owid-covid-data.csv?raw=true')
    df.to_csv('SourceFiles/GlobalData.csv')
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    df = df[df['date'] > df['date'].max() - datetime.timedelta(days=7)]
    df = df.sort_values(by='date', ascending=False)
    # df.fillna(method = "backfill")
    df['Last 7 per 100k'] = df['new_cases_smoothed_per_million'] * 7 / 10
    df['Weekly tests per 100k'] = df['new_tests_smoothed_per_thousand'] * 7 * 100

    # df['positive_rate'] = (df['new_cases_smoothed']/df['new_tests_smoothed']).round(6).fillna('n/a')
    df.set_index(['location'], inplace=True)

    df = df.drop(['Isle of Man', 'Gibraltar'], errors='ignore')
    isoSet = sorted(set(df.index))
    dfnew = pd.DataFrame()
    for x in isoSet:
        if type(df.loc[x].fillna(method='backfill')) == pd.DataFrame:
            dfnew = dfnew.append(pd.DataFrame(df.loc[x].fillna(method='backfill').iloc[0]).T)
        else:
            dfnew = dfnew.append(pd.DataFrame(df.loc[x]).T)
    # dfnew = dfnew.fillna('N/A')
    dfnew = dfnew.sort_values(by='new_cases_smoothed', ascending=False)
    dfnew['new_vaccinations_smoothed_per_hundred'] = dfnew['new_vaccinations_smoothed_per_million'] / 10000
    dfnew['total_vaccinations_per_hundred_Max2Dose'] = (dfnew['people_vaccinated_per_hundred']
                                                        + dfnew['people_fully_vaccinated_per_hundred'])
    # dfnew['total_vaccinations_per_hundred_Max2Dose'] = dfnew['total_vaccinations_per_hundred_Max2Dose'].fillna(dfnew['total_vaccinations_per_hundred'])
    dfnew = dfnew[dfnew['population'] > 1000000]

    dfnew = dfnew[['new_cases_smoothed', 'Last 7 per 100k', 'positive_rate', 'Weekly tests per 100k',
                   'total_vaccinations_per_hundred_Max2Dose', 'people_vaccinated_per_hundred',
                   'icu_patients_per_million', 'new_vaccinations_smoothed_per_hundred',
                   'total_boosters_per_hundred', 'people_fully_vaccinated_per_hundred']]

    dfnew_G20 = dfnew.loc[dfnew.index.isin(locationSet_G20)]
    dfnew_G20.to_pickle('Pickle/G20Data.pickle')

    sys.stdout = open('TextOutput/GlobalData.txt', 'w')

    # ###################################################################################
    # ###################################################################################
    # # This section calculates the Global vaccine counts for countries (first dose+ second dose %)
    # print("**Global Vaccine Comparison:** - doses administered per 100 people (% with at least 1 dose / both doses), to date (ignoring 3rd doses)  - Full list on Tab 6 - [Source](https://ourworldindata.org/covid-vaccinations)")

    # df_temp = dfnew_G20[dfnew_G20['total_vaccinations_per_hundred_Max2Dose'].notna()]
    # df_temp = df_temp.sort_values(by='total_vaccinations_per_hundred_Max2Dose', ascending=False)
    # df_temp['people_vaccinated_per_hundred'] = df_temp['people_vaccinated_per_hundred'].astype(float).round(decimals=1)
    # df_temp['people_fully_vaccinated_per_hundred'] = df_temp['people_fully_vaccinated_per_hundred'].astype(float).round(decimals=1)

    # df_temp = df_temp.fillna('?')

    # for i in range(len(df_temp)):
    #     if (i % 4 == 0):
    #         print()
    #         print('* ', end='')

    #     if not np.isnan(df_temp.iloc[i]['total_vaccinations_per_hundred_Max2Dose']):
    #         print(df_temp.index[i], ': ', round(df_temp.iloc[i]['total_vaccinations_per_hundred_Max2Dose'], 1),
    #               ' (', df_temp.iloc[i]['people_vaccinated_per_hundred'], '/',
    #               df_temp.iloc[i]['people_fully_vaccinated_per_hundred'], '),  ', sep='', end='')
    ###################################################################################
    ###################################################################################
    # This section calculates the Global vaccine counts for countries (first dose+ second dose %)
    print("**Global Vaccine Comparison:** - % fully vaxxed  - Full list on Tab 6 - [Source](https://ourworldindata.org/covid-vaccinations)")

    df_temp = dfnew_G20[dfnew_G20['people_fully_vaccinated_per_hundred'].notna()]
    df_temp = df_temp.sort_values(by='people_fully_vaccinated_per_hundred', ascending=False)
    df_temp['people_fully_vaccinated_per_hundred'] = df_temp['people_fully_vaccinated_per_hundred'].astype(float).round(decimals=1)

    df_temp = df_temp.fillna('?')

    for i in range(len(df_temp)):
        if (i % 4 == 0):
            print()
            print('* ', end='')

        if not np.isnan(df_temp.iloc[i]['people_fully_vaccinated_per_hundred']):
            # print(df_temp.index[i], ': ', round(df_temp.iloc[i]['people_fully_vaccinated_per_hundred'], 1),
            #       sep='', end='')
            print(f"{df_temp.index[i]}: {df_temp.iloc[i]['people_fully_vaccinated_per_hundred']:.1f}, ",
                  sep='', end='')
    print()
    print('* Map charts showing rates of [at least one dose](https://docs.google.com/spreadsheets/d/e/2PACX-1vQ7fegCALd11ElozUYcMi-e9Dj69YaiNQhvEpk81JHsyTACl0UXkWK5zfMNFe49Tq3VuN9Av-fuEZqV/pubchart?oid=1188693775&format=interactive) and [total doses per 100 people](https://docs.google.com/spreadsheets/d/e/2PACX-1vQ7fegCALd11ElozUYcMi-e9Dj69YaiNQhvEpk81JHsyTACl0UXkWK5zfMNFe49Tq3VuN9Av-fuEZqV/pubchart?oid=760938472&format=interactive)')
    print()
    print()

    # ###################################################################################
    # ###################################################################################
    # # This section calculates the vaccine pace for each country
    # print("**Global Vaccine Pace Comparison - doses per 100 people in the last week:** - [Source](https://ourworldindata.org/grapher/daily-covid-vaccination-doses-per-capita?tab=chart)")
    # dfnew_G20_temp = dfnew_G20[dfnew_G20['new_vaccinations_smoothed_per_hundred'].notna()]
    # dfnew_G20_temp = dfnew_G20_temp.sort_values(by='new_vaccinations_smoothed_per_hundred',
    #                                             ascending=False)
    # for i in range(len(dfnew_G20_temp)):
    #     if (i % 5 == 0):
    #         print()
    #         print('* ',end = '')

    #     if not np.isnan(dfnew_G20_temp.iloc[i]['new_vaccinations_smoothed_per_hundred']):
    #         print(dfnew_G20_temp.index[i], ': ',
    #               round(dfnew_G20_temp.iloc[i]['new_vaccinations_smoothed_per_hundred'] * 7, 2),
    #               '  ',sep = '',end='')
    # ###################################################################################
    # ###################################################################################

    ###################################################################################
    ###################################################################################
    # This section calculates the number of boosterse for each country
    print("**Global Boosters (% fully vaxxed), doses per 100 people  to date:**")
    dfnew_G20_temp = dfnew_G20[dfnew_G20['total_boosters_per_hundred'].notna()]
    dfnew_G20_temp = dfnew_G20_temp.sort_values(by='total_boosters_per_hundred',
                                                ascending=False)
    for i in range(len(dfnew_G20_temp)):
        if (i % 5 == 0):
            print()
            print('* ', end='')

        if not np.isnan(dfnew_G20_temp.iloc[i]['total_boosters_per_hundred']):
            print(f"{dfnew_G20_temp.index[i]}: {dfnew_G20_temp.iloc[i]['total_boosters_per_hundred']:.1f}",
                  f"({dfnew_G20_temp.iloc[i]['people_fully_vaccinated_per_hundred']:.1f}) ",
                  end='')

    ###############################################################################################
    ###############################################################################################
    print()
    print()
    print("**Global Case Comparison:** - Major Countries - Cases per 100k in the last week (% with at least one dose)  - Full list - tab 6 [Source](https://ourworldindata.org/explorers/coronavirus-data-explorer?zoomToSelection=true&time=40..latest)")
    print()

    # locationSet = [['Canada','United States','Mexico'],['Germany','Italy','France','Spain'],['United Kingdom','Israel','Sweden','Russia'],['Vietnam','South Korea','Australia','New Zealand'],['Dominican Republic','Monaco','Cuba','Jamaica']]
    # for x in locationSet:
    #     print('* ',end = '')
    #     for y in x:
    #         if  np.isnan(dfnew.loc[y]['Weekly tests per 100k']):
    #             print(y,': ',round(dfnew.loc[y]['Last 7 per 100k'],2),'   ',sep = '',end='')
    #         else:
    #             print(y,': ',round(dfnew.loc[y]['Last 7 per 100k'],2)," (","{:,.0f}".format(dfnew.loc[y]['Weekly tests per 100k']),')  ',sep = '',end='')
    #     print()
    # print()
    dfnew_G20_temp = dfnew_G20[dfnew_G20['Last 7 per 100k'].notna()]
    dfnew_G20_temp = dfnew_G20_temp.sort_values(by='Last 7 per 100k', ascending=False)
    dfnew_G20_temp = dfnew_G20_temp.fillna('n/a')

    for i in range(len(dfnew_G20_temp)):
        if (i % 4 == 0):
            print()
            print('* ', end='')

        if  not np.isnan(dfnew_G20_temp.iloc[i]['Last 7 per 100k']):
            print(dfnew_G20_temp.index[i],': ', round(dfnew_G20_temp.iloc[i]['Last 7 per 100k'], 1),
                  ' (', (dfnew_G20_temp.iloc[i]['people_vaccinated_per_hundred']), ')', '  ', sep='', end='')
    print()
    print()
    ###############################################################################################
    ###############################################################################################
    # # Top 16 countries globally table
    # print("**Global Case Comparison:** Top 16 countries by Cases per 100k in the last week (% with at least one dose)  - Full list - tab 6 [Source](https://ourworldindata.org/explorers/coronavirus-data-explorer?zoomToSelection=true&time=40..latest)")
    # dfnew = dfnew.sort_values(by='Last 7 per 100k', ascending = False)
    # dfnew = dfnew.fillna('n/a')
    # for i in range(16):
    #     if (i % 4 == 0):
    #         print()
    #         print('* ',end = '')

    #     if  not np.isnan(dfnew.iloc[i]['Last 7 per 100k']):
    #         print(dfnew.index[i],': ',round(dfnew.iloc[i]['Last 7 per 100k'],1),' (',(dfnew.iloc[i]['people_vaccinated_per_hundred']),')','  ',sep = '',end='')
    # print()
    # print()
    ###############################################################################################
    ###############################################################################################

    # print("**G20 Vaccine Comparison - doses per 100k in the last week:** - [Source](https://ourworldindata.org/grapher/daily-covid-vaccination-doses-per-capita?tab=chart)")

    # dfnew_G20 = dfnew_G20.sort_values(by = 'icu_patients_per_million',ascending = False)

    # i = 0
    # for i in range(len(locationSet_G20)):
    #     if (i % 5 == 0):
    #         print()
    #         print('* ',end = '')

    #     if  not np.isnan(dfnew_G20.iloc[i]['icu_patients_per_million']):
    #         print(dfnew_G20.index[i],': ',round(dfnew_G20.iloc[i]['icu_patients_per_million'],2),', ',sep = '',end='')
    # print()


    print("**Global ICU Comparison:** - Current, adjusted to Ontario's population - [Source](https://ourworldindata.org/grapher/covid-icu-patients-per-million)")
    dfnew_G20_temp = dfnew_G20[dfnew_G20['icu_patients_per_million'].notna()]
    dfnew_G20_temp = dfnew_G20_temp.sort_values(by = 'icu_patients_per_million',ascending = False)

    for i in range(len(dfnew_G20_temp)):
        if (i % 5 == 0):
            print()
            print('* ',end = '')

        if  not np.isnan(dfnew_G20_temp.iloc[i]['icu_patients_per_million']):
            #print(dfnew_G20_temp.index[i],': ',round(dfnew_G20_temp.iloc[i]['icu_patients_per_million'],2),', ',sep = '',end='')
            print(dfnew_G20_temp.index[i],': ',"{:,.0f}".format(dfnew_G20_temp.iloc[i]['icu_patients_per_million']*14.936396),', ',sep = '',end='')
            #print(df.loc[dfnew_G20_temp.index[i]].iloc[0]['population'])
    print()
    print()

    sys.stdout = ConsoleOut

    # df2 = pd.read_csv('https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/testing/covid-testing-all-observations.csv')
    # df2['Date'] = pd.to_datetime(df2['Date'],format = '%Y-%m-%d')

    # df2['Positive_Rate']=((1/df2['Short-term tests per case'])).round(4).fillna(0)
    # df2['Weekly tests per 100k'] = (df2['7-day smoothed daily change per thousand']    *7*100).fillna(0).round(1)
    # df2 = df2.replace([np.inf, -np.inf], 9999)

    # df2.set_index(['ISO code'],inplace = True)
    # df2.sort_values(by = ['Date','Positive_Rate'],ascending = [False,False],inplace = True)
    # countrySet = sorted(set(df2.index))
    # df2new = pd.DataFrame()
    # for x in countrySet:
    #     df2new = df2new.append(pd.DataFrame(df2.loc[x].iloc[0]).T)
    # df2 = df2new.copy()
    # df2 = df2[df2['Date']>pd.Timestamp(datetime.datetime.now()-datetime.timedelta(days=7))]

    # # abc = pd.merge(dfnew,df2,left_index=True,right_index = True, how = 'outer')
    # # abc.sort_values(by = "new_cases_smoothed", ascending = False, inplace = True)
    # # abc = abc.fillna('n/a')
    # # abc = abc.replace(0,'n/a')

    # # abcToPrint = abc[['location','new_cases_smoothed','Last 7 per 100k','Positive_Rate', 'Weekly tests per 100k']].T

    ##################################################################################
    # #US State Data  - old
    # #http://www2.census.gov/programs-surveys/popest/datasets/2010-2019/national/totals/nst-est2019-alldata.csv
    # USPopDF = pd.read_csv('http://www2.census.gov/programs-surveys/popest/datasets/2010-2019/national/totals/nst-est2019-alldata.csv')
    # USPopDF.rename(columns = {'NAME':'State Name'},inplace = True)
    # USPopDF = USPopDF[['State Name','POPESTIMATE2019']]

    # USdf = pd.read_csv('https://api.covidtracking.com/v1/states/daily.csv')

    # USOutputDF = pd.DataFrame(set(USdf['state']))
    # USOutputDF.columns = ['state']
    # USOutputDF.set_index('state',inplace = True)
    # for states in set(USdf['state']):
    #     USOutputDF.loc[states,'State Name'] = stateabbrev.abbrev_us_state[states]
    #     USOutputDF.loc[states,'Yesterday'] = USdf[USdf['state']==states].iloc[0]['positive'] - USdf[USdf['state']==states].iloc[1]['positive']
    #     USOutputDF.loc[states,'Last 7 ave.'] = ((USdf[USdf['state']==states].iloc[0]['positive'] - USdf[USdf['state']==states].iloc[7]['positive'])/7).round(1)
    #     USOutputDF.loc[states,'Prev 7 ave.'] = ((USdf[USdf['state']==states].iloc[7]['positive'] - USdf[USdf['state']==states].iloc[14]['positive'])/7).round(1)
    #     USOutputDF.loc[states,'Tests/day (last 7)'] = ((USdf[USdf['state']==states].iloc[0]['totalTestResults'] - USdf[USdf['state']==states].iloc[7]['totalTestResults'])/7).round(1)
    #     USOutputDF.loc[states,'Tests (last 7 days)'] = ((USdf[USdf['state']==states].iloc[0]['totalTestResults'] - USdf[USdf['state']==states].iloc[7]['totalTestResults'])).round(1)
    #     USOutputDF.loc[states,'Last 7 total'] = (USdf[USdf['state']==states].iloc[0]['positive'] - USdf[USdf['state']==states].iloc[7]['positive'])
    # USOutputDF['Positive % - last 7'] = (USOutputDF['Last 7 ave.']/USOutputDF['Tests/day (last 7)']*1).round(4)
    # USOutputDF.set_index('State Name',inplace = True)
    # USOutputDF.sort_values('Last 7 ave.',ascending = False, inplace = True)
    # USOutputDF = pd.merge(USOutputDF,USPopDF,how = 'left',on ='State Name')
    # USOutputDF['Cases - Last 7 per 100k'] = (USOutputDF['Last 7 total']/USOutputDF['POPESTIMATE2019']*100000).round(1)
    # USOutputDF['Tests - Last 7 per 100k'] = (USOutputDF['Tests (last 7 days)']/USOutputDF['POPESTIMATE2019']*100000).round(1)
    # USOutputDF = USOutputDF[(['State Name', 'Yesterday', 'Last 7 ave.', 'Prev 7 ave.', 'Cases - Last 7 per 100k',
    # 'Positive % - last 7','Tests - Last 7 per 100k'])]
    # USOutputDF = USOutputDF.fillna('n/a')
    # #USOutputDF = USOutputDF.T

    ##################################################################################
    ##################################################################################
    # US State Data  - new

    # USPopDF = pd.read_csv('http://www2.census.gov/programs-surveys/popest/datasets/2010-2019/national/totals/nst-est2019-alldata.csv')
    # USPopDF.rename(columns = {'NAME':'State Name'},inplace = True)
    # USPopDF = USPopDF[['State Name','POPESTIMATE2019']]
    # USPopDF.to_pickle('Pickle/USPopDF.pickle')

    USPopDF = pd.read_pickle('Pickle/USPopDF.pickle')
    USdf = pd.read_csv('https://github.com/nytimes/covid-19-data/blob/master/us-states.csv?raw=true')
    USdf.to_csv('SourceFiles/GlobalData-USData.csv')
    USdf = USdf.sort_values(by='date', ascending=False)
    USOutputDF = pd.DataFrame(set(USdf['state']))
    USOutputDF.columns = ['state']
    USOutputDF.set_index('state', inplace=True)
    USOutputDF = pd.DataFrame(set(USdf['state']))
    USOutputDF.columns = ['state']
    USOutputDF.set_index('state', inplace=True)

    for states in set(USdf['state']):
        try:
            # USOutputDF.loc[states,'State Name'] = stateabbrev.abbrev_us_state[states]
            USOutputDF.loc[states, 'Yesterday'] = (USdf[USdf['state'] == states].iloc[0]['cases']
                                                   - USdf[USdf['state'] == states].iloc[1]['cases'])
            USOutputDF.loc[states, 'Last 7 ave.'] = ((USdf[USdf['state'] == states].iloc[0]['cases']
                                                      - USdf[USdf['state'] == states].iloc[7]['cases']) / 7).round(1)
            USOutputDF.loc[states, 'Prev 7 ave.'] = ((USdf[USdf['state'] == states].iloc[7]['cases']
                                                      - USdf[USdf['state'] == states].iloc[14]['cases']) / 7).round(1)
            USOutputDF.loc[states, 'Tests/day (last 7)'] = 0
            USOutputDF.loc[states, 'Tests (last 7 days)'] = 0
            USOutputDF.loc[states, 'Last 7 total'] = (USdf[USdf['state'] == states].iloc[0]['cases']
                                                      - USdf[USdf['state'] == states].iloc[7]['cases'])
        except:
            USOutputDF = USOutputDF.drop(states, axis='index')

    USVaccineDF = pd.read_csv('https://github.com/owid/covid-19-data/blob/master/public/data/vaccinations/us_state_vaccinations.csv?raw=true')
    USVaccineDF.to_csv('SourceFiles/USVaccine.csv')
    USVaccineDF['date'] = pd.to_datetime(USVaccineDF['date'], format='%Y-%m-%d')

    USVaccineDF = USVaccineDF.sort_values(by=['location', 'date'], ascending=[True, False])
    USVaccineDF = USVaccineDF.fillna(method='bfill')
    USVaccineDF['people_vaccinated_per_hundred_week'] = (USVaccineDF['people_vaccinated_per_hundred']
                                                         - USVaccineDF['people_vaccinated_per_hundred'].shift(-7))
    USVaccineDF['people_fully_vaccinated_per_hundred_week'] = (USVaccineDF['people_fully_vaccinated_per_hundred']
                                                               - USVaccineDF['people_fully_vaccinated_per_hundred'].shift(-7))

    USVaccineDF = USVaccineDF[USVaccineDF['date'] == max(USVaccineDF['date'])]
    USVaccineDF = USVaccineDF.sort_values(by='people_vaccinated_per_hundred', ascending=False)

    USOutputDF.sort_values(by='Last 7 total', ascending=False)

    USOutputDF['Positive % - last 7'] = (USOutputDF['Last 7 ave.']
                                         / USOutputDF['Tests/day (last 7)'] * 1).round(4)
    # USOutputDF.set_index('State Name',inplace = True)
    USOutputDF.sort_values('Last 7 ave.', ascending=False, inplace=True)
    USOutputDF = pd.merge(USOutputDF, USPopDF, left_index=True, right_on='State Name')
    USOutputDF['Cases - Last 7 per 100k'] = (USOutputDF['Last 7 total']
                                             / USOutputDF['POPESTIMATE2019'] * 100000).round(1)
    USOutputDF['Tests - Last 7 per 100k'] = (USOutputDF['Tests (last 7 days)']
                                             / USOutputDF['POPESTIMATE2019'] * 100000).round(1)
    USOutputDF = USOutputDF[(['State Name', 'Yesterday', 'Last 7 ave.', 'Prev 7 ave.', 'Cases - Last 7 per 100k'])]

    # merge_cols_to_use = USVaccineDF.columns.difference(USOutputDF.columns)
    USVaccineDF.rename(columns={'location': 'State Name'}, inplace=True)
    USVaccineDF = USVaccineDF.drop(columns='date')
    USVaccineDF = USVaccineDF.replace('New York State', 'New York')

    USOutputDF = pd.merge(USOutputDF, USVaccineDF, how='inner', left_on='State Name',
                          right_on='State Name')
    USOutputDF = USOutputDF.fillna('n/a')

    USOutputDF = USOutputDF.sort_values(by='Last 7 ave.', ascending=False)

    sys.stdout = open('TextOutput/GlobalData.txt', 'a')
    print()
    print('**US State comparison - case count** - Top 25 by last 7 ave. case count (Last 7/100k) - [Source](https://www.nytimes.com/interactive/2020/us/coronavirus-us-cases.html)')

    for i in range(25):
        if (i % 5 == 0):
            print()
            print('* ', end='')
        print('*', stateabbrev.us_state_abbrev[USOutputDF.iloc[i]['State Name']], ':* ',
              "{:,.0f}".format(USOutputDF.iloc[i]['Last 7 ave.']),
              ' (', "{:,.1f}".format(USOutputDF.iloc[i]['Cases - Last 7 per 100k']), ')',
              sep='', end=', ')
    print()
    print()
    USOutputDF = USOutputDF.sort_values(by='people_fully_vaccinated_per_hundred', ascending=False)

    # print('**US State comparison - vaccines count** - % single (fully) dosed - [Source](https://ourworldindata.org/us-states-vaccinations#what-share-of-the-population-has-received-at-least-one-dose-of-the-covid-19-vaccine)')
    # #print()
    # #print('* ',end = '')
    # for i in range(len(USOutputDF)):
    #     if (i%5 == 0):
    #         print()
    #         print('* ', end = '')
    #     print('*',stateabbrev.us_state_abbrev[USOutputDF.iloc[i]['State Name']],':* ',"{:,.1%}".format(USOutputDF.iloc[i]['people_vaccinated_per_hundred']/100),' (',"{:.1%}".format(USOutputDF.iloc[i]['people_fully_vaccinated_per_hundred']/100),')',sep='', end = ', ')
    # print()
    USOutputDF = USOutputDF.replace({"n/a": 0})
    USOutputDF['people_fully_vaccinated_per_hundred'] = USOutputDF['people_fully_vaccinated_per_hundred'].astype(float)
    USOutputDF['people_vaccinated_per_hundred_week'] = USOutputDF['people_vaccinated_per_hundred_week'].astype(float)

    print('**US State comparison - vaccines count** - % 2+ dosed (change in week) - [Source](https://ourworldindata.org/us-states-vaccinations#what-share-of-the-population-has-received-at-least-one-dose-of-the-covid-19-vaccine)')

    for i in range(len(USOutputDF)):
        if (i % 5 == 0):
            print()
            print('* ', end='')
        print('*',stateabbrev.us_state_abbrev[USOutputDF.iloc[i]['State Name']], ':* ',
              "{:,.1%}".format(USOutputDF.iloc[i]['people_fully_vaccinated_per_hundred'] / 100),
              ' (', "{:.1%}".format(USOutputDF.iloc[i]['people_fully_vaccinated_per_hundred_week']/100), ')',
              sep='', end=', ')
    print()

    sys.stdout = ConsoleOut

    ###############################################################################################
    # UK Data
    UKData()

    UKCaseDF = pd.read_csv('https://coronavirus.data.gov.uk/api/v1/data?filters=areaType=overview&structure=%7B%22areaType%22:%22areaType%22,%22areaName%22:%22areaName%22,%22areaCode%22:%22areaCode%22,%22date%22:%22date%22,%22newCasesByPublishDate%22:%22newCasesByPublishDate%22,%22cumCasesByPublishDate%22:%22cumCasesByPublishDate%22%7D&format=csv')
    UKCaseDF.to_csv('SourceFiles/GlobalData-UKCaseDF.csv')
    UKCaseDF['date'] = pd.to_datetime(UKCaseDF['date'], format='%Y-%m-%d')
    UKCaseDF.sort_values(by='date', ascending=True, inplace=True)
    UKCaseDF['7-day average'] = UKCaseDF['newCasesByPublishDate'].rolling(7).mean().round(0).fillna(0)
    UKPeakCase = (UKCaseDF['7-day average'].max())
    UKCaseDF['7-day average'] = UKCaseDF['7-day average'].map('{:,.0f}'.format)
    UKCaseDF.sort_values(by='date',ascending = False, inplace=True)

    UKVentDF = pd.read_csv('https://coronavirus.data.gov.uk/api/v1/data?filters=areaType=overview&structure=%7B%22areaType%22:%22areaType%22,%22areaName%22:%22areaName%22,%22areaCode%22:%22areaCode%22,%22date%22:%22date%22,%22covidOccupiedMVBeds%22:%22covidOccupiedMVBeds%22%7D&format=csv')
    UKVentDF.to_csv('SourceFiles/GlobalData-UKVentDF.csv')
    UKVentDF['date'] = pd.to_datetime(UKVentDF['date'], format='%Y-%m-%d')
    UKPeakVent = UKVentDF['covidOccupiedMVBeds'].max()

    #UKVentDF['covidOccupiedMVBeds'] = UKVentDF['covidOccupiedMVBeds'].map('{:,.0f}'.format)
    UKVentDF['covidOccupiedMVBeds'] = UKVentDF['covidOccupiedMVBeds'].map('{:,.0f}'.format)

    UKHospDF = pd.read_csv('https://coronavirus.data.gov.uk/api/v1/data?filters=areaType=overview&structure=%7B%22areaType%22:%22areaType%22,%22areaName%22:%22areaName%22,%22areaCode%22:%22areaCode%22,%22date%22:%22date%22,%22hospitalCases%22:%22hospitalCases%22%7D&format=csv')
    UKHospDF.to_csv('SourceFiles/UGlobalData-UKHosp.csv')
    UKHospDF['date'] = pd.to_datetime(UKHospDF['date'],format = '%Y-%m-%d')
    UKPeakHosp = UKHospDF['hospitalCases'].max()
    UKHospDF['hospitalCases'] = UKHospDF['hospitalCases'].map('{:,.0f}'.format)


    EngCaseDF = pd.read_pickle('Pickle/EngCaseAvgPer100k.pickle')

    sys.stdout = open('TextOutput/GlobalData.txt','a')

    print()
    print('**UK Watch** - [Source](https://coronavirus.data.gov.uk/)')
    print()
    print("The England age group data below is actually lagged by four days, i.e. the , the 'Today' data is actually '4 day ago' data.")
    print()
    print('Metric|Today|7d ago |14d ago|21d ago |30d ago|Peak')
    print(':--|:-:|:-:|:-:|:-:|:-:|:-:|')
    print('**Cases - 7-day avg**|',UKCaseDF['7-day average'].iloc[0],'|',UKCaseDF['7-day average'].iloc[7],'|',UKCaseDF['7-day average'].iloc[14],'|',UKCaseDF['7-day average'].iloc[21],'|',UKCaseDF['7-day average'].iloc[30],'|','{:,.0f}'.format(UKPeakCase))

    print('**Hosp. - current**|',UKHospDF['hospitalCases'].iloc[0],'|',UKHospDF['hospitalCases'].iloc[7],'|',UKHospDF['hospitalCases'].iloc[14],'|',UKHospDF['hospitalCases'].iloc[21],'|',UKHospDF['hospitalCases'].iloc[30],'|','{:,.0f}'.format(UKPeakHosp))

    print('**Vent. - current**|',UKVentDF['covidOccupiedMVBeds'].iloc[0],'|',UKVentDF['covidOccupiedMVBeds'].iloc[7],'|',UKVentDF['covidOccupiedMVBeds'].iloc[14],'|',UKVentDF['covidOccupiedMVBeds'].iloc[21],'|',UKVentDF['covidOccupiedMVBeds'].iloc[30],'|','{:,.0f}'.format(UKPeakVent))
    print(' **England weekly cases/100k by age:** | |  | | | | |')

    print('<60|',EngCaseDF.iloc[0]['00_59'],'|',EngCaseDF.iloc[7]['00_59'],'|',EngCaseDF.iloc[14]['00_59'],'|',EngCaseDF.iloc[21]['00_59'],'|',EngCaseDF.iloc[30]['00_59'],'|',EngCaseDF['00_59'].max())

    print('60+|',EngCaseDF.iloc[0]['60+'],'|',EngCaseDF.iloc[7]['60+'],'|',EngCaseDF.iloc[14]['60+'],'|',EngCaseDF.iloc[21]['60+'],'|',EngCaseDF.iloc[30]['60+'],'|',EngCaseDF['60+'].max())

    sys.stdout = ConsoleOut

    ##################################################################################

    # xxx = abc.T.copy()
    # xxx = xxx.set_index('location')

    with open(filename, 'w',newline='') as f:
        f.write('Global Country Data\n')
        dfnew = dfnew.replace({'n/a':0})
        dfnew.T.to_csv(f,header = True)
        f.write('\n')

    with open(filename, 'a',newline='') as f:
        f.write('US State Data\n')
        USOutputDF = USOutputDF.replace({'n/a':0})
        USOutputDF.T.to_csv(f,header = False)
        f.write('\n')

    endTime = datetime.datetime.now()
    TimeTaken = (endTime - starttime).total_seconds()
    print(f'Ended:   {endTime:%Y-%m-%d %H:%M:%S} {TimeTaken:.2f} seconds')
    print('------------------------------------------------------------------------')


def TestingData():
    starttime = time.time()
    print('------------------------------------------------------------------------')
    print(f'TestingData \nStarted: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}')
    filename = 'TestingPivot.csv'
    df = pd.read_csv('https://data.ontario.ca/dataset/a2dfa674-a173-45b3-9964-1e3d2130b40f/resource/07bc0e21-26b5-4152-b609-c1958cb7b227/download/testing_metrics_by_phu.csv',
                     parse_dates=[0], infer_datetime_format=True)
    df.to_csv('SourceFiles/TestingData.csv')
    # df['DATE'] = pd.to_datetime(df['DATE'],format='%d-%b-%y')
    df['DATE'] = pd.to_datetime(df['DATE'], format='%b/%d/%Y')
    df['test_volumes_7d_avg'] = df['test_volumes_7d_avg'].str.replace(',', '').astype(int)
    # df = df[df['DATE'] == df['DATE'].max()].copy()
    # df['percent_positive_7d_avg'] = df['percent_positive_7d_avg']*100

    df['PHU_name'] = df['PHU_name'].replace(['City of Toronto Health Unit'], 'Toronto PHU')
    df['PHU_name'] = df['PHU_name'].replace(['Peel Regional Health Unit'], 'Peel')
    df['PHU_name'] = df['PHU_name'].replace(['York Regional Health Unit'], 'York')
    df['PHU_name'] = df['PHU_name'].replace(['Waterloo Health Unit'], 'Waterloo Region')
    df['PHU_name'] = df['PHU_name'].replace(['Durham Regional Health Unit'], 'Durham')
    df['PHU_name'] = df['PHU_name'].replace(['City of Hamilton Health Unit'], 'Hamilton')
    df['PHU_name'] = df['PHU_name'].replace(['Middlesex-London Health Unit'], 'London')
    df['PHU_name'] = df['PHU_name'].replace(['Halton Regional Health Unit'], 'Halton')
    df['PHU_name'] = df['PHU_name'].replace(['Simcoe Muskoka District Health Unit'], 'Simcoe-Muskoka')
    df['PHU_name'] = df['PHU_name'].replace(['Niagara Regional Area Health Unit'], 'Niagara')
    df['PHU_name'] = df['PHU_name'].replace(['Windsor-Essex County Health Unit'], 'Windsor')
    df['PHU_name'] = df['PHU_name'].replace(['Wellington-Dufferin-Guelph Health Unit'],
                                            'Wellington-Guelph')
    df['PHU_name'] = df['PHU_name'].replace(['Kingston, Frontenac and Lennox and Addington Health Unit'],
                                            'Kingston')
    df['PHU_name'] = df['PHU_name'].replace(['Southwestern Public Health'], 'Southwestern')
    df['PHU_name'] = df['PHU_name'].replace(['Chatham-Kent Health Unit'], 'Chatham-Kent')
    df['PHU_name'] = df['PHU_name'].replace(['City of Ottawa Health Unit'], 'Ottawa')
    df['PHU_name'] = df['PHU_name'].replace(['District of Algoma Health Unit'], 'Algoma')
    df['PHU_name'] = df['PHU_name'].replace(['Thunder Bay District Health Unit'], 'Thunder Bay')
    df['PHU_name'] = df['PHU_name'].replace(['Timiskaming Health Unit'], 'Timiskaming')
    df['PHU_name'] = df['PHU_name'].replace(['Porcupine Health Unit'], 'Porcupine')
    df['PHU_name'] = df['PHU_name'].replace(['Sudbury and District Health Unit'], 'Sudbury')
    df['PHU_name'] = df['PHU_name'].replace(['Brant County Health Unit'], 'Brant')
    df['PHU_name'] = df['PHU_name'].replace(['Eastern Ontario Health Unit'], 'Eastern Ontario')
    df['PHU_name'] = df['PHU_name'].replace(['Leeds, Grenville and Lanark District Health Unit'],
                                            'Leeds, Grenville, Lanark')
    df['PHU_name'] = df['PHU_name'].replace(['Haldimand-Norfolk Health Unit'], 'Haldimand-Norfolk')
    df['PHU_name'] = df['PHU_name'].replace(['Lambton Health Unit'], 'Lambton')
    df['PHU_name'] = df['PHU_name'].replace(['Haliburton, Kawartha, Pine Ridge District Health Unit'],
                                            'Haliburton, Kawartha')
    df['PHU_name'] = df['PHU_name'].replace(['Grey Bruce Health Unit'], 'Grey Bruce')
    df['PHU_name'] = df['PHU_name'].replace(['Huron Perth'], 'Huron Perth')
    df['PHU_name'] = df['PHU_name'].replace(['Peterborough County-City Health Unit'], 'Peterborough')
    df['PHU_name'] = df['PHU_name'].replace(['Renfrew County and District Health Unit'], 'Renfrew')
    df['PHU_name'] = df['PHU_name'].replace(['Hastings and Prince Edward Counties Health Unit'],
                                            'Hastings')
    df['PHU_name'] = df['PHU_name'].replace(['Northwestern Health Unit'], 'Northwestern')
    df['PHU_name'] = df['PHU_name'].replace(['North Bay Parry Sound District Health Unit'],
                                            'North Bay')

    df = df.rename(columns={'PHU_name': "Reporting_PHU",
                            'percent_positive_7d_avg': '%positive (week)',
                            'test_volumes_7d_avg': 'Tests/day (week)',
                            'tests_per_1000_7d_avg': 'Tests per 100k/day (week)'}).copy()

    pivotpositive = pd.pivot_table(df, index='Reporting_PHU', values='%positive (week)',
                                   columns='DATE', aggfunc='max')
    pivotpositive = pivotpositive.reindex(columns=sorted(pivotpositive.columns, reverse=True))
    pivotpositive.sort_values(by=pivotpositive.columns[0], ascending=False, inplace=True)

    pivottestsper100 = pd.pivot_table(df, index='Reporting_PHU', values='Tests per 100k/day (week)',
                                      columns='DATE', aggfunc='max')
    pivottestsper100 = pivottestsper100.reindex(columns=sorted(pivottestsper100.columns,
                                                               reverse=True))
    pivottestsper100.sort_values(by=pivottestsper100.columns[0], ascending=False,
                                 inplace=True)

    pivottestvolumes = pd.pivot_table(df, index='Reporting_PHU', values='Tests/day (week)',
                                      columns='DATE', aggfunc='sum')
    pivottestvolumes = pivottestvolumes.reindex(columns=sorted(pivottestvolumes.columns, reverse=True))
    pivottestvolumes = pivottestvolumes.sort_values(by=pivottestvolumes.columns[0], ascending=False)

    Report_Date = df['DATE'].max()

    PivotPositiveSingle = pd.DataFrame(pivotpositive[pivotpositive.columns[0]].copy())
    PivotPositiveSingle.rename(columns={PivotPositiveSingle.columns[0]: 'Positive Rate (% in week)'},
                               inplace=True)
    PivotPositiveSingle['Positive Rate (% in week)'] = PivotPositiveSingle['Positive Rate (% in week)'] * 100
    PivotTestsPer100Single = pd.DataFrame(pivottestsper100[pivottestsper100.columns[0]].copy())
    PivotTestsPer100Single.rename(columns={PivotTestsPer100Single.columns[0]: 'Tests per 100k/day (week)'},
                                  inplace=True)
    PivotTestVolumesSingle = pd.DataFrame(pivottestvolumes[pivottestvolumes.columns[0]].copy())
    PivotTestVolumesSingle.rename(columns={PivotTestVolumesSingle.columns[0]: 'Test volumes/day (week)'},
                                  inplace=True)
    TestDataSnapshot = PivotPositiveSingle.copy().join(PivotTestsPer100Single).join(PivotTestVolumesSingle)

    dfAgeTesting = pd.read_csv('https://data.ontario.ca/dataset/ab5f4a2b-7219-4dc7-9e4d-aa4036c5bf36/resource/05214a0d-d8d9-4ea4-8d2a-f6e3833ba471/download/percent_positive_by_agegrp.csv')
    dfAgeTesting.to_csv('SourceFiles/Testing-TestingByAge.csv')
    dfAgeTesting['DATE'] = pd.to_datetime(dfAgeTesting['DATE'])
    dfAgeTesting['age_category'] = dfAgeTesting['age_category'].replace(['0to13', '14to17',
                                                                         '18to24', '25to64', '65+'],
                                                                        ['13 and under', '14 to 17',
                                                                         '18 to 24', '25 to 64',
                                                                         '65 and over'])
    pivotAge = pd.pivot_table(dfAgeTesting, index='age_category',
                              values='percent_positive_7d_avg', columns='DATE')
    pivotAge = pivotAge.T.sort_values(by='DATE', ascending=False).T

    with open(filename, 'w', newline='') as f:
        f.write('Positive rates per PHU (trailing 7 day) ' + str(Report_Date) + '\n')
        pivotpositive.to_csv(f, header=True)
        f.write('\n')
        f.write('Tests per 100k (trailing 7 day)' + str(Report_Date) + '\n')
        pivottestsper100.to_csv(f, header=True)
        f.write('\n')
        f.write('Tests (trailing 7 day)' + str(Report_Date) + '\n')
        pivottestvolumes.to_csv(f, header=True)
        f.write('\n')
        f.write('Testing Snapshot' + str(Report_Date) + '\n')
        TestDataSnapshot.to_csv(f, header=True)
        f.write('\n')
        f.write('Positive rates by age (trailing 7 day)')
        pivotAge.to_csv(f, header=True)

    print(f'Ended:   {datetime.datetime.now():%Y-%m-%d %H:%M:%S} {round(time.time() - starttime, 2)} seconds')
    print(f'Report as of: {(Report_Date + datetime.timedelta(8)): %Y-%m-%d}')
    print('------------------------------------------------------------------------')


def COVIDAppData():
    starttime = time.time()
    ConsoleOut = sys.stdout
    TextFileName = 'TextOutput/COVIDAppDataText.txt'

    dfUpload = pd.read_csv('https://data.ontario.ca/dataset/06a61019-62c1-48d8-8d4d-2267ae0f1144/resource/b792e734-9c69-47d5-8451-40fc85c2f3c6/download/covid_alert_positive_uploads_ontario.csv',
                           parse_dates=[0], infer_datetime_format=True)
    dfUpload.to_csv('SourceFiles/COVIDAppData-uploads')
    dfUpload = dfUpload.sort_values(by='date', ascending=False)
    dfUpload = dfUpload.set_index('date')
    dfUpload.to_pickle('Pickle/AppUpload.pickle')
    CaseStatusDF = pd.read_pickle('Pickle/OntarioCaseStatus.pickle')
    CaseStatusDF = CaseStatusDF[CaseStatusDF.index > '2020-07-30']
    PctPositiveCasesDay = round(dfUpload.iloc[0].daily_positive_otks_uploaded_ontario
                                / CaseStatusDF.iloc[0]['Day new cases'], 4)
    PctPositiveCasesDay = "{:,.1%}".format(PctPositiveCasesDay)
    PctPositiveCasesWeek = round(dfUpload.iloc[0:7].daily_positive_otks_uploaded_ontario.sum()
                                 / CaseStatusDF.iloc[0:7]['Day new cases'].sum(), 4)
    PctPositiveCasesWeek = "{:,.1%}".format(PctPositiveCasesWeek)
    PctPositiveCasesMonth = round(dfUpload.iloc[0:30].daily_positive_otks_uploaded_ontario.sum()
                                  / CaseStatusDF.iloc[0:30]['Day new cases'].sum(), 4)
    PctPositiveCasesMonth = "{:,.1%}".format(PctPositiveCasesMonth)
    PctPositiveCasesAllTime = round(dfUpload['daily_positive_otks_uploaded_ontario'].sum()
                                    / CaseStatusDF['Day new cases'].sum(), 4)
    PctPositiveCasesAllTime = "{:,.1%}".format(PctPositiveCasesAllTime)


    dfDown = pd.read_csv('https://data.ontario.ca/dataset/06a61019-62c1-48d8-8d4d-2267ae0f1144/resource/37cfeca2-059e-4a5f-a228-249f6ab1b771/download/covid_alert_downloads_canada.csv',
                         parse_dates=[0], infer_datetime_format=True)
    dfDown.to_csv('SourceFiles/COVIDAppData-downloads.csv')
    dfDown = dfDown.sort_values(by='date', ascending=False)
    dfDown = dfDown.set_index('date')
    dfDown = dfDown.fillna(0)
    for column in dfDown.columns:
        if dfDown[column].dtypes == 'O':
            dfDown[column] = dfDown[column].str.replace(',', '')
            dfDown[column] = dfDown[column].fillna(0).astype(int)
            dfDown[column] = dfDown[column].astype(int)

    dfDown.to_pickle('Pickle/AppDownload.pickle')

    PctAndroidDay = (dfDown.iloc[0].daily_android_downloads_canada
                     / dfDown.iloc[0].daily_total_downloads_canada)
    PctAndroidDay = "{:,.1%}".format(PctAndroidDay)

    PctAndroidWeek = (dfDown.iloc[0:7].daily_android_downloads_canada.sum()
                      / dfDown.iloc[0:7].daily_total_downloads_canada.sum())
    PctAndroidWeek = "{:,.1%}".format(PctAndroidWeek)

    PctAndroidMonth = (dfDown.iloc[0:30].daily_android_downloads_canada.sum()
                       / dfDown.iloc[0:30].daily_total_downloads_canada.sum())
    PctAndroidMonth = "{:,.1%}".format(PctAndroidMonth)

    PctAndroidAllTime = (dfDown.iloc[:].daily_android_downloads_canada.sum()
                         / dfDown.iloc[:].daily_total_downloads_canada.sum())
    PctAndroidAllTime = "{:,.1%}".format(PctAndroidAllTime)

    sys.stdout = open(TextFileName, 'w')
    print('**COVID App Stats** - *latest data as of ', dfUpload.iloc[0].name.strftime('%B %d'),
          '* - [Source](https://data.ontario.ca/dataset/covid-alert-impact-data)', sep='')
    print()
    print('* Positives Uploaded to app in last day/week/month/since launch: ',
          "{:,}".format(dfUpload.iloc[0].daily_positive_otks_uploaded_ontario),
          ' / ', "{:,}".format(dfUpload.iloc[0:7].daily_positive_otks_uploaded_ontario.sum()),
          ' / ', "{:,}".format(dfUpload.iloc[0:30].daily_positive_otks_uploaded_ontario.sum()),
          ' / ', "{:,}".format(dfUpload['daily_positive_otks_uploaded_ontario'].sum()),
          ' (', PctPositiveCasesDay, ' / ', PctPositiveCasesWeek, ' / ', PctPositiveCasesMonth, ' / ',
          PctPositiveCasesAllTime, ' of all cases)', sep='')


    print('* App downloads in last day/week/month/since launch: ',
          "{:,}".format(dfDown.iloc[0].daily_android_downloads_canada),
          ' / ', "{:,}".format(dfDown.iloc[0:7].daily_android_downloads_canada.sum()),
          ' / ', "{:,}".format(dfDown.iloc[0:30].daily_android_downloads_canada.sum()),
          ' / ', "{:,}".format(dfDown.iloc[:].daily_android_downloads_canada.sum()),
          ' (', PctAndroidDay, ' / ', PctAndroidWeek, ' / ', PctAndroidMonth, ' / ',
          PctAndroidAllTime, ' Android share)', sep='')
    sys.stdout = ConsoleOut
    # print('Report as of: ',dfDown.iloc[0].name.strftime('%B %d'))
    print('------------------------------------------------------------------------')
    print('COVIDAppData: ', round(time.time() - starttime, 2), 'seconds')
    print('------------------------------------------------------------------------')


def LTCData():
    starttime = time.time()

    textfilename = 'TextOutput/LTCDataText.txt'
    ##################################################################################################
    # ActiveOutbreak
    df = pd.read_csv('https://data.ontario.ca/dataset/42df36df-04a0-43a9-8ad4-fac5e0e22244/resource/4b64488a-0523-4ebb-811a-fac2f07e6d59/download/activeltcoutbreak.csv',
                     encoding='cp1252')
    df.to_csv('SourceFiles/LTCData-ActiveOutbreaks.csv')
    df.columns = EVHelper.remove_specialchars(df.columns)
    df['Report_Data_Extracted'] = pd.to_datetime(df['Report_Data_Extracted'])
    df['Active Outbreak'] = True

    df.sort_values(by='Report_Data_Extracted', ascending=False, inplace=True)
    abc = pd.DataFrame()
    for LTCHome in set(df['LTC_Home']):
        # print(LTCHome,df[df['LTC_Home']==LTCHome].iloc[0]['Report_Data_Extracted'])
        abc = abc.append(df[df['LTC_Home'] == LTCHome].iloc[0])

    ###############################################################################################
    # Resolved outbreaks
    try:
        df2 = pd.read_csv('https://data.ontario.ca/dataset/42df36df-04a0-43a9-8ad4-fac5e0e22244/resource/0cf2f01e-d4e1-48ed-8027-2133d059ec8b/download/resolvedltc.csv')
    except ValueError:
        df2 = pd.read_csv('https://data.ontario.ca/dataset/42df36df-04a0-43a9-8ad4-fac5e0e22244/resource/0cf2f01e-d4e1-48ed-8027-2133d059ec8b/download/resolvedltc.csv',
                          names=['Report_Data_Extracted', 'PHU_Num', 'PHU', 'LTC_Home', 'City',
                                 'Beds', 'Total_LTC_Resident_Deaths'],
                          skiprows=1)
    df2.to_csv('SourceFiles/LTCData-ActiveOutbreaks.csv')

    df2['Active Outbreak'] = False
    df2['Report_Data_Extracted'] = pd.to_datetime(df2['Report_Data_Extracted'])
    df2['Total_LTC_Resident_Deaths'] = df2['Total_LTC_Resident_Deaths'].str.replace("^<5", "<5",
                                                                                    regex=True)
    abc = df.copy()
    abc = abc.append(df2)
    abc = abc.replace({'<5': 2.5})
    abc = abc.fillna(0)

    abc.sort_values(by='Report_Data_Extracted', ascending=False)
    abc = abc.sort_values(by='Report_Data_Extracted', ascending=False)
    abc['Total_LTC_Resident_Cases'] = pd.to_numeric(abc['Total_LTC_Resident_Cases'])
    try:
        abc['Total_LTC_Resident_Deaths'] = pd.to_numeric(abc['Total_LTC_Resident_Deaths'])
    except ValueError:
        abc['Total_LTC_Resident_Deaths'] = pd.to_numeric(abc['Total_LTC_Resident_Deaths'],
                                                         errors='coerce')

    abc['Total_LTC_HCW_Cases'] = pd.to_numeric(abc['Total_LTC_HCW_Cases'])

    # pivotdf = pd.pivot_table(abc,values = 'Report_Date',index = 'LTC_Home',aggfunc = np.count_non_zero(), fill_value=0)
    pivotLTCDeaths = pd.pivot_table(abc, values='Total_LTC_Resident_Deaths', index='LTC_Home',
                                    columns='Report_Data_Extracted', aggfunc=np.sum, fill_value=0)
    pivotLTCDeaths = pivotLTCDeaths.reindex(columns=sorted(pivotLTCDeaths.columns, reverse=True))
    pivotLTCDeaths = pivotLTCDeaths.sort_values(by=pivotLTCDeaths.columns[0], ascending=False)
    pivotLTCDeaths = pivotLTCDeaths.rename(columns={pivotLTCDeaths.columns[0]: "All-time Deaths"})

    newLTCDeaths = pivotLTCDeaths - pivotLTCDeaths.shift(-1, axis=1)
    newLTCDeaths = newLTCDeaths.sort_values(by=[newLTCDeaths.columns[0], newLTCDeaths.columns[1]],
                                            ascending=False)
    newLTCDeaths = newLTCDeaths.rename(columns={newLTCDeaths.columns[0]: "Today's Deaths"})
    Report_Date = abc['Report_Data_Extracted'].max()

    DisplayDF = pd.DataFrame(abc[['LTC_Home', 'City', 'Beds']])
    DisplayDF = DisplayDF.drop_duplicates(subset=['LTC_Home'], keep='first')
    DisplayDF = DisplayDF.set_index('LTC_Home')

    DeathDisplayDF = DisplayDF.copy()
    DeathDisplayDF['Beds'] = DeathDisplayDF['Beds'].astype(int)
    DeathDisplayDF = DeathDisplayDF.merge(newLTCDeaths[newLTCDeaths.columns[0]], how='inner',
                                          left_index=True, right_index=True)
    DeathDisplayDF = DeathDisplayDF.merge(pivotLTCDeaths[pivotLTCDeaths.columns[0]], how='inner',
                                          left_index=True, right_index=True)
    DeathDisplayDF = DeathDisplayDF.sort_values(by="Today's Deaths", ascending=False)
    DeathDisplayDF = DeathDisplayDF[abs(DeathDisplayDF["Today's Deaths"]) > 0]

    NewLTCCasePivot = pd.pivot_table(abc, values='Total_LTC_Resident_Cases', index='LTC_Home',
                                     columns='Report_Data_Extracted', aggfunc=np.sum, fill_value=0)
    NewLTCCasePivot = NewLTCCasePivot.reindex(columns=sorted(NewLTCCasePivot.columns, reverse=True))
    NewLTCCasePivot = NewLTCCasePivot.sort_values(by=NewLTCCasePivot.columns[0], ascending=False)
    NewLTCCasePivot = NewLTCCasePivot.rename(columns={NewLTCCasePivot.columns[0]: "Current Active Cases"})
    NewLTCCasePivot['Change in active cases'] = (NewLTCCasePivot[NewLTCCasePivot.columns[0]]
                                                 - NewLTCCasePivot[NewLTCCasePivot.columns[1]])
    NewLTCCasePivot = NewLTCCasePivot[['Current Active Cases', 'Change in active cases']]
    NewLTCCasePivot = pd.merge(NewLTCCasePivot, newLTCDeaths[newLTCDeaths.columns[0]],
                               left_index=True, right_index=True)
    NewLTCCasePivot = pd.merge(NewLTCCasePivot, DisplayDF, left_index=True,
                               right_index=True, how='left')
    NewLTCCasePivot['New LTC cases'] = NewLTCCasePivot['Change in active cases'] + NewLTCCasePivot["Today's Deaths"]
    # NewLTCCasePivot = NewLTCCasePivot.sort_values(by = 'New LTC cases',ascending = False)
    # NewLTCCasePivot = NewLTCCasePivot.replace(to_replace='.0', value='', regex=True)
    NewLTCCasePivot['Beds'] = NewLTCCasePivot['Beds'].astype(float).astype(int)
    NewLTCCasePivot = NewLTCCasePivot[['City', 'Beds', 'New LTC cases', 'Current Active Cases']]
    NewLTCCasePivot = NewLTCCasePivot[NewLTCCasePivot['New LTC cases'] > 4].sort_values(by='New LTC cases', ascending=False)

    # with open(filename, 'w',newline='') as f:
    #     f.write('New LTC Deaths '+str(Report_Date)+'\n')
    #     DisplayDF.to_csv(f,header = True)
    #     f.write('\n')

    #     f.write('LTC Deaths by Home '+str(Report_Date)+'\n')
    #     newLTCDeaths.to_csv(f,header = True)
    #     f.write('\n')

    # with open(textfilename,'a',newline='') as f:
    #     f.write(''+'\n')
    #     f.write('New LTC Cases '+str(Report_Date)+'\n\n')
    #     NewLTCCasePivot.to_csv(f,sep = '|')
    #     f.write(''+'\n')
    #     f.write('New LTC Deaths '+str(Report_Date)+'\n\n')
    #     DeathDisplayDF.to_csv(f,header = False,sep = '|')

    header = 'LTC_Home|'
    for column in range(0, NewLTCCasePivot.columns.size):
        header = header + NewLTCCasePivot.columns[column] + '|'

    with open(textfilename, 'w', newline='') as f:
        f.write('**LTCs with 5+ new cases today:** [Why are there 0.5 cases/deaths?](https://www.reddit.com/r/ontario/comments/ksfe1d/ontario_january_07_update_3519_new_cases_2776/giflq3y/)\n\n')
        f.write(header)
        f.write('\n')
        f.write(':--|:--|--:|--:|--:|')
        f.write('\n')
        NewLTCCasePivot.to_csv(f, sep='|', header=False)
        f.write('\n')

    header = 'LTC_Home|'
    for column in range(0, DeathDisplayDF.columns.size):
        header = header + DeathDisplayDF.columns[column] + '|'

    with open(textfilename, 'a', newline='') as f:
        f.write('**LTC Deaths today:** - this section is reported by the Ministry of LTC and the data may not reconcile with the LTC data above because that is published by the MoH.\n\n')
        f.write(header)
        f.write('\n')
        f.write(':--|:--|:--|--:|--:|\n')
        DeathDisplayDF.to_csv(f, sep='|', header=False)
        if DeathDisplayDF.size == 0:
            f.write('\n')
            f.write('None reported by the Ministry of LTC\n')
        f.write('\n')

    del NewLTCCasePivot
    del DeathDisplayDF

    print(Report_Date)
    print('LTCData: (time) ', round(time.time() - starttime, 2), 'seconds')
    print('------------------------------------------------------------------------')


def VaccineData(download=True):
    HospitalMetrics(download=download)
    starttime = time.time()
    ConsoleOut = sys.stdout
    VaccineDataFileName = 'TextOutput/VaccineDataText.txt'

    TotalDelivered = 35805101
    DeliveryDataDate = datetime.datetime(2022, 2, 3).strftime('%B %#d')

    OntarioPopulationDF = pd.read_pickle('Pickle/OntarioPopulation.pickle')
    OntarioPopulation = (int)(OntarioPopulationDF.loc['REFERENCE', 2021, 'TOTAL'].sum())

    if download:
        df = pd.read_csv('https://data.ontario.ca/dataset/752ce2b7-c15a-4965-a3dc-397bf405e7cc/resource/8a89caa9-511c-4568-af89-7f2174b4378c/download/vaccine_doses.csv')
        df.to_csv('SourceFiles/VaccineData-VaccineDoses.csv')
        df['report_date'] = pd.to_datetime(df['report_date'])
        df = df.set_index('report_date')

    else:
        df = pd.read_pickle('Pickle/VaccinesData.pickle')

    df = df.fillna(0)
    # df['total_individuals_fully_vaccinated']= df['previous_day_fully_vaccinated'].cumsum(axis=0)
    # df['total_doses_in_fully_vaccinated_individuals'] = df['total_individuals_fully_vaccinated']*2
    df = df.sort_index(ascending=False)
    # df = df.fillna(0)
    # df['previous_day_doses_administered'] = df['previous_day_doses_administered'].str.replace(',','').fillna(0).astype(int)
    # df['total_doses_administered'] = df['total_doses_administered'].str.replace(',','').fillna(0).astype(int)
    # df['total_individuals_fully_vaccinated'] = df['total_individuals_fully_vaccinated'].str.replace(',','').fillna(0).astype(int)
    # df['total_doses_in_fully_vaccinated_individuals '] = df['total_doses_in_fully_vaccinated_individuals '].str.replace(',','').fillna(0).astype(int)
    df['previous_day_total_doses_administered'] = df['previous_day_total_doses_administered'].fillna(0).astype(int)
    # df['previous_day_doses_administered'] = df['previous_day_doses_administered'].fillna(0).astype(int)
    df['total_doses_administered'] = df['total_doses_administered'].fillna(0).astype(int)
    df['total_individuals_fully_vaccinated'] = df['total_individuals_fully_vaccinated'].fillna(0).astype(int)
    df['total_doses_in_fully_vaccinated_individuals'] = df['total_doses_in_fully_vaccinated_individuals'].fillna(0).astype(int)
    # df['SecondDoseInDay'] = (df['total_individuals_fully_vaccinated'] - df['total_individuals_fully_vaccinated'].shift(-1)).fillna(0).astype(int)
    df['SecondDoseInDay'] = df['previous_day_fully_vaccinated'].fillna(0).astype(int)
    # df['FirstDoseInDay'] = (df['previous_day_doses_administered'] - df['SecondDoseInDay']).fillna(0).astype(int)
    df['FirstDoseInDay'] = df['previous_day_at_least_one'].fillna(0).astype(int)
    df['ThirdDoseInDay'] = (df['total_individuals_3doses']
                            - df['total_individuals_3doses'].shift(-1).fillna(0)).astype(int)
    df['FourthDoseInDay'] =(df['previous_day_total_doses_administered'] - df['SecondDoseInDay']
                            - df['ThirdDoseInDay'])

    df['total_individuals_3doses'] = df['total_individuals_3doses'].astype(int)
    df['ThreeDosedPopulation_5Plus'] = (df['total_individuals_3doses']
                                        / OntarioPopulationDF.loc['REFERENCE', 2021, 'TOTAL'][5:].sum())
    df['ThreeDosedPopulation_5Plus_Day'] = (df['ThreeDosedPopulation_5Plus']
                                            - df['ThreeDosedPopulation_5Plus'].shift(-1))

    picklefilename = 'Pickle/VaxStatusCases_AgeDF.pickle'
    if download:
        VaxStatusCases_AgeDF = pd.read_csv('https://data.ontario.ca/dataset/752ce2b7-c15a-4965-a3dc-397bf405e7cc/resource/c08620e0-a055-4d35-8cec-875a459642c3/download/cases_by_age_vac_status.csv')
        VaxStatusCases_AgeDF.to_csv('SourceFiles/cases_by_age_vac_status.csv')
        # VaxStatusCases_AgeDF['Date'] = pd.to_datetime(VaxStatusCases_AgeDF['date'],format='%Y-%m-%d')
        VaxStatusCases_AgeDF['Date'] = pd.to_datetime(VaxStatusCases_AgeDF['date'],)
        VaxStatusCases_AgeDF = VaxStatusCases_AgeDF.set_index('agegroup')
        VaxStatusCases_AgeDF.to_pickle(picklefilename)
    else:
        VaxStatusCases_AgeDF = pd.read_pickle(picklefilename)
    VaxStatusCases_AgeDF_Today = VaxStatusCases_AgeDF[VaxStatusCases_AgeDF['Date']
                                                      == VaxStatusCases_AgeDF['Date'].max()]

    picklefilename = 'Pickle/dfVaxAge.pickle'
    if download:
        dfVaxAge = pd.read_csv('https://data.ontario.ca/dataset/752ce2b7-c15a-4965-a3dc-397bf405e7cc/resource/775ca815-5028-4e9b-9dd4-6975ff1be021/download/vaccines_by_age.csv',
                           parse_dates=[0], infer_datetime_format=True)
        dfVaxAge.to_csv('SourceFiles/VaccineData-vaccines_by_age.csv')
        dfVaxAge.to_pickle(picklefilename)
    else:
        dfVaxAge = pd.read_pickle(picklefilename)

    df.rename(columns={'AGEGROUP': 'Agegroup'}, inplace=True)

    dfVaxAge = dfVaxAge.fillna(0)

    OntarioAdultPopulation = dfVaxAge[dfVaxAge['Agegroup'] == 'Adults_18plus']['Total population'].iloc[0]

    # dfVaxAge = dfVaxAge[dfVaxAge['Agegroup']!='Adults_18plus']
    dfVaxAge = dfVaxAge.replace('80+', '80+ yrs')
    dfVaxAge = dfVaxAge.replace('Undisclosed_or_missing', 'Unknown')
    dfVaxAge['fully_vaccinated_cumulative'] = (dfVaxAge['fully_vaccinated_cumulative']
                                               + dfVaxAge['Second_dose_cumulative'])
    ###########################################################################
    ###########################################################################
    # This section renames the unknown to the 5-11 category because that is what
    # it appears to be.
    # dfVaxAge = dfVaxAge.replace('Unknown', '05-11yrs')  # Reclass unknown to 5-11 yrs age group

    ###########################################################################
    ###########################################################################
    dfVaxAge = dfVaxAge.sort_values(by=['Date', 'Agegroup'],
                                    ascending=[False, True])

    dfVaxAge = dfVaxAge.set_index('Agegroup')
    # dfVaxAge.loc['05-11yrs', 'Total population'] = OntarioPopulationDF.loc['REFERENCE', 2021, 'TOTAL'][5:12].sum()

    for age in set(dfVaxAge.index):
        # print(age)
        dfVaxAge.loc[age, 'FirstDose - in last day'] = (dfVaxAge.loc[age]['At least one dose_cumulative']
                                                        - dfVaxAge.loc[age]['At least one dose_cumulative'].shift(-1)).fillna(0).astype(int)

        dfVaxAge.loc[age, 'SecondDose - in last day'] = (dfVaxAge.loc[age]['fully_vaccinated_cumulative']
                                                         - dfVaxAge.loc[age]['fully_vaccinated_cumulative'].shift(-1)).fillna(0).astype(int)

        dfVaxAge.loc[age, 'ThirdDose - in last day'] = (dfVaxAge.loc[age]['third_dose_cumulative']
                                                        - dfVaxAge.loc[age]['third_dose_cumulative'].shift(-1)).fillna(0).astype(int)

        dfVaxAge.loc[age, 'FirstDose - in last week'] = (dfVaxAge.loc[age]['At least one dose_cumulative']
                                                         - dfVaxAge.loc[age]['At least one dose_cumulative'].shift(-7)).fillna(0).astype(int)

        dfVaxAge.loc[age, 'SecondDose - in last week'] = (dfVaxAge.loc[age]['fully_vaccinated_cumulative']
                                                          - dfVaxAge.loc[age]['fully_vaccinated_cumulative'].shift(-7)).fillna(0).astype(int)

        dfVaxAge.loc[age, 'ThirdDose - in last week'] = (dfVaxAge.loc[age]['third_dose_cumulative']
                                                         - dfVaxAge.loc[age]['third_dose_cumulative'].shift(-7)).fillna(0).astype(int)

    ###########################################################################
    ###########################################################################
    for day in set(dfVaxAge['Date']):
        # dfVaxAge[dfVaxAge['Date'] == day].loc['Ontario_12plus'] = dfVaxAge[dfVaxAge['Date'] == day].fillna(0).loc['12-17yrs':
        #                                                                                                           '80+ yrs'].sum()
        # dfVaxAge[dfVaxAge['Date'] == day].loc['Adults_18plus', dfVaxAge.columns[1]:] = dfVaxAge[dfVaxAge['Date']
        #                                                                                         == day].fillna(0).loc['18-29yrs': '80+ yrs',
        #                                                                                         dfVaxAge.columns[1]:].sum()

        # dfVaxAge.loc[(dfVaxAge['Date'] == day) & (dfVaxAge.index == 'Adults_18plus'), dfVaxAge.columns[1]:] = \
        #     list(dfVaxAge[dfVaxAge['Date'] == day].fillna(0).loc['18-29yrs': '80+ yrs',
        #                                                     dfVaxAge.columns[1]:].sum())

        # dfVaxAge[dfVaxAge['Date'] == day].loc['Total - 5+'] = dfVaxAge[dfVaxAge['Date'] == day].fillna(0).loc[: '80+ yrs'].sum()

        tempSeries = dfVaxAge[dfVaxAge['Date'] == day].fillna(0).loc['12-17yrs':
                                                                     '80+ yrs'].iloc[:, 1:].sum()
        ccc = pd.Series({'Date': day})
        tempSeries = tempSeries.append(ccc)
        tempSeries.name = 'Ontario_12plusNew'
        dfVaxAge = dfVaxAge.append(tempSeries)
        # print(day)

        # FivePlus = dfVaxAge[dfVaxAge['Date'] == day].fillna(0).loc[: '80+ yrs'].sum()
        # FivePlus.name = 'Total - 5+'
        # FivePlus['Date'] = day
        # dfVaxAge = dfVaxAge.append(FivePlus)
    dfVaxAge = dfVaxAge.drop('Ontario_12plus')
    dfVaxAge = dfVaxAge.rename(index={'Ontario_12plusNew': 'Ontario_12plus'})
    dfVaxAge = dfVaxAge.sort_values(['Date', 'Agegroup'], ascending=[False, True])
    # dfVaxAge.to_csv('aa.csv')

    dfVaxAge['Percent_at_least_one_dose'] = dfVaxAge['At least one dose_cumulative'] / dfVaxAge['Total population']
    dfVaxAge['Percent_fully_vaccinated'] = dfVaxAge['fully_vaccinated_cumulative'] / dfVaxAge['Total population']
    dfVaxAge['Percent_only_one_dose'] = dfVaxAge['Percent_at_least_one_dose'] - dfVaxAge['Percent_fully_vaccinated']
    dfVaxAge['Percent_Remaining_at_least_one_dose'] = 1 - dfVaxAge['Percent_at_least_one_dose']

    dfVaxAge['FirstDose - in last day %'] = ((dfVaxAge['FirstDose - in last day']
                                              / dfVaxAge['Total population']).round(4))
    dfVaxAge['SecondDose - in last day %'] = ((dfVaxAge['SecondDose - in last day']
                                               / dfVaxAge['Total population']).round(4))
    dfVaxAge['ThirdDose - in last day %'] = ((dfVaxAge['ThirdDose - in last day']
                                              / dfVaxAge['Total population']).round(4))
    dfVaxAge['FirstDose - in last week %'] = (dfVaxAge['FirstDose - in last week']
                                              / dfVaxAge['Total population']).round(4)
    dfVaxAge['SecondDose - in last week %'] = (dfVaxAge['SecondDose - in last week']
                                               / dfVaxAge['Total population']).round(4)
    dfVaxAge['ThirdDose - in last week %'] = ((dfVaxAge['ThirdDose - in last week']
                                               / dfVaxAge['Total population']).round(4))

    dfVaxAge = dfVaxAge.sort_values(by='Date', ascending=False)

    for age in set(dfVaxAge.index):
        dfVaxAge.loc[age, 'PercentOfRemaining_First - in last day'] = (dfVaxAge.loc[age]['FirstDose - in last day %']
                                                                       / dfVaxAge.loc[age]['Percent_Remaining_at_least_one_dose'].shift(-1))
        dfVaxAge.loc[age, 'PercentOfRemaining_First - in last week'] = (dfVaxAge.loc[age]['FirstDose - in last week %']
                                                                        / dfVaxAge.loc[age]['Percent_Remaining_at_least_one_dose'].shift(-7))
        dfVaxAge.loc[age, 'PercentOfRemaining_SecondElig - in last week'] = (dfVaxAge.loc[age]['SecondDose - in last week %']
                                                                             / dfVaxAge.loc[age]['Percent_only_one_dose'].shift(-28))

    ###########################################################################
    ###########################################################################

    TodaysDFVax = dfVaxAge[dfVaxAge['Date'] == pd.Timestamp(dfVaxAge['Date'].max())].copy()
    TodaysDFVax.loc['Unknown', 'Total population'] = TodaysDFVax.loc['Ontario_5plus', 'Total population']

    totals = TodaysDFVax.loc['Ontario_12plus']
    totals.name = 'Total - eligible 12+'
    TodaysDFVax = TodaysDFVax.append(totals)

    ###########################################################################
    ###########################################################################
    # This section renames the unknown to the 5-11 category because that is what
    # it appears to be.

    TodaysDFVax = TodaysDFVax.drop('Date', axis=1)
    TodaysDFVax = TodaysDFVax.reset_index()
    # TodaysDFVax = TodaysDFVax.replace('Unknown', '05-11yrs').sort_values(by='Agegroup')
    TodaysDFVax = TodaysDFVax.set_index('Agegroup')
    # TodaysDFVax.loc['05-11yrs', 'Total population'] = OntarioPopulationDF.loc['REFERENCE', 2021, 'TOTAL'][5:12].sum()
    totals_5Plus = TodaysDFVax.fillna(0).loc[: '80+ yrs'].sum()
    totals_5Plus.name = 'Total - 5+'
    # TodaysDFVax = TodaysDFVax.append(totals_5Plus)

    # TodaysDFVax.loc['Adults_18plus'] = TodaysDFVax.fillna(0).loc['18-29yrs':'80+ yrs'].sum()
    # TodaysDFVax.loc['Total - eligible 12+'] = TodaysDFVax.fillna(0).loc['12-17yrs':'80+ yrs'].sum()

    ###########################################################################
    ###########################################################################

    TodaysDFVax['Percent_at_least_one_dose'] = TodaysDFVax['At least one dose_cumulative'] / TodaysDFVax['Total population']
    TodaysDFVax['Percent_fully_vaccinated'] = TodaysDFVax['fully_vaccinated_cumulative'] / TodaysDFVax['Total population']
    TodaysDFVax['Percent_3doses'] = TodaysDFVax['third_dose_cumulative'] / TodaysDFVax['Total population']

    TodaysDFVax['FirstDose - in last day %'] = ((TodaysDFVax['FirstDose - in last day']
                                                 / TodaysDFVax['Total population']).round(4))
    TodaysDFVax['SecondDose - in last day %'] = ((TodaysDFVax['SecondDose - in last day']
                                                  / TodaysDFVax['Total population']).round(4))
    TodaysDFVax['ThirdDose - in last day %'] = ((TodaysDFVax['ThirdDose - in last day']
                                                  / TodaysDFVax['Total population']).round(4))

    TodaysDFVax['FirstDose - in last week %'] = (TodaysDFVax['FirstDose - in last week']
                                                 / TodaysDFVax['Total population']).round(4)
    TodaysDFVax['SecondDose - in last week %'] = (TodaysDFVax['SecondDose - in last week']
                                                  / TodaysDFVax['Total population']).round(4)
    TodaysDFVax['ThirdDose - in last week %'] = ((TodaysDFVax['ThirdDose - in last week']
                                                  / TodaysDFVax['Total population']).round(4))

    # TodaysDFVax['FirstDose - in last week %'] = TodaysDFVax['FirstDose - in last week %']

    if (TodaysDFVax.loc['05-11yrs']['FirstDose - in last week'] == 0):
        TodaysDFVax.at['05-11yrs', 'FirstDose - in last week %'] = TodaysDFVax.at['05-11yrs', 'Percent_at_least_one_dose']
    if (TodaysDFVax.loc['05-11yrs']['SecondDose - in last week'] == 0):
        TodaysDFVax.at['05-11yrs', 'SecondDose - in last week %'] = TodaysDFVax.at['05-11yrs', 'Percent_fully_vaccinated']

    # TodaysDFVax['FirstDose - in last week %'] = TodaysDFVax[['FirstDose - in last week %',
    #                                                          'FirstDose - in last day %']].max(axis=1)
    # TodaysDFVax['SecondDose - in last week %'] = TodaysDFVax[['SecondDose - in last week %',
    #                                                      'SecondDose - in last day %']].max(axis=1)

    TodaysDFVax = TodaysDFVax.sort_index(ascending=True)

    TodaysDFVax.to_csv('TodayDFVax.csv')

    Total_AtLeastOne = TodaysDFVax['At least one dose_cumulative'].sum()
    Total_BothDose = TodaysDFVax['Second_dose_cumulative'].sum()
    # Total_AtLeastOne_18Plus = TodaysDFVax['At least one dose_cumulative'].sum() - TodaysDFVax.loc['12-17yrs']['At least one dose_cumulative']- TodaysDFVax.loc['Unknown']['At least one dose_cumulative']
    # Total_BothDose_18Plus = TodaysDFVax['Second_dose_cumulative'].sum() - TodaysDFVax.loc['12-17yrs']['Second_dose_cumulative']- TodaysDFVax.loc['Unknown']['Second_dose_cumulative']
    # Total_AtLeastOne_12Plus = TodaysDFVax['At least one dose_cumulative'].sum() - TodaysDFVax.loc['Unknown']['At least one dose_cumulative']
    # Total_BothDose_12Plus = TodaysDFVax['Second_dose_cumulative'].sum() - TodaysDFVax.loc['Unknown']['Second_dose_cumulative']

    PercentVax_18Plus_AtLeastOne = TodaysDFVax.loc['Adults_18plus']['Percent_at_least_one_dose']
    PercentVax_18Plus_BothDose = TodaysDFVax.loc['Adults_18plus']['Percent_fully_vaccinated']

    # PercentVax_12Plus_AtLeastOne = Total_AtLeastOne_12Plus/OntarioPopulation_12Plus
    # PercentVax_12Plus_BothDose = Total_BothDose_12Plus/OntarioPopulation_12Plus

    AvgDailyVaccinationsWeek = df['previous_day_total_doses_administered'][0:7].mean()
    TotalAdministered = df['total_doses_administered'][0]
    TotalTwoDoses = df['total_individuals_fully_vaccinated'][0]
    TotalOneDose = TotalAdministered - TotalTwoDoses * 2
    TotalAtLeastOneDose = df['total_individuals_at_least_one'][0]
    TotalThreeDosed = df['total_individuals_3doses'][0]

    RemainingAdults_80Pct_Both = TodaysDFVax.loc['Adults_18plus']['Total population']*1.6 - TodaysDFVax.loc['Adults_18plus']['At least one dose_cumulative'] - TodaysDFVax.loc['Adults_18plus']['Second_dose_cumulative']

    TotalVaccinationsLeft = (OntarioAdultPopulation - TotalTwoDoses) * 2 - TotalOneDose

    #DaysTo80Pct_First_Adults = RemainingAdults_80Pct_First/df['FirstDoseInDay'][0:7].mean()
    # DaysTo80Pct_First_Adults = (RemainingAdults_80Pct_First
    #                             / (TodaysDFVax.loc['Adults_18plus']['FirstDose - in last week %'] / 7))

    DaysTo80Pct_First_Eligible = ((0.8 - TodaysDFVax.loc['Total - eligible 12+']['Percent_at_least_one_dose'])
                                  / (TodaysDFVax.loc['Total - eligible 12+']['FirstDose - in last week %'] / 7))
    DaysTo75Pct_Second_Eligible = ((0.75 - TodaysDFVax.loc['Total - eligible 12+']['Percent_fully_vaccinated'])
                                   / (TodaysDFVax.loc['Total - eligible 12+']['SecondDose - in last week %'] / 7))
    DaysTo80Pct_Second_Eligible = ((0.80 - TodaysDFVax.loc['Total - eligible 12+']['Percent_fully_vaccinated'])
                                   / (TodaysDFVax.loc['Total - eligible 12+']['SecondDose - in last week %'] / 7))
    DaysTo85Pct_Second_Eligible = ((0.85 - TodaysDFVax.loc['Total - eligible 12+']['Percent_fully_vaccinated'])
                                   / (TodaysDFVax.loc['Total - eligible 12+']['SecondDose - in last week %'] / 7))

    TotalUnused = TotalDelivered - TotalAdministered
    # df['TotalAtLeastOneDose'] = df['total_doses_administered']-df['total_doses_in_fully_vaccinated_individuals']/2
    df['TotalAtLeastOneDose'] = df['total_individuals_at_least_one']
    df['TotalAtLeastOneDose'] = df['TotalAtLeastOneDose'].astype(int)
    df['PercentAtLeastOneDosed'] = (df['TotalAtLeastOneDose'] / OntarioPopulation)

    # df['TotalTwoDosed'] = (df['total_doses_in_fully_vaccinated_individuals']/2).astype(int)
    df['TotalTwoDosed'] = df['total_individuals_fully_vaccinated']
    df['PercentTwoDosed'] = (df['TotalTwoDosed'] / OntarioPopulation)
    df['PartialVax_Count'] = df['TotalAtLeastOneDose'] - df['TotalTwoDosed']
    df['Unvax_Count'] = OntarioPopulation - df['TotalAtLeastOneDose']
    df['boosted_count'] = df['total_individuals_3doses']
    df['fourth_dosed_individuals'] = (TotalAdministered - df['TotalAtLeastOneDose'] - df['TotalTwoDosed']
                                      - TotalThreeDosed)
    TotalFourDosed = df['fourth_dosed_individuals'][0]

    df['not_fullvax_count'] = df['PartialVax_Count'] + df['Unvax_Count']

    df = EVHelper.TestDFIsPrime(df, 'previous_day_total_doses_administered', 'DayVaccineCount_IsPrime')
    df.to_pickle('Pickle/VaccinesData.pickle')

    VaccineData_PHU()  # Vaccines by PHU by age
    VaccineData_CaseStatus(download=download)  # Cases by vaccine status
    dfCaseByVaxStatus = pd.read_pickle('Pickle/CasesByVaxStatus.pickle')

    VaccineData_HospData()  # Hospital data by vax status
    dfICUHospByVaxStatus = pd.read_pickle('Pickle/ICUByVaxStatus.pickle')
    HospitalDF = pd.read_pickle('Pickle/HospitalData.pickle')  # Calculate unknown status ICU data
    TotalICUCount_Unknown = (HospitalDF[HospitalDF['date']
                                        == dfICUHospByVaxStatus.index.max() - datetime.timedelta(days=0)]['icu_crci_total'].sum()
                             - dfICUHospByVaxStatus.loc[dfICUHospByVaxStatus.index.max()]['ICU_total'])

    ###########################################################################################

    TodaysDFVax.to_pickle('Pickle/VaccineAgeData.pickle')
    dfVaxAge.to_pickle('Pickle/VaccineAgeAll.pickle')
    PopulationDF = pd.read_pickle('Pickle/OntarioPopulation.pickle')
    Population_0to4 = PopulationDF.loc['REFERENCE', 2021, 'TOTAL'][0:5].sum()
    Population_5to11 = PopulationDF.loc['REFERENCE', 2021, 'TOTAL'][5:12].sum()
    Population_5Plus = PopulationDF.loc['REFERENCE', 2021, 'TOTAL'][5:].sum()

    # print(Population_0to4, Population_5to11)

    TodaysDate = dfCaseByVaxStatus.index.max()
    time_now = datetime.datetime.now()
    today_actual = datetime.datetime(time_now.year, time_now.month, time_now.day)
    VaxStatusCases_AgeDF_Today_Date = VaxStatusCases_AgeDF_Today['Date'].max()

    # Under12DF.loc[TodaysDate]['0to4']
    # print(Under12DF.loc[TodaysDate]['0to4'],Under12DF.loc[TodaysDate]['5to11'])
    sys.stdout = open(VaccineDataFileName, 'w')

    print('**Vaccine effectiveness data: (assumed 14 days to effectiveness)** [Source](https://data.ontario.ca/dataset/covid-19-vaccine-data-in-ontario)')
    if (dfCaseByVaxStatus.index.max() == today_actual):
        print()
        print('|Metric|Not_FullVax|Not_FullVax_5+|Full|Boosted|Unknown|')
        print('|:-:|:-:|:-:|:-:|:-:|:-:|')
        print(f"|**Cases - today**|{dfCaseByVaxStatus['covid19_cases_notfull_vac'][0]:,.0f}|{dfCaseByVaxStatus['covid19_cases_unvac_5plus'][0]:,.0f}|{dfCaseByVaxStatus['covid19_cases_full_vac'][0]:,.0f}|{dfCaseByVaxStatus['covid19_cases_boost_vac'][0]:,.0f}|{dfCaseByVaxStatus['covid19_cases_vac_unknown'][0]:,.0f}|")
        print(f"|**Cases Per 100k - today**| {dfCaseByVaxStatus['NotFullVax_Per100k_Day'][0]:.2f}|{dfCaseByVaxStatus['NotFullVax_Per100k_Day_5Plus'][0]:.2f} | {dfCaseByVaxStatus['Fully_Per100k_Day'][0]:.2f}|{dfCaseByVaxStatus['Boosted_Per100k_Day'][0]:.2f}|- |")
        print(f"|**Risk vs. boosted - today**| {dfCaseByVaxStatus['NotFullVax_Risk_Higher'][0]:.2f}x|{dfCaseByVaxStatus['NotFullVax_Risk_Higher_5Plus'][0]:.2f}x | {dfCaseByVaxStatus['FullVax_Risk_Higher'][0]:.2f}x|1.00x|- |")
        print(f"|**Case % less (more) risk vs. not full vax - today**| -|- | {dfCaseByVaxStatus['FullyVax_Risk_Lower_Day'][0]:.1%}|{dfCaseByVaxStatus['Boosted_Risk_Lower_Day'][0]:.1%}|- |")
        print('|||||||')
        print(f"|**Avg daily Per 100k - week**| {dfCaseByVaxStatus['NotFullVax_Per100k_Week'][0]:.2f}|{dfCaseByVaxStatus['NotFullVax_Per100k_Week_5Plus'][0]:.2f} | {dfCaseByVaxStatus['Fully_Per100k_Week'][0]:.2f}|{dfCaseByVaxStatus['Boosted_Per100k_Week'][0]:.2f}|- |")
        print(f"|**Risk vs. boosted - week**| {dfCaseByVaxStatus['NotFullVax_Risk_Higher'][0]:.2f}x|{dfCaseByVaxStatus['NotFullVax_Risk_Higher_Week_5Plus'][0]:.2f}x | {dfCaseByVaxStatus['FullVax_Risk_Higher'][0]:.2f}x|1.00x|- |")
        print(f"|**[Case % less risk vs. unvax - week](https://docs.google.com/spreadsheets/d/e/2PACX-1vQ7fegCALd11ElozUYcMi-e9Dj69YaiNQhvEpk81JHsyTACl0UXkWK5zfMNFe49Tq3VuN9Av-fuEZqV/pubchart?oid=206167456&format=interactive)**| -|- | {dfCaseByVaxStatus['FullyVax_Risk_Lower_Week'][0]:.1%}|{dfCaseByVaxStatus['Boosted_Risk_Lower_Week'][0]:.1%}|- |")
        print('|||||||')

        print()
        print('|Metric|Unvax_All|Unvax_5+|Partial|Full|Unknown|')
        print('|:-:|:-:|:-:|:-:|:-:|:-:|')
        print(f"|**ICU - count**|{dfICUHospByVaxStatus['icu_unvac'][0]:,.0f}|n/a|{dfICUHospByVaxStatus['icu_partial_vac'][0]:,.0f}|{dfICUHospByVaxStatus['icu_full_vac'][0]:,.0f}|{TotalICUCount_Unknown:,.0f}|")
        print(f"|**ICU per mill**|{dfICUHospByVaxStatus['ICU_Unvax_PerMillion'][0]:,.2f}|-|{dfICUHospByVaxStatus['ICU_Partial_PerMillion'][0]:,.2f}|{dfICUHospByVaxStatus['ICU_Fully_PerMillion'][0]:,.2f}|-|")
        print(f"|**ICU % less risk vs. unvax**|-|-|{(1-dfICUHospByVaxStatus['ICU_Partial_PerMillion'][0]/dfICUHospByVaxStatus['ICU_Unvax_PerMillion'][0]):,.1%}|{(1-dfICUHospByVaxStatus['ICU_Fully_PerMillion'][0]/dfICUHospByVaxStatus['ICU_Unvax_PerMillion'][0]):,.1%}|-|")
        print(f"|**ICU risk vs. full**|{(dfICUHospByVaxStatus['Unvax_Risk_Higher_ICU'][0]):.2f}x|-|{(dfICUHospByVaxStatus['Partial_Risk_Higher_ICU'][0]):.2f}x|1.00x|-|")
        print('|||||||')
        print(f"|**Non_ICU Hosp - count**|{dfICUHospByVaxStatus['hospitalnonicu_unvac'][0]:,.0f}|n/a|{dfICUHospByVaxStatus['hospitalnonicu_partial_vac'][0]:,.0f}|{dfICUHospByVaxStatus['hospitalnonicu_full_vac'][0]:,.0f}|-|")
        print(f"|**Non_ICU Hosp per mill**|{dfICUHospByVaxStatus['Non_ICU_Hosp_Unvax_PerMillion'][0]:,.2f}|-|{dfICUHospByVaxStatus['Non_ICU_Hosp_Partial_PerMillion'][0]:,.2f}|{dfICUHospByVaxStatus['Non_ICU_Hosp_Fully_PerMillion'][0]:,.2f}|-|")
        print(f"|**Non_ICU Hosp % less risk vs. unvax**|-|-|{(1-dfICUHospByVaxStatus['Non_ICU_Hosp_Partial_PerMillion'][0]/dfICUHospByVaxStatus['Non_ICU_Hosp_Unvax_PerMillion'][0]):,.1%}|{(1-dfICUHospByVaxStatus['Non_ICU_Hosp_Fully_PerMillion'][0]/dfICUHospByVaxStatus['Non_ICU_Hosp_Unvax_PerMillion'][0]):,.1%}|-|")
        print(f"|**Non_ICU Hosp risk vs. full**|{(dfICUHospByVaxStatus['Unvax_Risk_Higher_NonICUHosp'][0]):.2f}x|-|{(dfICUHospByVaxStatus['Partial_Risk_Higher_NonICUHosp'][0]):.2f}x|1.00x|-|")
        print('|||||||')
    else:
        print(f"Case by vaccination status data last published {dfCaseByVaxStatus.index.max():%B %d}")

    Under12DF = pd.read_pickle('Pickle/AgeCaseDF.pickle')

    if VaxStatusCases_AgeDF_Today_Date > EVHelper.datetime_offset_days(TODAYS_DATE_GLOBAL, days=-5):
        print(f"|**Age group per 100k - day - {VaxStatusCases_AgeDF_Today['Date'].max():%B %d}:**||||||")
        # print(f"|**0-4** |{Under12DF.loc[TodaysDate]['0to4']/Population_0to4*100000:.2f}|-|{0:.2f}|{0:.2f}|-|")
        # print(f"|**5-11** |{Under12DF.loc[TodaysDate]['5to11']/Population_5to11*100000:.2f}|-|{0:.2f}|{0:.2f}|-|")
        # print(f"|**0-11** |{VaxStatusCases_AgeDF_Today.loc['0-11yrs']['cases_unvac_rate_per100K']:.2f}|-|{VaxStatusCases_AgeDF_Today.loc['0-11yrs']['cases_partial_vac_rate_per100K']:.2f}|{VaxStatusCases_AgeDF_Today.loc['0-11yrs']['cases_full_vac_rate_per100K']:.2f}|-|")
        # print(f"|**12-17** |{VaxStatusCases_AgeDF_Today.loc['12-17yrs']['cases_unvac_rate_per100K']:.2f}|-|{VaxStatusCases_AgeDF_Today.loc['12-17yrs']['cases_partial_vac_rate_per100K']:.2f}|{VaxStatusCases_AgeDF_Today.loc['12-17yrs']['cases_full_vac_rate_per100K']:.2f}|-|")
        # print(f"|**18-39** |{VaxStatusCases_AgeDF_Today.loc['18-39yrs']['cases_unvac_rate_per100K']:.2f}|-|{VaxStatusCases_AgeDF_Today.loc['18-39yrs']['cases_partial_vac_rate_per100K']:.2f}|{VaxStatusCases_AgeDF_Today.loc['18-39yrs']['cases_full_vac_rate_per100K']:.2f}|-|")
        # print(f"|**40-59** |{VaxStatusCases_AgeDF_Today.loc['40-59yrs']['cases_unvac_rate_per100K']:.2f}|-|{VaxStatusCases_AgeDF_Today.loc['40-59yrs']['cases_partial_vac_rate_per100K']:.2f}|{VaxStatusCases_AgeDF_Today.loc['40-59yrs']['cases_full_vac_rate_per100K']:.2f}|-|")
        # print(f"|**60-79** |{VaxStatusCases_AgeDF_Today.loc['60-79yrs']['cases_unvac_rate_per100K']:.2f}|-|{VaxStatusCases_AgeDF_Today.loc['60-79yrs']['cases_partial_vac_rate_per100K']:.2f}|{VaxStatusCases_AgeDF_Today.loc['60-79yrs']['cases_full_vac_rate_per100K']:.2f}|-|")
        # print(f"|**80+** |{VaxStatusCases_AgeDF_Today.loc['80+']['cases_unvac_rate_per100K']:.2f}|-|{VaxStatusCases_AgeDF_Today.loc['80+']['cases_partial_vac_rate_per100K']:.2f}|{VaxStatusCases_AgeDF_Today.loc['80+']['cases_full_vac_rate_per100K']:.2f}|-|")
        for age in ['0-4yrs', '5-11yrs', '12-17yrs', '18-39yrs', '40-59yrs', '60+']:
            print(f"|**{age}** |{VaxStatusCases_AgeDF_Today.loc[age]['cases_notfull_vac_rate_per100K']:.2f}|-|{VaxStatusCases_AgeDF_Today.loc[age]['cases_full_vac_rate_per100K']:.2f}|{VaxStatusCases_AgeDF_Today.loc[age]['cases_boost_vac_rate_per100K']:.2f}|-|")
    print("")

    """
    print(f"* Today, the per 100k **case** rates for un/partially/fully vaxxed people were **{dfCaseByVaxStatus['Unvax_Per100k_Day'][0]:.2f} / {dfCaseByVaxStatus['Partial_Per100k_Day'][0]:.2f} / {dfCaseByVaxStatus['Fully_Per100k_Day'][0]:.2f}** (Count: {dfCaseByVaxStatus['covid19_cases_unvac'][0]:,.0f} / {dfCaseByVaxStatus['covid19_cases_partial_vac'][0]:,.0f} / {dfCaseByVaxStatus['covid19_cases_full_vac'][0]:,.0f}) ")
    print('* Translated into effectiveness rates, fully/partially vaxxed people are **'+ "{:.1%}".format(1-(dfCaseByVaxStatus['Fully_Per100k_Day'][0]/dfCaseByVaxStatus['Unvax_Per100k_Day'][0]))+' / '+ "{:.1%}".format(1-(dfCaseByVaxStatus['Partial_Per100k_Day'][0]/dfCaseByVaxStatus['Unvax_Per100k_Day'][0]))+'** less likely to get infected than unvaxxed people')
    print('* Translated into effectiveness rates, un/partially vaxxed people are **'+ "{:.1f}".format(dfCaseByVaxStatus['Unvax_Risk_Higher'][0])+'x / '+ "{:.1f}".format(dfCaseByVaxStatus['Partial_Risk_Higher'][0])+'x** more likely to get infected than fully vaxxed people')
    print(f"* Over the **last week**, the per 100k **case** rates for un/partially/fully vaxxed people were {dfCaseByVaxStatus['Unvax_Per100k_Week'][0] :.2f} / {dfCaseByVaxStatus['Partial_Per100k_Week'][0] :.2f} / {dfCaseByVaxStatus['Fully_Per100k_Week'][0] :.2f}")
    print(f"* Translated into effectiveness rates, fully/partially vaxxed people are **{1-(dfCaseByVaxStatus['Fully_Per100k_Week'][0]/dfCaseByVaxStatus['Unvax_Per100k_Week'][0]) :.1%} / {1-(dfCaseByVaxStatus['Partial_Per100k_Week'][0]/dfCaseByVaxStatus['Unvax_Per100k_Week'][0]) :.1%}** less likely to get infected than unvaxxed people")
    print('* Translated into effectiveness rates, un/partially vaxxed people are **'+ "{:.1f}".format(dfCaseByVaxStatus['Unvax_Risk_Higher_Week'][0])+'x / '+ "{:.1f}".format(dfCaseByVaxStatus['Partial_Risk_Higher_Week'][0])+'x** more likely to get infected than fully vaxxed people')
    print('* ')
    print(f"* Today, the per 100k **case** rates for 12+ un/partially/fully vaxxed people were **{dfCaseByVaxStatus['Unvax_Per100k_Day_12Plus'][0]:.2f} / {dfCaseByVaxStatus['Partial_Per100k_Day'][0]:.2f} / {dfCaseByVaxStatus['Fully_Per100k_Day'][0]:.2f}** (Count: {dfCaseByVaxStatus['covid19_cases_unvac_12plus'][0]:,.0f} / {dfCaseByVaxStatus['covid19_cases_partial_vac'][0]:,.0f} / {dfCaseByVaxStatus['covid19_cases_full_vac'][0]:,.0f}) ")
    print('* Translated into effectiveness rates, 12+ fully/partially vaxxed people are **'+ "{:.1%}".format(1-(dfCaseByVaxStatus['Fully_Per100k_Day'][0]/dfCaseByVaxStatus['Unvax_Per100k_Day_12Plus'][0]))+' / '+ "{:.1%}".format(1-(dfCaseByVaxStatus['Partial_Per100k_Day'][0]/dfCaseByVaxStatus['Unvax_Per100k_Day_12Plus'][0]))+'** less likely to get infected than unvaxxed people')
    print('* Translated into effectiveness rates, 12+ un/partially vaxxed people are **'+ "{:.1f}".format(dfCaseByVaxStatus['Unvax_Risk_Higher_12plus'][0])+'x / '+ "{:.1f}".format(dfCaseByVaxStatus['Partial_Risk_Higher'][0])+'x** more likely to get infected than fully vaxxed people')
    print(f"* Over the **last week**, the per 100k **case** rates for 12+ un/partially/fully vaxxed people were {dfCaseByVaxStatus['Unvax_Per100k_Week_12Plus'][0] :.2f} / {dfCaseByVaxStatus['Partial_Per100k_Week'][0] :.2f} / {dfCaseByVaxStatus['Fully_Per100k_Week'][0] :.2f}")
    print(f"* Translated into effectiveness rates, 12+ fully/partially vaxxed people are **{1-(dfCaseByVaxStatus['Fully_Per100k_Week'][0]/dfCaseByVaxStatus['Unvax_Per100k_Week_12Plus'][0]) :.1%} / {1-(dfCaseByVaxStatus['Partial_Per100k_Week'][0]/dfCaseByVaxStatus['Unvax_Per100k_Week_12Plus'][0]) :.1%}** less likely to get infected than unvaxxed people")
    print('* Translated into effectiveness rates, 12+ un/partially vaxxed people are **'+ "{:.1f}".format(dfCaseByVaxStatus['Unvax_Risk_Higher_Week_12Plus'][0])+'x / '+ "{:.1f}".format(dfCaseByVaxStatus['Partial_Risk_Higher_Week'][0])+'x** more likely to get infected than fully vaxxed people')
    print('* ')

    print('* Today, the per million **current ICU** rates for un/partially/fully vaxxed people were **'+"{:.2f}".format(dfICUHospByVaxStatus['ICU_Unvax_PerMillion'][0])+' / '+"{:.2f}".format(dfICUHospByVaxStatus['ICU_Partial_PerMillion'][0])+' / '+"{:.2f}".format(dfICUHospByVaxStatus['ICU_Fully_PerMillion'][0])+"**")
    print('* Translated into effectiveness rates, fully/partially vaxxed people are **'+ "{:.1%}".format(1-(dfICUHospByVaxStatus['ICU_Fully_PerMillion'][0]/dfICUHospByVaxStatus['ICU_Unvax_PerMillion'][0]))+' / '+ "{:.1%}".format(1-(dfICUHospByVaxStatus['ICU_Partial_PerMillion'][0]/dfICUHospByVaxStatus['ICU_Unvax_PerMillion'][0]))+'** less likely to be in the ICU than unvaxxed people')
    print('* Translated into effectiveness rates, un/partially vaxxed people are **'+ "{:.1f}".format(dfICUHospByVaxStatus['Unvax_Risk_Higher_ICU'][0])+'x / '+ "{:.1f}".format(dfICUHospByVaxStatus['Partial_Risk_Higher_ICU'][0])+'x** more likely to be in the ICU than fully vaxxed people')

    print("* Note that this ICU data is not complete because not all ICU patients have vaccination status recorded. Today's ICU total in this database is:",dfICUHospByVaxStatus['ICU_total'][0],'(', dfICUHospByVaxStatus['icu_unvac'][0],'/', dfICUHospByVaxStatus['icu_partial_vac'][0],'/', dfICUHospByVaxStatus['icu_full_vac'][0],') un/part/full vax split')
    """
    print()

    print('**Vaccines - detailed data:** [Source](https://data.ontario.ca/dataset/covid-19-vaccine-data-in-ontario)')
    print()
    # print('* Total administered: ',"{:,}".format(TotalAdministered),
    #        ' (',"{:+,}".format(df['previous_day_doses_administered'][0]),' / ',"{:+,}".format(df['previous_day_doses_administered'][0:7].sum()),
    #        ' in last day/week)', sep='')
    print(f"* Total admin: {TotalAdministered:,.0f} ",
          f"({df['previous_day_total_doses_administered'][0]:+,.0f} / ",
          f"{df['previous_day_total_doses_administered'][0:7].sum():+,.0f} in last day/week) ")

    print(f"* First doses admin: {TotalAtLeastOneDose:,.0f} / ",
          f"({df['FirstDoseInDay'][0]:+,.0f} / {df['FirstDoseInDay'][0:7].sum():+,.0f} in last day/week) ")

    print('* Second doses admin: ', "{:,}".format(TotalTwoDoses),
          ' (', "{:+,}".format(df['SecondDoseInDay'][0]), ' / ',
          "{:+,}".format(df['SecondDoseInDay'][0:7].sum()),
          ' in last day/week)', sep='')

    print(f"* Third doses admin: {df['total_individuals_3doses'][0]:,}",
          f"({df['ThirdDoseInDay'][0]:+,} / {df['ThirdDoseInDay'][0:7].sum():+,} in last day/week) ")

    print(f"* Fourth+ doses admin: {df['fourth_dosed_individuals'][0]:,}",
          f"({df['FourthDoseInDay'][0]:+,} / {df['FourthDoseInDay'][0:7].sum():+,} in last day/week) ")

    # print('* ',"{:.2%}".format(PercentVax_18Plus_AtLeastOne),' / ',"{:.2%}".format(PercentVax_18Plus_BothDose)," of **all adult** Ontarians have received at least one / both dose(s) to date",sep = '')

    FirstDoseTodayPct_All = df['FirstDoseInDay'][0] / OntarioPopulation
    FirstDoseWeekPct_All = df['FirstDoseInDay'][0:7].sum() / OntarioPopulation
    SecondDoseTodayPct_All = df['SecondDoseInDay'][0] / OntarioPopulation
    SecondDoseWeekPct_All = df['SecondDoseInDay'][0:7].sum() / OntarioPopulation
    ThirdDoseTodayPct_All = df['ThirdDoseInDay'][0] / OntarioPopulation
    ThirdDoseWeekPct_All = df['ThirdDoseInDay'][0:7].sum() / OntarioPopulation
    FourthDoseTodayPct_All = df['FourthDoseInDay'][0] / OntarioPopulation
    FourthDoseWeekPct_All = df['FourthDoseInDay'][0:7].sum() / OntarioPopulation

    print(f"* {TotalAtLeastOneDose/OntarioPopulation:.2%} / {TotalTwoDoses/OntarioPopulation:.2%} / {TotalThreeDosed/OntarioPopulation:.2%}",
          f"/ {TotalFourDosed/OntarioPopulation:.2%}",
          " of **all** Ontarians have received at least one / two / three / four doses to date",
          f"({FirstDoseTodayPct_All:.2%} / {SecondDoseTodayPct_All:.2%} / {ThirdDoseTodayPct_All:.2%} / {FourthDoseTodayPct_All:.2%} today)",
          f"({FirstDoseWeekPct_All:.2%} / {SecondDoseWeekPct_All:.2%} / {ThirdDoseWeekPct_All:.2%} / {FourthDoseWeekPct_All:.2%} in last week)")

    FirstDoseTodayPct_5Plus = df['FirstDoseInDay'][0] / Population_5Plus
    FirstDoseWeekPct_5Plus = df['FirstDoseInDay'][0:7].sum() / Population_5Plus
    SecondDoseTodayPct_5Plus = df['SecondDoseInDay'][0] / Population_5Plus
    SecondDoseWeekPct_5Plus = df['SecondDoseInDay'][0:7].sum() / Population_5Plus
    ThirdDoseTodayPct_5Plus = df['ThirdDoseInDay'][0] / Population_5Plus
    ThirdDoseWeekPct_5Plus = df['ThirdDoseInDay'][0:7].sum() / Population_5Plus
    FourthDoseTodayPct_5Plus = df['FourthDoseInDay'][0] / Population_5Plus
    FourthDoseWeekPct_5Plus = df['FourthDoseInDay'][0:7].sum() / Population_5Plus

    print(f"* {TodaysDFVax.loc['Ontario_5plus']['Percent_at_least_one_dose']:.2%} / {TodaysDFVax.loc['Ontario_5plus']['Percent_fully_vaccinated']:.2%} / {TodaysDFVax.loc['Ontario_5plus']['Percent_3doses']:.2%} / {TotalFourDosed/Population_5Plus:.2%}",
          " of **5+** Ontarians have received at least one / two / three / four doses to date",
          f"({FirstDoseTodayPct_5Plus:.2%} / {SecondDoseTodayPct_5Plus:.2%} / {ThirdDoseTodayPct_5Plus:.2%} / {FourthDoseTodayPct_5Plus:.2%} today)",
          f"({FirstDoseWeekPct_5Plus:.2%} / {SecondDoseWeekPct_5Plus:.2%} / {ThirdDoseWeekPct_5Plus:.2%} / {FourthDoseWeekPct_5Plus:.2%} in last week)")

    # print('* ',"{:.2%}".format(TodaysDFVax.loc['Total - eligible 12+']['Percent_at_least_one_dose']),' / ',"{:.2%}".format(TodaysDFVax.loc['Total - eligible 12+']['Percent_fully_vaccinated'])," of **12+** Ontarians have received at least one / both dose(s) to date (",
    #       "{:.2%}".format(TodaysDFVax.loc['Total - eligible 12+']['FirstDose - in last day %']),' / ', "{:.2%}".format(TodaysDFVax.loc['Total - eligible 12+']['SecondDose - in last day %']),' today, ',
    #       "{:.2%}".format(TodaysDFVax.loc['Total - eligible 12+']['FirstDose - in last week %']),' / ',"{:.2%}".format(TodaysDFVax.loc['Total - eligible 12+']['SecondDose - in last week %']),' in last week)',sep = '')

    # print('* ',"{:.2%}".format(TodaysDFVax.loc['Adults_18plus']['Percent_at_least_one_dose']),' / ',"{:.2%}".format(TodaysDFVax.loc['Adults_18plus']['Percent_fully_vaccinated'])," of **18+** Ontarians have received at least one / both dose(s) to date (",
    #       "{:.2%}".format(TodaysDFVax.loc['Adults_18plus']['FirstDose - in last day %']),' / ', "{:.2%}".format(TodaysDFVax.loc['Adults_18plus']['SecondDose - in last day %']),' today, ',
    #       "{:.2%}".format(TodaysDFVax.loc['Adults_18plus']['FirstDose - in last week %']),' / ',"{:.2%}".format(TodaysDFVax.loc['Adults_18plus']['SecondDose - in last week %']),' in last week)',sep = '')

    PercentOfRemaining_First_Day = TodaysDFVax.loc['Total - eligible 12+']['PercentOfRemaining_First - in last day']
    PercentOfRemaining_First_Week = TodaysDFVax.loc['Total - eligible 12+']['PercentOfRemaining_First - in last week']
    print(f"* {PercentOfRemaining_First_Day:.3%} / {PercentOfRemaining_First_Week:.3%} of the **remaining 12+** unvaccinated population got vaccinated today/this week")

    # print("* To deliver at least one/both doses to all **adult** Ontarians by September 30th, ","{:,.0f}".format((OntarioAdultPopulation-TotalAtLeastOneDose)/DaysToSep30),' / ',"{:,.0f}".format(TotalVaccinationsLeft/DaysToSep30),' people need to be vaccinated every day from here on',sep='')
    # print("* To deliver at least one dose to [all **adult** Ontarians by June 20th](https://globalnews.ca/news/7679338/ontario-coronavirus-phase-2-vaccine-rollout/), ","{:,.0f}".format((RemainingAdults_All)/DaysToJun20),' people need to be vaccinated every day from here on',sep='')

    print("* To date, ", "{:,}".format(TotalDelivered),
          " vaccines have been delivered to Ontario (last updated ", DeliveryDataDate,
          ")  - [Source](https://www.canada.ca/en/public-health/services/diseases/2019-novel-coronavirus-infection/prevention-risks/covid-19-vaccine-treatment/vaccine-rollout.html)",
          sep='')
    print("* There are", "{:,}".format(TotalUnused), "unused vaccines which will take",
          round(TotalUnused / AvgDailyVaccinationsWeek, 1),
          "days to administer based on the current 7 day average of",
          "{:,.0f}".format(AvgDailyVaccinationsWeek), '/day')
    # print("* Adults make up", "{:.0%}".format(OntarioAdultPopulation/OntarioPopulation), "of [Ontario's population](https://www.fin.gov.on.ca/en/economy/demographics/projections/#tables)")
    print("* Ontario's population is 14,822,201 as published [here](https://www.fin.gov.on.ca/en/economy/demographics/projections/#tables). Age group populations as provided by the [MOH here](https://data.ontario.ca/dataset/covid-19-vaccine-data-in-ontario/resource/775ca815-5028-4e9b-9dd4-6975ff1be021)")
    print('* Vaccine uptake report (updated weekly) incl. vaccination coverage by PHUs - [link](https://www.publichealthontario.ca/-/media/documents/ncov/epi/covid-19-vaccine-uptake-ontario-epi-summary.pdf?la=en)')

    # print("* Based on this week's vaccination rates, all adult Ontarians will have received at least one dose by",((datetime.datetime.now()+datetime.timedelta(days=DaysToAll)).strftime('%B %#d, %Y')),'-',int(DaysToAll),'days to go at this rate')
    print()
    # print('**Reopening vaccine [metrics](https://news.ontario.ca/en/release/1000161/ontario-releases-three-step-roadmap-to-safely-reopen-the-province) (based on current rates)**')
    print('**Random vaccine stats**')

    print()
    # print('* Step 1 to Step 3 criteria all met ')
    print()
    # print('* Step 3 exit criteria:')
    # print("* Based on this week's vaccination rates, **80% of 12+** Ontarians will have received **at least one** dose by **",((datetime.datetime.now()+datetime.timedelta(days=DaysTo80Pct_First_Eligible)).strftime('%B %#d, %Y')),'** - ',round(DaysTo80Pct_First_Eligible),' days to go ',sep='')
    # print("* **80% of 12+** Ontarians have already received **at least one** dose",sep='')
    # Threshold_75Second_Date = (datetime.datetime.now()+datetime.timedelta(days=DaysTo75Pct_Second_Eligible))
    # Threshold_80Second_Date = (datetime.datetime.now() + datetime.timedelta(days=DaysTo80Pct_Second_Eligible))

    ########################################################
    ########################################################
    Threshold_ThirdDose_Date = (datetime.timedelta(days=((0.60 - TodaysDFVax.loc['Ontario_5plus']['Percent_3doses'])
                                                         / (TodaysDFVax.loc['Ontario_5plus']['ThirdDose - in last week %'] / 7)))
                                + (datetime.datetime.now().replace(hour=9, minute=0, second=0)))
    Threshold_Third_DaysTo = (Threshold_ThirdDose_Date
                              - datetime.datetime.now().replace(hour=0, minute=0, second=0)).days
    print("* Based on this week's vaccination rates, **60% of 5+** Ontarians will have received"
          " **boosters by ", EVHelper.ConvertDayToWorkingDay(Threshold_ThirdDose_Date, 9, 18).strftime('%B %#d, %Y at %H:%M'),
          '** - ', round(Threshold_Third_DaysTo), ' days to go ', sep='')

    # print("* Another projection assumes that second doses will follow the pace of the 1st doses, and therefore will slow down as we approach the 75% number. We crossed today's second dose percentage in first doses on *"+DateFirstDoseWasCurrentSecondDose.strftime('%B %#d, %Y')+"*, and the 75% first dose threshold on *"+DateFirstDoseWas75.strftime('%B %#d, %Y')+"*, **"+str(int(DaysRemaining.days))+" days** later. In this projection, we will reach the 75% second dose threshold on **"+(EVHelper.ConvertDayToWorkingDay((datetime.datetime.now()+DaysRemaining),9,18)).strftime('%B %#d, %Y')+"**")
    # print("* Assuming that second doses will follow the pace of the 1st doses: We crossed today's second dose percentage in first doses on *"+DateFirstDoseWasCurrentSecondDose.strftime('%B %#d, %Y')+"*, and the 85% first dose threshold on *"+DateFirstDoseWas85.strftime('%B %#d, %Y')+"*, **"+str(int(DaysRemaining_85.days))+" days** later. In this projection, we will reach the 85% second dose threshold on **"+(EVHelper.ConvertDayToWorkingDay((datetime.datetime.now()+DaysRemaining_85),9,18)).strftime('%B %#d, %Y')+"**")

    ########################################################
    ########################################################
    # Threshold_First_Date = (datetime.timedelta(days=((0.85 - TodaysDFVax.loc['05-11yrs']['Percent_at_least_one_dose'])
    #                                                  / (TodaysDFVax.loc['05-11yrs']['FirstDose - in last week %'] / 7)))
    #                         + (datetime.datetime.now().replace(hour=9, minute=0, second=0)))
    # Threshold_First_Date_DaysTo = (Threshold_First_Date - datetime.datetime.now().replace(hour=0, minute=0, second=0)).days

    # print("* Based on this week's vaccination rates, **85% of 5-11 year olds+** will have received"
    #       " **at least one** dose by **", EVHelper.ConvertDayToWorkingDay(Threshold_First_Date, 9, 18).strftime('%B %#d, %Y at %H:%M'),
    #       '** - ', round(Threshold_First_Date_DaysTo), ' days to go ', sep='')

    ########################################################
    ########################################################
    """
    if (not sympy.isprime(df['previous_day_total_doses_administered'][0])):
        print('*',"{:,}".format(df['previous_day_total_doses_administered'][0]),'is NOT a prime number but it is',(sympy.nextprime(df['previous_day_total_doses_administered'][0])-df['previous_day_total_doses_administered'][0]),'lower than the next prime number and',(df['previous_day_total_doses_administered'][0]-sympy.prevprime(df['previous_day_total_doses_administered'][0])),'higher than the previous prime number. The prime factorization of this is',
              str(sympy.factorint(df['previous_day_total_doses_administered'][0])).replace(':','^').replace(' ','').replace(',',', '))
        print(f"* The last date we had a prime number of doses was {df[df['DayVaccineCount_IsPrime']==True].iloc[0].name.strftime('%B %#d')} ({(datetime.datetime.today()-df[df['DayVaccineCount_IsPrime']==True].iloc[0].name).days} days ago), when we had {df[df['DayVaccineCount_IsPrime']==True].iloc[0].previous_day_total_doses_administered :,.0f} doses")
              #f" Between the lowest and highest vaccine counts this week, {len(list(sympy.primerange(MinVax_Week,MaxVax_Week)))/(MaxVax_Week-MinVax_Week):.2%} of numbers are prime"

    else:
        print('* There were a prime number of vaccine doses today!!')

    MinVax_Week = df[0:7]['previous_day_total_doses_administered'].min()
    MaxVax_Week =df[0:7]['previous_day_total_doses_administered'].max()
    print(f"* To date, we have had {len(df[df['DayVaccineCount_IsPrime']]) :,.0f} prime daily vaccine counts,"
          f" ({len(df[df['DayVaccineCount_IsPrime']])/len(df):.2%} of the total vaccine count days)."
          f" Between the lowest and highest vaccine counts this week, {len(list(sympy.primerange(MinVax_Week,MaxVax_Week)))/(MaxVax_Week-MinVax_Week):.2%} of numbers are prime"
          )
    """
    ########################################################
    ########################################################

    print('')
    print()

    sys.stdout = ConsoleOut

    TodaysDFVax = TodaysDFVax.replace([np.inf, -np.inf], 0)

    TodaysDFVax['FirstDose - in last day'] = TodaysDFVax['FirstDose - in last day'].map('{:,.0f}'.format)
    TodaysDFVax['FirstDose - in last week'] = TodaysDFVax['FirstDose - in last week'].map('{:,.0f}'.format)

    TodaysDFVax['SecondDose - in last day'] = TodaysDFVax['SecondDose - in last day'].map('{:,.0f}'.format)
    TodaysDFVax['SecondDose - in last week'] = TodaysDFVax['SecondDose - in last week'].map('{:,.0f}'.format)

    TodaysDFVax['ThirdDose - in last day'] = TodaysDFVax['ThirdDose - in last day'].map('{:,.0f}'.format)
    TodaysDFVax['ThirdDose - in last week'] = TodaysDFVax['ThirdDose - in last week'].map('{:,.0f}'.format)

    TodaysDFVax['FirstDose - in last day %'] = TodaysDFVax['FirstDose - in last day %'].map('{:+.2%}'.format)
    TodaysDFVax['FirstDose - in last week %'] = TodaysDFVax['FirstDose - in last week %'].map('{:+.2%}'.format)
    TodaysDFVax['SecondDose - in last day %'] = TodaysDFVax['SecondDose - in last day %'].map('{:+.2%}'.format)
    TodaysDFVax['SecondDose - in last week %'] = TodaysDFVax['SecondDose - in last week %'].map('{:+.2%}'.format)
    TodaysDFVax['ThirdDose - in last day %'] = TodaysDFVax['ThirdDose - in last day %'].map('{:+.2%}'.format)
    TodaysDFVax['ThirdDose - in last week %'] = TodaysDFVax['ThirdDose - in last week %'].map('{:+.2%}'.format)

    TodaysDFVax['Percent_at_least_one_dose'] = TodaysDFVax['Percent_at_least_one_dose'].map('{:.2%}'.format)
    TodaysDFVax['Percent_fully_vaccinated'] = TodaysDFVax['Percent_fully_vaccinated'].map('{:.2%}'.format)
    TodaysDFVax['Percent_3doses'] = TodaysDFVax['Percent_3doses'].map('{:.2%}'.format)

    TodaysDFVax['First dose % (day/week)'] = (TodaysDFVax['Percent_at_least_one_dose'].astype(str)
                                              + ' (' + TodaysDFVax['FirstDose - in last day %'].astype(str)
                                              + ' / ' + TodaysDFVax['FirstDose - in last week %'].astype(str) + ')')
    TodaysDFVax['Second dose % (day/week)'] = (TodaysDFVax['Percent_fully_vaccinated'].astype(str) + ' ('
                                               + TodaysDFVax['SecondDose - in last day %'].astype(str)
                                               + ' / ' + TodaysDFVax['SecondDose - in last week %'].astype(str) + ')')
    TodaysDFVax['Third dose % (day/week)'] = (TodaysDFVax['Percent_3doses'].astype(str) + ' ('
                                              + TodaysDFVax['ThirdDose - in last day %'].astype(str)
                                              + ' / ' + TodaysDFVax['ThirdDose - in last week %'].astype(str) + ')')

    # TodaysDFVax = TodaysDFVax.drop('Date', axis=1)

    TodaysDFVax = TodaysDFVax[['FirstDose - in last day', 'SecondDose - in last day', 'ThirdDose - in last day',
                               'First dose % (day/week)', 'Second dose % (day/week)', 'Third dose % (day/week)',
                               ]]

    xyz_18plus = TodaysDFVax.loc['Adults_18plus']
    xyz_18plus.name = 'Total - 18+'
    xyz_12plus = TodaysDFVax.loc['Ontario_12plus']
    xyz_12plus.name = 'Total - 12+'
    TodaysDFVax = TodaysDFVax.drop('Adults_18plus')
    TodaysDFVax = TodaysDFVax.drop('Ontario_12plus')
    TodaysDFVax = TodaysDFVax.drop('Total - eligible 12+')
    xyz_5plus = TodaysDFVax.loc['Ontario_5plus']
    xyz_5plus.name = 'Total - 5+'

    TodaysDFVax = TodaysDFVax.drop('Ontario_5plus')

    TodaysDFVax = TodaysDFVax.T
    TodaysDFVax.insert(10, ' ', '')
    TodaysDFVax = TodaysDFVax.T
    TodaysDFVax = TodaysDFVax.append(xyz_18plus)
    TodaysDFVax = TodaysDFVax.append(xyz_12plus)
    TodaysDFVax = TodaysDFVax.append(xyz_5plus)

    AgeGroupPopDF = pd.read_pickle('Pickle/AgeGroupPopDF.pickle')
    # AgeGroupPopDF = AgeGroupPopDF.drop('0to4')
    AgeGroupPopDF.index = AgeGroupPopDF.index.str.replace('to', '-')
    AgeGroupPopDF.index = AgeGroupPopDF.index + 'yrs'

    ChangeInCasesByAge = pd.read_pickle('PickleNew/ChangeInCasesByAge.pickle')
    CaseByAge_DailyReport = pd.read_pickle('Pickle/AgeCaseDF.pickle')
    New_Row = {
        '0-4yrs': CaseByAge_DailyReport['0to4'][0],
        '05-11yrs': CaseByAge_DailyReport['5to11'][0],
        '12-17yrs': CaseByAge_DailyReport['12to19'][0],
        '18-29yrs': ChangeInCasesByAge.iloc[:, 1]['20s'],
        '30-39yrs': ChangeInCasesByAge.iloc[:, 1]['30s'],
        '40-49yrs': ChangeInCasesByAge.iloc[:, 1]['40s'],
        '50-59yrs': ChangeInCasesByAge.iloc[:, 1]['50s'],
        '60-69yrs': ChangeInCasesByAge.iloc[:, 1]['60s'],
        '70-79yrs': ChangeInCasesByAge.iloc[:, 1]['70s'],
        '80+ yrs': ChangeInCasesByAge.iloc[:, 1]['80s'] + ChangeInCasesByAge.iloc[:, 1]['90+']
    }
    NewCases_AgeDF = pd.DataFrame(New_Row, index=['Cases']).T
    CasesPer_100k = NewCases_AgeDF['Cases'] / AgeGroupPopDF['Population'] * 100000
    CasesPer_100k = pd.DataFrame(CasesPer_100k, columns=['CasesPer100k - Day']).round(1)
    TodaysDFVax = TodaysDFVax.merge(pd.DataFrame(CasesPer_100k), how='left',
                                    left_index=True, right_index=True)
    TodaysDFVax['CasesPer100k - Day'] = TodaysDFVax['CasesPer100k - Day'].fillna('')
    c = TodaysDFVax.pop('CasesPer100k - Day')
    TodaysDFVax.insert(0, c.name, c)
    TodaysDFVax.to_pickle('Pickle/TodaysDFVax.pickle')

    # TodaysDFVax = TodaysDFVax.append(xyz_5plus)
    # TodaysDFVax.rename(index={'Unknown': 'Unknown and 5-11'}, inplace=True)
    # TodaysDFVax.to_csv('aaa.csv')

    with open('TextOutput/VaccineAgeTable.txt', 'w', newline='') as f:
        f.write('**Vaccine data (by age)** - Charts of [first doses]() and [second doses]() \n\n')

        f.write('**Age**|**Cases/100k**|**First doses**|**Second doses**|**Third doses**|**First Dose % (day/week)**|**Second Dose % (day/week)**|**Third Dose % (day/week)**')
        f.write('\n')
        f.write(':--|:-:|:-:|:-:|:-:|:-:|:-:|:-:|')
        f.write('\n')
        TodaysDFVax.to_csv(f, header=False, sep='|')
        f.write('\n')

    FirstDosePivot = pd.pivot_table(dfVaxAge, values='Percent_at_least_one_dose',
                                    columns='Date', index='Agegroup')
    FirstDosePivot = FirstDosePivot.reindex(columns=sorted(FirstDosePivot.columns,
                                                           reverse=True))
    FirstDosePivot = FirstDosePivot.round(4)
    SecondDosePivot = pd.pivot_table(dfVaxAge, values='Percent_fully_vaccinated',
                                     columns='Date', index='Agegroup')
    SecondDosePivot = SecondDosePivot.reindex(columns=sorted(SecondDosePivot.columns,
                                                             reverse=True))
    SecondDosePivot = SecondDosePivot.round(4)

    FirstDoseCountPivot = pd.pivot_table(dfVaxAge, values='FirstDose - in last day',
                                         columns='Date', index='Agegroup')
    FirstDoseCountPivot = FirstDoseCountPivot.reindex(columns=sorted(FirstDoseCountPivot.columns,
                                                                     reverse=True))

    SecondDoseCountPivot = pd.pivot_table(dfVaxAge, values='SecondDose - in last day',
                                          columns='Date', index='Agegroup')
    SecondDoseCountPivot = SecondDoseCountPivot.reindex(columns=sorted(SecondDoseCountPivot.columns,
                                                                       reverse=True))

    # with open('PivotTable.csv', 'a',newline='') as f:
    #     f.write('\n')
    #     f.write('Vaccines - 1st dose by day \n')
    #     FirstDosePivot.to_csv(f,header = True)
    #     f.write('\n')

    #     f.write('Vaccines - 2nd dose by day \n')
    #     SecondDosePivot.to_csv(f,header = True)
    #     f.write('\n')

    #     f.write('Vaccines - 1st dose count by day \n')
    #     FirstDoseCountPivot.to_csv(f,header = True)
    #     f.write('\n')

    #     f.write('Vaccines - 2nd dose count by day \n')
    #     SecondDoseCountPivot.to_csv(f,header = True)
    #     f.write('\n')

    #     f.write('Cases By Vax Status \n')
    #     dfCaseByVaxStatus.T.to_csv(f)
    #     f.write('\n')

    #     f.write('ICU By Vax Status \n')
    #     dfICUHospByVaxStatus.T.to_csv(f)

    FirstDosePivot.to_pickle('PickleNew/FirstDosePivot-Display.pickle')
    SecondDosePivot.to_pickle('PickleNew/SecondDosePivot-Display.pickle')
    FirstDoseCountPivot.to_pickle('PickleNew/FirstDoseCountPivot-Display.pickle')
    SecondDoseCountPivot.to_pickle('PickleNew/SecondDoseCountPivot-Display.pickle')
    dfCaseByVaxStatus.to_pickle('PickleNew/dfCaseByVaxStatus-Display.pickle')
    dfICUHospByVaxStatus.to_pickle('PickleNew/dfICUHospByVaxStatus-Display.pickle')

    del FirstDosePivot, SecondDosePivot, FirstDoseCountPivot, SecondDoseCountPivot

    print('Report as of: ', df.iloc[0].name.strftime('%B %d'))
    print('Vaccine Data: ', round(time.time() - starttime, 2), 'seconds')
    print('------------------------------------------------------------------------')


def VaccineData_CaseStatus(download=True):
    starttime = time.time()
    print('------------------------------------------------------------------------')
    print(f'VaccineData_CaseStatus \nStarted: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}')
    df = pd.read_pickle('Pickle/VaccinesData.pickle')
    picklefilename = 'Pickle/CasesByVaxStatus.pickle'
    OntarioPopulationDF = pd.read_pickle('Pickle/OntarioPopulation.pickle')
    OntarioPopulation = (int)(OntarioPopulationDF.loc['REFERENCE', 2021, 'TOTAL'].sum())

    if download:
        dfCaseByVaxStatus = pd.read_csv('https://data.ontario.ca/dataset/752ce2b7-c15a-4965-a3dc-397bf405e7cc/resource/eed63cf2-83dd-4598-b337-b288c0a89a16/download/vac_status.csv')
        dfCaseByVaxStatus.to_csv('SourceFiles/VaccineData_CaseStatus-vac_status.csv')
        dfCaseByVaxStatus['date'] = pd.to_datetime(dfCaseByVaxStatus['Date'])
        dfCaseByVaxStatus = dfCaseByVaxStatus.set_index('date')

        dfCaseByVaxStatus = dfCaseByVaxStatus.fillna(0)
        dfCaseByVaxStatus = dfCaseByVaxStatus.sort_index(ascending=False)

        Under12DF = pd.read_csv('Pickle/AgeData.csv')
        Under12DF['Date'] = Under12DF['Date'] = pd.to_datetime(Under12DF['Date'])
        Under12DF.set_index('Date', inplace=True)
        Under12DF['Total_U12'] = Under12DF['0to4'] + Under12DF['5to11']
        Under12DF['Total_U5'] = Under12DF['0to4']
        Under12DF.to_pickle('Pickle/AgeCaseDF.pickle')
        dfCaseByVaxStatus = dfCaseByVaxStatus.merge(Under12DF['Total_U5'],
                                                    left_index=True, right_index=True, how='left')
        dfCaseByVaxStatus.pop('Date')

        dfCaseByVaxStatus['AllCases_Today'] = 0
        for column in ['covid19_cases_unvac', 'covid19_cases_partial_vac',
                       'covid19_cases_full_vac', 'covid19_cases_vac_unknown',
                       'covid19_cases_notfull_vac', 'covid19_cases_boost_vac'
                       ]:
            dfCaseByVaxStatus['AllCases_Today'] = (dfCaseByVaxStatus['AllCases_Today']
                                                   + dfCaseByVaxStatus[column])
    else:
        dfCaseByVaxStatus = pd.read_pickle(picklefilename)


    dfCaseByVaxStatus['AllCases_Today_5Plus'] = (dfCaseByVaxStatus['AllCases_Today']
                                                 - dfCaseByVaxStatus['Total_U5'])
    dfCaseByVaxStatus['covid19_cases_unvac_5plus'] = (dfCaseByVaxStatus['covid19_cases_unvac']
                                                      - dfCaseByVaxStatus['Total_U5'])
    dfCaseByVaxStatus['covid19_cases_notfull_5plus'] = (dfCaseByVaxStatus['covid19_cases_notfull_vac']
                                                      - dfCaseByVaxStatus['Total_U5'])

    dfCaseByVaxStatus['Unvax_Pop_14DaysAgo'] = df.shift(-14)['Unvax_Count']
    dfCaseByVaxStatus['Partial_Pop_14DaysAgo'] = df.shift(-14)['PartialVax_Count']
    dfCaseByVaxStatus['Fully_Pop_14DaysAgo'] = df.shift(-14)['TotalTwoDosed']
    dfCaseByVaxStatus['not_fullvax_pop_14DaysAgo'] = df.shift(-14)['not_fullvax_count']
    dfCaseByVaxStatus['boosted_14DaysAgo'] = df.shift(-14)['boosted_count']

    dfCaseByVaxStatus = dfCaseByVaxStatus.fillna(method='bfill')
    OntarioPopulation_U5 = OntarioPopulationDF.loc['REFERENCE', 2021, 'TOTAL'][0:5].sum()
    dfCaseByVaxStatus['Unvax_Pop_14DaysAgo_5plus'] = (dfCaseByVaxStatus['Unvax_Pop_14DaysAgo']
                                                      - OntarioPopulation_U5)
    dfCaseByVaxStatus['not_fullvax_pop14DaysAgo_5plus'] = (dfCaseByVaxStatus['not_fullvax_pop_14DaysAgo']
                                                           - OntarioPopulation_U5)


    dfCaseByVaxStatus['Unvax_Per100k_Day'] = (dfCaseByVaxStatus['covid19_cases_unvac']
                                              / dfCaseByVaxStatus['Unvax_Pop_14DaysAgo']) * 100000

    dfCaseByVaxStatus['Unvax_Per100k_Day_5Plus'] = ((dfCaseByVaxStatus['covid19_cases_unvac']
                                                     - dfCaseByVaxStatus['Total_U5'])
                                                    / dfCaseByVaxStatus['Unvax_Pop_14DaysAgo_5plus'] * 100000)

    dfCaseByVaxStatus['Partial_Per100k_Day'] = (dfCaseByVaxStatus['covid19_cases_partial_vac']
                                                / dfCaseByVaxStatus['Partial_Pop_14DaysAgo']) * 100000
    dfCaseByVaxStatus['Fully_Per100k_Day'] = (dfCaseByVaxStatus['covid19_cases_full_vac']
                                              / (dfCaseByVaxStatus['Fully_Pop_14DaysAgo']
                                                 - dfCaseByVaxStatus['boosted_14DaysAgo']) ) * 10**5
    dfCaseByVaxStatus['All_Per100k_Day'] = (dfCaseByVaxStatus['AllCases_Today']
                                            / OntarioPopulation * 10**5)
    dfCaseByVaxStatus['NotFullVax_Per100k_Day'] = (dfCaseByVaxStatus['covid19_cases_notfull_vac']
                                          / dfCaseByVaxStatus['not_fullvax_pop_14DaysAgo']) * 10**5
    dfCaseByVaxStatus['NotFullVax_Per100k_Day_5Plus'] = ((dfCaseByVaxStatus['covid19_cases_notfull_vac'] - dfCaseByVaxStatus['Total_U5'])
                                          / dfCaseByVaxStatus['not_fullvax_pop14DaysAgo_5plus']) * 10**5

    dfCaseByVaxStatus['Boosted_Per100k_Day'] = (dfCaseByVaxStatus['covid19_cases_boost_vac']
                                      / dfCaseByVaxStatus['boosted_14DaysAgo']) * 10**5

    # Calculation of risk lower vs. fully. Replaced by calculation of risk lower vs. boost vaxxed
    dfCaseByVaxStatus['Unvax_Risk_Higher'] = (dfCaseByVaxStatus['Unvax_Per100k_Day']
                                              / dfCaseByVaxStatus['Fully_Per100k_Day'])
    dfCaseByVaxStatus['Unvax_Risk_Higher_5plus'] = (dfCaseByVaxStatus['Unvax_Per100k_Day_5Plus']
                                                    / dfCaseByVaxStatus['Fully_Per100k_Day'])
    dfCaseByVaxStatus['Partial_Risk_Higher'] = (dfCaseByVaxStatus['Partial_Per100k_Day']
                                                / dfCaseByVaxStatus['Fully_Per100k_Day'])
    dfCaseByVaxStatus['NotFullVax_Risk_Higher'] = (dfCaseByVaxStatus['NotFullVax_Per100k_Day']
                                                / dfCaseByVaxStatus['Boosted_Per100k_Day'])
    dfCaseByVaxStatus['NotFullVax_Risk_Higher_5Plus'] = (dfCaseByVaxStatus['NotFullVax_Per100k_Day_5Plus']
                                                / dfCaseByVaxStatus['Boosted_Per100k_Day'])
    dfCaseByVaxStatus['FullVax_Risk_Higher'] = (dfCaseByVaxStatus['NotFullVax_Per100k_Day']
                                                / dfCaseByVaxStatus['Boosted_Per100k_Day'])


    # Calculation of risk lower vs. unvaxxed. Replaced by calculation of risk lower vs. not fully vaxxed
    # dfCaseByVaxStatus['FullyVax_Risk_Lower_Day'] = 1 - (dfCaseByVaxStatus['Fully_Per100k_Day']
    #                                                    / dfCaseByVaxStatus['Unvax_Per100k_Day_5Plus'])
    # dfCaseByVaxStatus['PartialVax_Risk_Lower_Day'] = 1 - (dfCaseByVaxStatus['Partial_Per100k_Day']
    #                                                       / dfCaseByVaxStatus['Unvax_Per100k_Day_5Plus'])
    dfCaseByVaxStatus['FullyVax_Risk_Lower_Day'] = 1 - (dfCaseByVaxStatus['Fully_Per100k_Day']
                                                          / dfCaseByVaxStatus['NotFullVax_Per100k_Day_5Plus'])
    dfCaseByVaxStatus['Boosted_Risk_Lower_Day'] = 1 - (dfCaseByVaxStatus['Boosted_Per100k_Day']
                                                          / dfCaseByVaxStatus['NotFullVax_Per100k_Day_5Plus'])

    dfCaseByVaxStatus.sort_index(ascending=True, inplace=True)

    dfCaseByVaxStatus['Unvax_Per100k_Week'] = (dfCaseByVaxStatus['covid19_cases_unvac'].rolling(7).mean()
                                               / dfCaseByVaxStatus['Unvax_Pop_14DaysAgo'].rolling(7).mean() * 100000)
    dfCaseByVaxStatus['Unvax_Per100k_Week_5Plus'] = (dfCaseByVaxStatus['covid19_cases_unvac_5plus'].rolling(7).mean()
                                                     / dfCaseByVaxStatus['Unvax_Pop_14DaysAgo_5plus'].rolling(7).mean() * 100000)

    dfCaseByVaxStatus['Partial_Per100k_Week'] = (dfCaseByVaxStatus['covid19_cases_partial_vac'].rolling(7).mean()
                                                 / dfCaseByVaxStatus['Partial_Pop_14DaysAgo'].rolling(7).mean()) * 100000
    dfCaseByVaxStatus['Fully_Per100k_Week'] = (dfCaseByVaxStatus['covid19_cases_full_vac'].rolling(7).mean()
                                               / (dfCaseByVaxStatus['Fully_Pop_14DaysAgo'] -
                                                  dfCaseByVaxStatus['boosted_14DaysAgo']).rolling(7).mean()) * 100000
    dfCaseByVaxStatus['All_Per100k_Week'] = (dfCaseByVaxStatus['AllCases_Today'].rolling(7).mean()
                                             / OntarioPopulation) * 100000
    dfCaseByVaxStatus['NotFullVax_Per100k_Week'] = (dfCaseByVaxStatus['covid19_cases_notfull_vac'].rolling(7).mean()
                                               / dfCaseByVaxStatus['not_fullvax_pop_14DaysAgo'].rolling(7).mean() * 10**5)
    dfCaseByVaxStatus['NotFullVax_Per100k_Week_5Plus'] = (dfCaseByVaxStatus['covid19_cases_notfull_5plus'].rolling(7).mean()
                                               / dfCaseByVaxStatus['not_fullvax_pop14DaysAgo_5plus'].rolling(7).mean() * 10**5)

    dfCaseByVaxStatus['Boosted_Per100k_Week'] = (dfCaseByVaxStatus['covid19_cases_boost_vac'].rolling(7).mean()
                                               / dfCaseByVaxStatus['boosted_14DaysAgo'].rolling(7).mean() * 10**5)


    dfCaseByVaxStatus['Unvax_Risk_Higher_Week'] = (dfCaseByVaxStatus['Unvax_Per100k_Week']
                                                   / dfCaseByVaxStatus['Fully_Per100k_Week'])
    dfCaseByVaxStatus['Unvax_Risk_Higher_Week_5Plus'] = (dfCaseByVaxStatus['Unvax_Per100k_Week_5Plus']
                                                         / dfCaseByVaxStatus['Fully_Per100k_Week'])
    dfCaseByVaxStatus['Partial_Risk_Higher_Week'] = (dfCaseByVaxStatus['Partial_Per100k_Week']
                                                     / dfCaseByVaxStatus['Fully_Per100k_Week'])
    dfCaseByVaxStatus['NotFullVax_Risk_Higher_Week'] = (dfCaseByVaxStatus['NotFullVax_Per100k_Week']
                                                     / dfCaseByVaxStatus['Boosted_Per100k_Week'])
    dfCaseByVaxStatus['NotFullVax_Risk_Higher_Week_5Plus'] = (dfCaseByVaxStatus['NotFullVax_Per100k_Week_5Plus']
                                                     / dfCaseByVaxStatus['Boosted_Per100k_Week'])

    dfCaseByVaxStatus['FullVax_Risk_Higher_Week'] = (dfCaseByVaxStatus['Fully_Per100k_Week']
                                                     / dfCaseByVaxStatus['Boosted_Per100k_Week'])

    dfCaseByVaxStatus['FullyVax_Risk_Lower_Week'] = 1 - (dfCaseByVaxStatus['Fully_Per100k_Week']
                                                         / dfCaseByVaxStatus['Unvax_Per100k_Week_5Plus'])
    dfCaseByVaxStatus['PartialVax_Risk_Lower_Week'] = 1 - (dfCaseByVaxStatus['Partial_Per100k_Week']
                                                           / dfCaseByVaxStatus['Unvax_Per100k_Week_5Plus'])
    dfCaseByVaxStatus['FullyVax_Risk_Lower_Week'] = 1 - (dfCaseByVaxStatus['Fully_Per100k_Week']
                                                         / dfCaseByVaxStatus['NotFullVax_Per100k_Week_5Plus'])
    dfCaseByVaxStatus['Boosted_Risk_Lower_Week'] = 1 - (dfCaseByVaxStatus['Boosted_Per100k_Week']
                                                         / dfCaseByVaxStatus['NotFullVax_Per100k_Week_5Plus'])



    dfCaseByVaxStatus.sort_index(ascending=False, inplace=True)
    dfCaseByVaxStatus = dfCaseByVaxStatus.round(3)
    dfCaseByVaxStatus.to_pickle(picklefilename)
    print(f'Ended:   {datetime.datetime.now():%Y-%m-%d %H:%M:%S} {round(time.time() - starttime, 2)} seconds')
    print('------------------------------------------------------------------------')


def VaccineData_PHU(download=True):
    starttime = time.time()
    print('------------------------------------------------------------------------')
    print(f'VaccineData_PHU \nStarted: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}')

    VaxDF_PHU = pd.read_csv('https://data.ontario.ca/dataset/752ce2b7-c15a-4965-a3dc-397bf405e7cc/resource/2a362139-b782-43b1-b3cb-078a2ef19524/download/vaccines_by_age_phu.csv')
    VaxDF_PHU.to_csv('SourceFiles/VaccineData-vaccines_by_age_phu.csv')
    VaxDF_PHU['Date'] = pd.to_datetime(VaxDF_PHU['Date'], format='%Y-%m-%d')
    VaxDF_PHU['PHU name'] = VaxDF_PHU['PHU name'].str.title()
    VaxDF_PHU = PHUNameReplacements(VaxDF_PHU)

    VaxDF_PHU['PHU name'] = '**' + VaxDF_PHU['PHU name'] + '**'

    # VaxDF_PHU['PHU name'] = VaxDF_PHU['PHU name'].str.title()
    VaxDF_PHU.set_index('PHU name', inplace=True)
    VaxDF_PHU = VaxDF_PHU.sort_values(by='Date', ascending=False)
    VaxDF_PHU['Percent_at_least_one_dose'] = VaxDF_PHU['Percent_at_least_one_dose'] * 1
    VaxDF_PHU['Percent_fully_vaccinated'] = VaxDF_PHU['Percent_fully_vaccinated'] * 1
    VaxDF_PHU['Percent_3doses'] = VaxDF_PHU['Percent_3doses'] * 1

    VaxDF_PHU_Today = VaxDF_PHU[VaxDF_PHU['Date'] == VaxDF_PHU['Date'].max()]
    VaxDF_PHU_WkAgo = VaxDF_PHU[VaxDF_PHU['Date'] == (VaxDF_PHU['Date'].max()
                                                      - datetime.timedelta(days=7))]
    DisplayDF_VaxPHU = pd.DataFrame()

    VaxDF_PHU.to_pickle('PickleNew/VaxPHU.pickle')

    for age in set(VaxDF_PHU_Today['Agegroup']):
        DisplayDF_VaxPHU['AtLeastOne ' + age] = VaxDF_PHU_Today[VaxDF_PHU_Today['Agegroup'] == age]['Percent_at_least_one_dose']
        DisplayDF_VaxPHU['Both ' + age] = VaxDF_PHU_Today[VaxDF_PHU_Today['Agegroup'] == age]['Percent_fully_vaccinated']
        DisplayDF_VaxPHU['Third ' + age] = VaxDF_PHU_Today[VaxDF_PHU_Today['Agegroup'] == age]['Percent_3doses']

        DisplayDF_VaxPHU['AtLeastOne ' + age + '_WeekAgo'] = VaxDF_PHU_WkAgo[VaxDF_PHU_WkAgo['Agegroup'] == age]['Percent_at_least_one_dose']
        DisplayDF_VaxPHU['Both ' + age + '_WeekAgo'] = VaxDF_PHU_WkAgo[VaxDF_PHU_WkAgo['Agegroup'] == age]['Percent_fully_vaccinated']
        DisplayDF_VaxPHU['Third ' + age + '_WeekAgo'] = VaxDF_PHU_WkAgo[VaxDF_PHU_WkAgo['Agegroup'] == age]['Percent_3doses']
        DisplayDF_VaxPHU['AtLeastOne ' + age + '_Change'] = (DisplayDF_VaxPHU['AtLeastOne ' + age]
                                                             - DisplayDF_VaxPHU['AtLeastOne ' + age + '_WeekAgo']).map('{:+.1%}'.format)
        DisplayDF_VaxPHU['Both ' + age + '_Change'] = (DisplayDF_VaxPHU['Both ' + age]
                                                       - DisplayDF_VaxPHU['Both ' + age + '_WeekAgo']).map('{:+.1%}'.format)
        DisplayDF_VaxPHU['Third ' + age + '_Change'] = (DisplayDF_VaxPHU['Third ' + age]
                                                        - DisplayDF_VaxPHU['Third ' + age + '_WeekAgo']).map('{:+.1%}'.format)

        DisplayDF_VaxPHU['AtLeastOne ' + age + '_Text'] = DisplayDF_VaxPHU['AtLeastOne ' + age].map('{:.1%}'.format)
        DisplayDF_VaxPHU['Both ' + age + '_Text'] = DisplayDF_VaxPHU['Both ' + age].map('{:.1%}'.format)
        DisplayDF_VaxPHU['Third ' + age + '_Text'] = DisplayDF_VaxPHU['Third ' + age].map('{:.1%}'.format)

        # DisplayDF_VaxPHU[age] = (DisplayDF_VaxPHU['AtLeastOne ' + age + '_Text'].astype(str)
        #                          + '/' + DisplayDF_VaxPHU['Both ' + age + '_Text']
        #                          + ' *(' + DisplayDF_VaxPHU['AtLeastOne ' + age + '_Change'] + '/'
        #                          + DisplayDF_VaxPHU['Both ' + age + '_Change'] + ')*')

        DisplayDF_VaxPHU[age] = (DisplayDF_VaxPHU['Both ' + age + '_Text'].astype(str)
                                 + '/' + DisplayDF_VaxPHU['Third ' + age + '_Text']
                                 + ' *(' + DisplayDF_VaxPHU['Both ' + age + '_Change'] + '/'
                                 + DisplayDF_VaxPHU['Third ' + age + '_Change'] + ')*')

        DisplayDF_VaxPHU = DisplayDF_VaxPHU.drop('AtLeastOne ' + age, axis=1)
        DisplayDF_VaxPHU = DisplayDF_VaxPHU.drop('Both ' + age, axis=1)
        DisplayDF_VaxPHU = DisplayDF_VaxPHU.drop('Third ' + age, axis=1)
        DisplayDF_VaxPHU = DisplayDF_VaxPHU.drop('AtLeastOne ' + age + '_Text', axis=1)
        DisplayDF_VaxPHU = DisplayDF_VaxPHU.drop('Both ' + age + '_Text', axis=1)
        DisplayDF_VaxPHU = DisplayDF_VaxPHU.drop('Third ' + age + '_Text', axis=1)
        DisplayDF_VaxPHU = DisplayDF_VaxPHU.drop('AtLeastOne ' + age + '_WeekAgo', axis=1)
        DisplayDF_VaxPHU = DisplayDF_VaxPHU.drop('Both ' + age + '_WeekAgo', axis=1)
        DisplayDF_VaxPHU = DisplayDF_VaxPHU.drop('Third ' + age + '_WeekAgo', axis=1)
        DisplayDF_VaxPHU = DisplayDF_VaxPHU.drop('AtLeastOne ' + age + '_Change', axis=1)
        DisplayDF_VaxPHU = DisplayDF_VaxPHU.drop('Both ' + age + '_Change', axis=1)
        DisplayDF_VaxPHU = DisplayDF_VaxPHU.drop('Third ' + age + '_Change', axis=1)

    DisplayDF_VaxPHU = DisplayDF_VaxPHU.fillna(0)
    # DisplayDF_VaxPHU = VaxDF_PHU_Today[VaxDF_PHU_Today['Agegroup']=='Ontario_12plus'][['Percent_at_least_one_dose','Percent_fully_vaccinated']]
    DisplayDF_VaxPHU = DisplayDF_VaxPHU.sort_values(by='Ontario_5plus', ascending=False)
    DisplayDF_VaxPHU = DisplayDF_VaxPHU.drop('**Unknown**')
    DisplayDF_VaxPHU = DisplayDF_VaxPHU.drop('Undisclosed_or_missing', axis=1)
    DisplayDF_VaxPHU = DisplayDF_VaxPHU.reindex(columns=sorted(DisplayDF_VaxPHU.columns, reverse=False))

    # DisplayDF_VaxPHU.insert(0,'Adults_18plus',DisplayDF_VaxPHU.pop('Adults_18plus'))
    DisplayDF_VaxPHU.insert(0, 'Ontario_12plus', DisplayDF_VaxPHU.pop('Ontario_12plus'))
    DisplayDF_VaxPHU.insert(0, 'Ontario_5plus', DisplayDF_VaxPHU.pop('Ontario_5plus'))

    pd.pivot(VaxDF_PHU_Today, columns='Agegroup').to_pickle('Pickle/VaccinesData_PHU.pickle')
    # pd.pivot(VaxDF_PHU_Today, columns='Agegroup').to_csv('Pickle/VaccinesData_PHU.csv')

    # DisplayDF_VaxPHU.to_pickle('Pickle/VaccinesData_PHU.pickle')
    DisplayDF_VaxPHU.insert(2, '', '')
    with open('TextOutput/VaccineAgePHU.txt', 'w', newline='') as f:
        f.write('**[Vaccine coverage by PHU/age group - as of '
                + VaxDF_PHU['Date'].max().strftime("%B %#d")
                + '](https://data.ontario.ca/en/dataset/covid-19-vaccine-data-in-ontario) (% at least two/three dosed, chg. week)** -  \n\n')
        # f.write('|PHU name|Ontario_12plus|Adults_18plus||80+|70-79yrs|60-69yrs|50-59yrs|40-49yrs|30-39yrs|18-29yrs|12-17yrs|\n')
        f.write('|**PHU name**|5+ population|12+||05-11yrs|12-17yrs|18-29yrs|30-39yrs|40-49yrs|50-59yrs|60-69yrs|70-79yrs|80+|\n')
        f.write(':-:|:-:|:-:|:-:|:-:|:-:|:--|:-:|:-:|:-:|:-:|:-:|:-:|\n')
        DisplayDF_VaxPHU.to_csv(f, header=False, sep='|')

    print(f'Ended:   {datetime.datetime.now():%Y-%m-%d %H:%M:%S} {round(time.time() - starttime, 2)} seconds')
    print('------------------------------------------------------------------------')


def VaccineData_HospData():
    starttime = time.time()
    print('------------------------------------------------------------------------')
    print(f'VaccineData_HospData \nStarted: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}')
    # HospitalDF = pd.read_pickle('Pickle/HospitalData.pickle')
    dfCaseByVaxStatus = pd.read_pickle('Pickle/CasesByVaxStatus.pickle')
    df_vaccines = pd.read_pickle('Pickle/VaccinesData.pickle')

    dfICUHospByVaxStatus = pd.read_csv('https://data.ontario.ca/dataset/752ce2b7-c15a-4965-a3dc-397bf405e7cc/resource/274b819c-5d69-4539-a4db-f2950794138c/download/vac_status_hosp_icu.csv')
    dfICUHospByVaxStatus.to_csv('SourceFiles/VaccineData_HospData-vac_status_hosp_icu.csv')
    dfICUHospByVaxStatus['date'] = pd.to_datetime(dfICUHospByVaxStatus['date'])
    dfICUHospByVaxStatus = dfICUHospByVaxStatus.set_index('date')
    dfICUHospByVaxStatus = dfICUHospByVaxStatus.fillna(0)
    dfICUHospByVaxStatus = dfICUHospByVaxStatus.sort_index(ascending=False)

    dfICUHospByVaxStatus['ICU_Unvax_PerMillion'] = (dfICUHospByVaxStatus['icu_unvac']
                                                    / df_vaccines['Unvax_Count'].shift(-14)) * 10**6
    dfICUHospByVaxStatus['ICU_Partial_PerMillion'] = (dfICUHospByVaxStatus['icu_partial_vac']
                                                      / df_vaccines['PartialVax_Count'].shift(-14)) * 10**6
    dfICUHospByVaxStatus['ICU_Fully_PerMillion'] = (dfICUHospByVaxStatus['icu_full_vac']
                                                    / df_vaccines['TotalTwoDosed'].shift(-14)) * 10**6
    dfICUHospByVaxStatus['ICU_total'] = (dfICUHospByVaxStatus['icu_unvac']
                                         + dfICUHospByVaxStatus['icu_partial_vac']
                                         + dfICUHospByVaxStatus['icu_full_vac'])

    dfICUHospByVaxStatus['Unvax_Risk_Higher_ICU'] = (dfICUHospByVaxStatus['ICU_Unvax_PerMillion']
                                                     / dfICUHospByVaxStatus['ICU_Fully_PerMillion'])

    dfICUHospByVaxStatus['Partial_Risk_Higher_ICU'] = (dfICUHospByVaxStatus['ICU_Partial_PerMillion']
                                                       / dfICUHospByVaxStatus['ICU_Fully_PerMillion'])

    dfICUHospByVaxStatus['Non_ICU_Hosp_Unvax_PerMillion'] = (dfICUHospByVaxStatus['hospitalnonicu_unvac']
                                                             / df_vaccines['Unvax_Count'].shift(-14)) * 10**6

    dfICUHospByVaxStatus['Non_ICU_Hosp_Partial_PerMillion'] = (dfICUHospByVaxStatus['hospitalnonicu_partial_vac']
                                                               / df_vaccines['PartialVax_Count'].shift(-14)) * 10**6

    dfICUHospByVaxStatus['Non_ICU_Hosp_Fully_PerMillion'] = (dfICUHospByVaxStatus['hospitalnonicu_full_vac']
                                                             / df_vaccines['TotalTwoDosed'].shift(-14)) * 10**6

    dfICUHospByVaxStatus['Unvax_Risk_Higher_NonICUHosp'] = (dfICUHospByVaxStatus['Non_ICU_Hosp_Unvax_PerMillion']
                                                            / dfICUHospByVaxStatus['Non_ICU_Hosp_Fully_PerMillion'])

    dfICUHospByVaxStatus['Partial_Risk_Higher_NonICUHosp'] = (dfICUHospByVaxStatus['Non_ICU_Hosp_Partial_PerMillion']
                                                              / dfICUHospByVaxStatus['Non_ICU_Hosp_Fully_PerMillion'])

    dfICUHospByVaxStatus.to_pickle('Pickle/ICUByVaxStatus.pickle')

    print(f'Ended:   {datetime.datetime.now():%Y-%m-%d %H:%M:%S} {round(time.time() - starttime, 2)} seconds')
    print('------------------------------------------------------------------------')


def DailyReportExtraction(fileDate, fileName=None, AgePage='3', HospPage='7',
                          VariantPage='13'):
    """
    Extract age, hospitalization data and variant data from the daily pdf file.


    Parameters
    ----------
    fileDate : String
        date of the odf file in the 2022-01-01 format.
    fileName : TYPE, optional
        DESCRIPTION. The default is None.
    AgePage : TYPE, optional
        DESCRIPTION. The default is '3'.
    HospPage : TYPE, optional
        DESCRIPTION. The default is '7'.
    VariantPage : TYPE, optional
        DESCRIPTION. The default is '13'.

    Returns
    -------
    None.

    """
    starttime = time.time()
    print('------------------------------------------------------------------------')
    print(f'DailyReportExtraction \nStarted: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}')
    import camelot.io as camelot
    from urllib.error import HTTPError
    import requests
    import PyPDF2.utils

    AgeDF_filePath = 'Pickle/AgeData.csv'
    CumulativeHospitalizations_filePath = 'Pickle/CumulativeHospitalizations.csv'
    VariantCounts_filePath = 'Pickle/VariantCounts.csv'
    DailyReportFile = 'SourceFiles/DailyReport.pdf'

    if fileName is None:
        fileName = fileDate

    try:
        file_path = 'https://files.ontario.ca/moh-covid-19-report-en-' + fileName + '.pdf'
        # file_path = 'https://www.publichealthontario.ca/-/media/Documents/nCoV/epi/covid-19-daily-epi-summary-report.pdf?sc_lang=en'

        r = requests.get(file_path, allow_redirects=True)
        open(DailyReportFile, 'wb').write(r.content)

        tables = camelot.read_pdf(DailyReportFile, flavor='stream', pages=AgePage)

    except (HTTPError, NotImplementedError, PyPDF2.utils.PdfReadError):
        print('Daily PDF file not found', file_path)
        return

    try:
        for i in [0, 1]:
            df = tables[i].df
            result = []
            for col in df.columns:
                result.append(df[col].str.contains('Ages: 0-4').any())
            if True in result:
                break

    except IndexError:
        print('Age data table not found')
        if (AgePage == '3'):
            DailyReportExtraction(fileDate, fileName, '2', HospPage)
        return

    # df = tables[1].df
    df = df.iloc[5:, ]
    df.columns = ['Category', 'Yesterday', 'Today', 'To date']
    df = df.set_index('Category')
    # print(df.dtypes)

    AgeDF = pd.read_csv(AgeDF_filePath)

    New_row = {'Date': fileDate, '0to4': df.loc['Ages: 0-4'][-2], '5to11': df.loc['Ages: 5-11'][-2],
               '12to19': df.loc['Ages: 12-19'][-2], '20to39': df.loc['Ages: 20-39'][-2],
               '40to59': df.loc['Ages: 40-59'][-2], '60to79': df.loc['Ages: 60-79'][-2],
               '80Plus': df.loc['Ages: 80 and over'][-2]
               }
    AgeDF = AgeDF.append(New_row, ignore_index=True)
    AgeDF = AgeDF.drop_duplicates(subset='Date', keep='last')
    AgeDF = AgeDF.sort_values(by='Date', ascending=False)
    AgeDF = AgeDF.replace(',', '', regex=True)
    AgeDF = AgeDF.fillna(0)
    for col in AgeDF.columns[1:]:
        AgeDF[col] = AgeDF[col].astype(int)
    AgeDF.to_csv(AgeDF_filePath, index=False)

    tables = camelot.read_pdf(DailyReportFile, flavor='stream', pages=HospPage)
    try:
        df = tables[0].df
        df = df.iloc[:, :2]
        df.columns = ['Category', 'Cumulative']
        df = df.set_index('Category')
        df = df.fillna(0)

        df = df.loc['Ever in ICU':'Ever hospitalized']
        df['Cumulative'] = df['Cumulative'].str.replace(',', '')
        # df['Ever in ICU'] = df['Ever in ICU'].str.replace(',', '')
        df = df.replace(',', '', regex=True)

        New_row = {'Date': fileDate,
                   'CumHosp': int(df.loc['Ever hospitalized'][0]),
                   'CumICU': int(df.loc['Ever in ICU'][0])}

    except (IndexError, KeyError):
        print('Hospitalization data table not found')
        DailyReportExtraction(fileDate, fileName, AgePage, '8')
        return

    HospitalizationsDF = pd.read_csv(CumulativeHospitalizations_filePath)
    HospitalizationsDF = HospitalizationsDF.append(New_row, ignore_index=True)

    HospitalizationsDF = HospitalizationsDF.drop_duplicates(subset='Date', keep='last')
    HospitalizationsDF = HospitalizationsDF.sort_values(by='Date', ascending=False)
    HospitalizationsDF['NewHosp'] = (HospitalizationsDF['CumHosp'].fillna(method='bfill')
                                     - HospitalizationsDF['CumHosp'].shift(-1))
    HospitalizationsDF['NewICU'] = (HospitalizationsDF['CumICU'].fillna(method='bfill')
                                    - HospitalizationsDF['CumICU'].shift(-1))

    HospitalizationsDF = HospitalizationsDF.fillna(0)
    for col in HospitalizationsDF.columns[1:]:
        HospitalizationsDF[col] = HospitalizationsDF[col].astype(int)

    HospitalizationsDF.to_csv(CumulativeHospitalizations_filePath, index=False)
    pypdf = EVHelper.text_extractor(DailyReportFile)

    # # Variant Data
    # try:
    #     s = pypdf[11]
    #     s.index('Lineage')
    # except (ValueError):
    #     s = pypdf[12]
    #     s.index('Lineage')
    # s = s.replace('\n', '')
    # s = s.replace(',', '')

    # Alpha = s[s.index('Lineage B.1.1.7 (Alpha)'):
    #           s.index('Lineage B.1.351 (Beta)') - 1].split(' ')
    # Beta = s[s.index('Lineage B.1.351 (Beta)'):
    #          s.index('Lineage P.1 (Gamma)') - 1].split(' ')
    # Gamma = s[s.index('Lineage P.1 (Gamma)'):
    #           s.index('Lineage B.1.617.2 (Delta)') - 1].split(' ')
    # Delta = s[s.index('Lineage B.1.617.2 (Delta)'):
    #           s.index('Lineage B.1.1.529 (Omicron)') - 1].split(' ')
    # Omicron = s[s.index('Lineage B.1.1.529 (Omicron)'):
    #             s.index('Mutations') - 1].split(' ')
    # Omicron_1 = s[s.index('S-gene Target Failure'):
    #               s.index('Note') - 1].split(' ')

    # New_row = {'Date': fileDate,
    #            'Alpha': int(Alpha[-2]),
    #            'Beta': int(Beta[-2]),
    #            'Gamma': int(Gamma[-2]),
    #            'Delta': int(Delta[-2]),
    #            'Omicron': int(Omicron[-2]) + int(Omicron_1[-2])}

    # # tables = camelot.read_pdf('DailyReport.pdf', flavor='stream', pages=VariantPage)
    # # try:
    # #     if VariantPage == '12':
    # #         df = tables[0].df
    # #         # df.to_csv('aaa.csv')

    # #         df = df.drop(1,axis=1)
    # #     else:
    # #         df = tables[0].df
    # #         df.to_csv('aaa.csv')
    # #         df = df.iloc[7:12,0:4]

    # #     df.columns = ['Category', 'Yesterday', 'Today', 'To date']
    # #     df = df.set_index('Category')
    # #     df = df.replace(',', '',regex=True)
    # #     New_row = {'Date': fileDate,
    # #                'Alpha': int(df.loc['Lineage B.1.1.7 (Alpha)'][1]),
    # #                'Beta': int(df.loc['Lineage B.1.351 (Beta)'][1]),
    # #                'Gamma': int(df.loc['Lineage P.1 (Gamma)'][1]),
    # #                'Delta': int(df.loc['Lineage B.1.617.2 (Delta)'][1]),
    # #                'Omicron': int(df.loc['Lineage B.1.1.529 (Omicron)'][1])}

    # # except (IndexError, KeyError, ValueError):
    # #     print('Variant data table not found')
    # #     if VariantPage == '13':
    # #         DailyReportExtraction(fileDate, fileName, AgePage, HospPage, '12')
    # #     return

    # VariantCountsDF = pd.read_csv(VariantCounts_filePath)
    # VariantCountsDF = VariantCountsDF.append(New_row, ignore_index=True)
    # VariantCountsDF = VariantCountsDF.drop_duplicates(subset='Date', keep='last')
    # VariantCountsDF = VariantCountsDF.sort_values(by='Date', ascending=False)
    # VariantCountsDF.to_csv(VariantCounts_filePath, index=False)

    print(f'Ended:   {datetime.datetime.now():%Y-%m-%d %H:%M:%S} {round(time.time() - starttime, 2)} seconds')
    print('------------------------------------------------------------------------')


def icu_capacity_stats():
    """
    ICU Capacity data

    Returns
    -------
    None.

    """
    starttime = time.time()
    print('------------------------------------------------------------------------')
    print(f'icu_capacity_stats \nStarted: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}')
    ConsoleOut = sys.stdout
    icu_capacity_filename = 'TextOutput/ICU_capacity.txt'

    df = pd.read_csv('https://data.ontario.ca/dataset/1b5ff63f-48a1-4db6-965f-ab6acbab9f29/resource/c7f2590f-362a-498f-a06c-da127ec41a33/download/icu_beds.csv')
    df.to_csv('SourceFiles/icu_capacity_stats-icu_beds.csv')
    df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
    df.set_index('date', inplace=True)
    df.sort_index(ascending=False, inplace=True)
    df = df.astype(int)

    report_date = df.index.max()
    icu_covid_count = df.loc[report_date]['adult_icu_crci_patients']
    icu_noncovid_count = df.loc[report_date]['adult_icu_non_crci_patients']
    icu_covid_count_wkchange = (df['adult_icu_crci_patients'][0]
                                - df['adult_icu_crci_patients'][7])
    icu_noncovid_count_wkchange = (df['adult_icu_non_crci_patients'][0]
                                   - df['adult_icu_non_crci_patients'][7])
    available_icu_all = df['available_adult_icu_beds'][0]
    available_icu_all_wkchange = (df['available_adult_icu_beds'][0]
                                  - df['available_adult_icu_beds'][7])
    total_icu_capacity = df['total_adult_icu_beds'][0]

    sys.stdout = open(icu_capacity_filename, 'w')
    print(f"**ICU Capacity (chg in week)** - last updated {report_date.strftime('%b %d')}")
    print()
    print(f"* Total COVID/non-COVID ICU patients: {icu_covid_count:,.0f} / {icu_noncovid_count:,.0f}",
          f"({icu_covid_count_wkchange:+.0f}/ {icu_noncovid_count_wkchange:+.0f})")
    print(f"* Total avail ICU capacity for ALL: {available_icu_all} ({available_icu_all_wkchange:+.0f})")
    print(f"* Total ICU capacity: {total_icu_capacity:,.0f}")
    sys.stdout = ConsoleOut
    endtime = datetime.datetime.now()
    print(f"Ended:   {endtime:%Y-%m-%d %H:%M:%S} {round(time.time() - starttime, 2)} seconds")
    print('------------------------------------------------------------------------')


def hospitalizations_with_for_covid():

    starttime = time.time()
    print('------------------------------------------------------------------------')
    print(f'hospitalizations_with_for_covid \nStarted: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}')
    df = pd.read_csv('https://data.ontario.ca/dataset/8033f5df-6db8-41fe-921a-5f1160b4d75b/resource/d1199d1b-dc82-4e63-bb80-5c715e97a127/download/hosp_icu_c19_breakdown.csv')
    df.to_csv('SourceFiles/hospitalizations_with_for_covid-hosp_icu_c19_breakdown.csv')
    df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
    df.set_index('date', inplace=True)

    df.sort_index(ascending=False, inplace=True)

    # df = df.astype(int)
    df.to_pickle('Pickle/WithForCOVID_Hosp.pickle')

    endtime = datetime.datetime.now()
    print(f"Ended:   {endtime:%Y-%m-%d %H:%M:%S} {round(time.time() - starttime, 2)} seconds")
    print('------------------------------------------------------------------------')


def deaths_with_for_covid():
    starttime = time.time()
    print('------------------------------------------------------------------------')
    print(f'deaths_with_for_covid \nStarted: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}')
    df = pd.read_csv('https://data.ontario.ca/dataset/c43fd28d-3288-4ad2-87f1-a95abac706b8/resource/3273c977-416f-407e-86d2-1e45a7261e7b/download/deaths_fatality_type.csv')

    df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
    df.set_index('date', inplace=True)
    df.sort_index(ascending=False, inplace=True)

    df.to_csv('SourceFiles/DeathsWithFor-deaths_fatality_type.csv')
    df.to_pickle('Pickle/WithForCOVID_Deaths.pickle')
    endtime = datetime.datetime.now()
    print(f"Ended:   {endtime:%Y-%m-%d %H:%M:%S} {round(time.time() - starttime, 2)} seconds")
    print('------------------------------------------------------------------------')


def school_closures():
    """
    Schools closure dataset.

    Returns
    -------
    None.

    """
    starttime = time.time()
    ConsoleOut = sys.stdout
    school_closuresfilename = 'TextOutput/school_closures.txt'

    print('------------------------------------------------------------------------')
    print(f'school_closures \nStarted: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}')

    df = pd.read_csv('https://data.ontario.ca/dataset/b1fef838-8784-4338-8ef9-ae7cfd405b41/resource/abc82e5c-44ed-41c9-817c-8cacd9b07d00/download/schoolclosures2022.csv')
    df.to_csv('SourceFiles/school_closures-list.csv')
    df['percent_closed'] = df['schools_closed'] / df['total_schools']
    df['report_date'] = pd.to_datetime(df['report_date'], infer_datetime_format=True)
    df = df.set_index('report_date')
    df.sort_index(ascending=False, inplace=True)
    df.to_pickle('Pickle/SchoolClosures-List.pickle')

    sys.stdout = open(school_closuresfilename, 'w', encoding='utf-8')
    print(f"* {df.loc[df.index.max()]['schools_closed']:.0f} schools are currently closed",
          f"({df.loc[df.index.max()]['percent_closed']:.2%} of all)")
    sys.stdout = ConsoleOut

    endtime = datetime.datetime.now()
    print(f"Ended:   {endtime:%Y-%m-%d %H:%M:%S} {round(time.time() - starttime, 2)} seconds")
    print('------------------------------------------------------------------------')


def school_absenteeism():
    """
    School absenteeism data - published
    Link: https://data.ontario.ca/dataset/summary-of-cases-in-schools/resource/e3214f57-9c24-4297-be27-a1809f9044ba

    Returns
    -------
    None.

    """
    starttime = time.time()
    ConsoleOut = sys.stdout
    school_absenteeism_filename = 'TextOutput/school_absenteeism.txt'

    print('------------------------------------------------------------------------')
    print(f'school_absenteeism \nStarted: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}')

    df = pd.read_csv('https://data.ontario.ca/dataset/b1fef838-8784-4338-8ef9-ae7cfd405b41/resource/e3214f57-9c24-4297-be27-a1809f9044ba/download/schoolabsences2022.csv')
    df.to_csv('SourceFiles/school_absenteeism-list.csv')
    df['date'] = pd.to_datetime(df['date'], infer_datetime_format=True)
    df = df.set_index('date')
    df = df.loc[df.index.max()]  # Only look at today's today
    median_absent = df['absence_percentage_staff_students'].median()
    lower_quartile = df['absence_percentage_staff_students'].quantile(0.25)
    upper_quartile = df['absence_percentage_staff_students'].quantile(0.75)
    df.to_pickle('Pickle/SchoolAbsenteeism-List.pickle')

    sys.stdout = open(school_absenteeism_filename, 'w', encoding='utf-8')
    if (df.index.max() + pd.Timedelta(days=3) >= TODAYS_DATE_GLOBAL):
        print(f"* The median school has {median_absent:.0%} of students/staff absent.",
              f"The interquartile range is {lower_quartile:.0%} to {upper_quartile:.0%} ")
    else:
        print("* No school absenteeism data reported.")
    sys.stdout = ConsoleOut

    endtime = datetime.datetime.now()
    print(f"Ended:   {endtime:%Y-%m-%d %H:%M:%S} {round(time.time() - starttime, 2)} seconds")
    print('------------------------------------------------------------------------')


def DailyReportExtraction_Cases():
    import camelot.io as camelot

    DailyReportFile = 'DailyReport.pdf'
    df = pd.DataFrame()
    for pages in ['9', '10', '11']:
        tables = camelot.read_pdf(DailyReportFile, flavor='stream', pages=pages)
        df = df.append(tables[0].df)
    df = df[~df[0].str.contains('TOTAL')]


def DeathProjection():
    ConsoleOut = sys.stdout
    TextFileName = 'TextOutput/DeathProjectionText.txt'
    TableFileName = 'TextOutput/FatalityRateTable.txt'

    df = pd.read_pickle(config.get('file_location', 'master_dataframe'))

    # df = df[df['Case_AcquisitionInfo'] == 'Outbreak']
    # df = df[df['Case_AcquisitionInfo'] != 'Outbreak']

    TodayDF = df[df['File_Date'] == df['File_Date'].max()]
    # TodayPivot = pd.crosstab(index=TodayDF['Age_Group'], columns = TodayDF['Outcome'] , values=TodayDF['Row_ID'], rownames=None, colnames=None, aggfunc=np.count_nonzero, margins=False,  normalize=False)
    MonthAgoDF = df[df['File_Date'] == df['File_Date'].max() - datetime.timedelta(days=30)]
    # MonthAgoPivot = pd.crosstab(index=MonthAgoDF['Age_Group'], columns = MonthAgoDF['Outcome'] , values=MonthAgoDF['Row_ID'], rownames=None, colnames=None, aggfunc=np.count_nonzero, margins=False,  normalize=False)
    # ChangePivot = TodayPivot - MonthAgoPivot

    # LastMonthDeathRates = ChangePivot['Fatal']/(ChangePivot['Fatal']+ChangePivot['Resolved'])

    # NewCasesToday = pd.read_pickle('Pickle/ChangeInCasesByAge.pickle')
    # TotalDeathsToday = (NewCasesToday[NewCasesToday.columns[0]]*LastMonthDeathRates).fillna(0)

    TodaysPivot = pd.pivot_table(TodayDF, columns=['Outbreak_Related', 'Outcome'],
                                 index='Age_Group', values='Row_ID', aggfunc=np.count_nonzero)
    MonthAgosPivot = pd.pivot_table(MonthAgoDF, columns=['Outbreak_Related', 'Outcome'],
                                    index='Age_Group', values='Row_ID', aggfunc=np.count_nonzero)

    ChangePivot_Outbreak = (TodaysPivot.loc[:, 'Yes'] - MonthAgosPivot.loc[:, 'Yes']).fillna(0)
    ChangePivot_NonOutbreak = (TodaysPivot.loc[:, 'No'] - MonthAgosPivot.loc[:, 'No']).fillna(0)
    LastMonthDeathRates_Outbreak = (ChangePivot_Outbreak['Fatal']
                                    / (ChangePivot_Outbreak['Fatal'] + ChangePivot_Outbreak['Resolved']))
    LastMonthDeathRates_Outbreak = LastMonthDeathRates_Outbreak.fillna(0)
    LastMonthDeathRates_NonOutbreak = (ChangePivot_NonOutbreak['Fatal']
                                       / (ChangePivot_NonOutbreak['Fatal'] + ChangePivot_NonOutbreak['Resolved']))
    LastMonthDeathRates_NonOutbreak = LastMonthDeathRates_NonOutbreak.fillna(0)

    casesByAgeAndOutbreak = pd.pivot_table(df, values='Row_ID',
                                           index=['Outbreak_Related', 'Age_Group'],
                                           columns='File_Date', aggfunc=np.count_nonzero)

    changeInCases_Outbreak = (casesByAgeAndOutbreak.loc['Yes']
                              - casesByAgeAndOutbreak.loc['Yes'].shift(1, axis=1))
    NewCases_Outbreak = changeInCases_Outbreak[changeInCases_Outbreak.columns[len(changeInCases_Outbreak.columns) - 1]]
    changeInCases_NonOutbreak = (casesByAgeAndOutbreak.loc['No'] - casesByAgeAndOutbreak.loc['No'].shift(1, axis=1))
    NewCases_NonOutbreak = changeInCases_NonOutbreak[changeInCases_NonOutbreak.columns[len(changeInCases_NonOutbreak.columns) - 1]]

    TotalDeathsToday = ((LastMonthDeathRates_Outbreak.T * NewCases_Outbreak)
                        + (LastMonthDeathRates_NonOutbreak.T * NewCases_NonOutbreak))
    TotalOutbreakDeathsToday = (LastMonthDeathRates_Outbreak.T * NewCases_Outbreak)
    TotalNonOutbreakDeathsToday = (LastMonthDeathRates_NonOutbreak.T * NewCases_NonOutbreak)

    sys.stdout = open(TextFileName, 'w')
    print('* Based on death rates from completed cases over the past month, **',
          TotalDeathsToday.sum().round(1),
          "** people from today's new cases are expected to die of which ",
          TotalDeathsToday[0:4].sum().round(1), ' are less than 50 years old, and ',
          TotalDeathsToday[4].sum().round(1), ', ', TotalDeathsToday[5].sum().round(1), ', ',
          TotalDeathsToday[6].sum().round(1), ', ', TotalDeathsToday[7].sum().round(1), ' and ',
          TotalDeathsToday[8].sum().round(1),
          ' are in their 50s, 60s, 70s, 80s and 90s respectively. Of these, ',
          TotalOutbreakDeathsToday.sum().round(1), ' are from outbreaks, and ',
          TotalNonOutbreakDeathsToday.sum().round(1), ' are non-outbreaks', sep='')

    sys.stdout = open(TableFileName, 'w')
    print('**Case fatality rates by age group (last 30 days):**')
    print()
    print('Age Group|Outbreak-->|CFR %|Deaths|Non-outbreak-->|CFR%|Deaths|')
    print(':-:|:--|--:|--:|:--|--:|--:|')
    for i in range(len(LastMonthDeathRates_Outbreak) - 1):
        print(LastMonthDeathRates_Outbreak.index[i], '||',
              (LastMonthDeathRates_Outbreak.iloc[i] * 100).round(2), '%|',
              ChangePivot_Outbreak.iloc[i, 0].astype(int), '||',
              (LastMonthDeathRates_NonOutbreak.iloc[i] * 100).round(2), '%|',
              ChangePivot_NonOutbreak.iloc[i, 0].astype(int), sep='')

    sys.stdout = ConsoleOut


def OutbreakData():
    ConsoleOut = sys.stdout
    TextFileName = 'TextOutput/OutbreakData.txt'
    df = pd.read_csv('https://data.ontario.ca/dataset/5472ffc1-88e2-48ca-bc9f-4aa249c1298d/resource/d5d8f478-765c-4246-b8a7-c3b13a4a1a41/download/outbreak_cases.csv')
    df.to_csv('SourceFiles/OutbreakData-outbreak_cases.csv')
    df['date'] = pd.to_datetime(df['date'])
    ReportDate = df['date'].max()
    df = df[df['date'] == ReportDate]
    df['outbreak_subgroup'] = df['outbreak_subgroup'].str.capitalize()

    sys.stdout = open(TextFileName, 'w')
    print('**Outbreak data** *(latest data as of ', ReportDate.strftime('%B %d'),
          ')*- [Source](https://data.ontario.ca/dataset/ontario-covid-19-outbreaks-data) and [Definitions](https://covid-19.ontario.ca/data/covid-19-case-data-glossary#outbreak)',
          sep='')
    print()
    print('* New outbreak cases:', df['TOTAL_CASES'].sum())
    print('* *New outbreak cases (groups with 2+):* ', end='')
    df = df[abs(df['TOTAL_CASES']) >= 2]
    for i in range(len(df)):
        print(df.iloc[i]['outbreak_subgroup'], ' (', df.iloc[i]['TOTAL_CASES'], '), ',
              sep='', end='')

    sys.stdout = ConsoleOut
    sys.stdout = open(TextFileName, 'a')

    dfActive = pd.read_csv('https://data.ontario.ca/dataset/5472ffc1-88e2-48ca-bc9f-4aa249c1298d/resource/66d15cce-bfee-4f91-9e6e-0ea79ec52b3d/download/ongoing_outbreaks.csv')
    dfActive.to_csv('SourceFiles/OutbreakData-ongoing_outbreaks.csv')
    dfActive['date'] = pd.to_datetime(dfActive['date'])
    ReportDateActive = dfActive['date'].max()
    activePivot = pd.pivot_table(dfActive, index='outbreak_subgroup', columns='date', aggfunc=sum)
    activePivot = activePivot.reindex(columns=sorted(activePivot.columns, reverse=True))
    activePivot = activePivot.sort_values(by=activePivot.columns[0], ascending=False)
    activePivot = activePivot.fillna(0).astype(int)
    print()
    print('* ', activePivot.iloc[:, 0].sum(), ' active cases in outbreaks (',
          format(activePivot.iloc[:, 0].sum() - activePivot.iloc[:, 7].sum(), "+,d"),
          ' vs. last week)', sep='')
    print('* Major categories with active cases (vs. last week): ', end='')
    for i in range(7):
        print(str(activePivot.index[i]).translate(str.maketrans('', '', '1234567890')), ': ',
              activePivot.iloc[i, 0],
              '(', format(activePivot.iloc[i, 0] - activePivot.iloc[i, 7], "+,d"), ')',
              sep='', end=',')
    print()
    sys.stdout = ConsoleOut


def AdHocCodes():

    #------------------------------------------------------------------------
    #Episode Date for specific PHU
    (pd.pivot_table(df[ (df['Reporting_PHU']=='Ottawa')],index = 'File_Date',values = 'Row_ID',columns ='Episode_Date',aggfunc = np.count_nonzero,fill_value = 0).sort_index(ascending = False)
     - pd.pivot_table(df[ (df['Reporting_PHU']=='Ottawa')],index = 'File_Date',values = 'Row_ID',columns ='Episode_Date',aggfunc = np.count_nonzero,fill_value=0).sort_index(ascending = False).shift(-1)).T.to_csv('aaa.csv')


    #########################################################################
    #------------------------------------------------------------------------
    #Pivot table showing episode dates by PHU for current day's reports
    starttime = time.time()
    EpisodeDateByPHUToday = pd.pivot_table(MasterDataFrame[MasterDataFrame['File_Date']==MasterDataFrame['File_Date'].max()],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'Episode_Date',fill_value = 0,aggfunc = np.count_nonzero,margins = False).sort_index(axis = 1,ascending = False)
    EpisodeDateByPHUYesterday = pd.pivot_table(MasterDataFrame[MasterDataFrame['File_Date']==(MasterDataFrame['File_Date'].max()-datetime.timedelta(days = 1))],values = 'Row_ID',index = ['Reporting_PHU'],columns = 'Episode_Date',fill_value = 0,aggfunc = np.count_nonzero,margins = False).sort_index(axis = 1,ascending = False)
    #pivotabc = EpisodeDateByPHUToday.sub(EpisodeDateByPHUYesterday,fill_value = 0,axis = 1)
    #EpisodeDateByPHUYesterday.insert(0,EpisodeDateByPHUToday.columns[0],0)

    if EpisodeDateByPHUToday.columns.size >= EpisodeDateByPHUYesterday.columns.size:
        for x in range(EpisodeDateByPHUYesterday.columns.size-1):
            if EpisodeDateByPHUYesterday.columns[x] not in EpisodeDateByPHUToday.columns:
                print (EpisodeDateByPHUYesterday.columns[x])
                EpisodeDateByPHUToday.insert(EpisodeDateByPHUToday.columns.size-1, EpisodeDateByPHUYesterday.columns[x],0)

        for x in range(EpisodeDateByPHUToday.columns.size-1):
            if EpisodeDateByPHUToday.columns[x] not in EpisodeDateByPHUYesterday.columns:
                print (EpisodeDateByPHUToday.columns[x])
                EpisodeDateByPHUYesterday.insert(EpisodeDateByPHUYesterday.columns.size-1, EpisodeDateByPHUToday.columns[x],0)

    else:
        for x in range(EpisodeDateByPHUToday.columns.size-1):
            if EpisodeDateByPHUToday.columns[x] not in EpisodeDateByPHUYesterday.columns:
                print (EpisodeDateByPHUToday.columns[x])
                EpisodeDateByPHUYesterday.insert(EpisodeDateByPHUYesterday.columns.size-1, EpisodeDateByPHUToday.columns[x],0)

        for x in range(EpisodeDateByPHUYesterday.columns.size-1):
            if EpisodeDateByPHUYesterday.columns[x] not in EpisodeDateByPHUToday.columns:
                print (EpisodeDateByPHUYesterday.columns[x])
                EpisodeDateByPHUToday.insert(EpisodeDateByPHUToday.columns.size-1, EpisodeDateByPHUYesterday.columns[x],0)

    EpisodeDateByPHUToday = EpisodeDateByPHUToday.sort_index(axis = 1,ascending = False)
    EpisodeDateByPHUYesterday = EpisodeDateByPHUYesterday.sort_index(axis = 1,ascending = False)
    ChangeEpisodeDatePivot = EpisodeDateByPHUToday-EpisodeDateByPHUYesterday
    ChangeEpisodeDatePivot = ChangeEpisodeDatePivot.sort_index(axis = 1,ascending = False)

    ChangeEpisodeDatePivot.insert(0,'Today',ChangeEpisodeDatePivot.sum(axis=1))
    ChangeEpisodeDatePivot = ChangeEpisodeDatePivot.sort_values(by = ChangeEpisodeDatePivot.columns[0],ascending = False)

    ChangeEpisodeDatePivot.to_csv('CSV/ChangeEpisodeDatePivot.csv')
    ChangeEpisodeDatePivot.to_pickle('Pickle/ChangeEpisodeDatePivot.pickle')



    casesByOutcomeByAgeByOutbreakStatus = pd.pivot_table(MasterDataFrame[MasterDataFrame['Outcome']=='Not Resolved'],values = 'Row_ID',index = ['Outcome','Age_Group','Case_AcquisitionInfo'],columns = 'File_Date',aggfunc=np.count_nonzero)

    activeCases70PlusOutbreakStatus = pd.DataFrame([casesByOutcomeByAge.loc[('Not Resolved',(['70s','80s','90s']),['Travel','Community','Close contact'])].sum(),casesByOutcomeByAge.loc[('Not Resolved',(['70s','80s','90s']),'Outbreak')].sum()]).to_csv('aaa.csv')

    #########################################################################
    #------------------------------------------------------------------------
    #30 day fatality rate
    df = pd.read_pickle('Pickle/changeInCasesByOutcomeByAge.pickle')
    Rolling30DayFatal = df.loc['Fatal'].T[::-1].rolling(30).sum().fillna(0)[::-1].T
    Rolling30DayResolved = df.loc['Resolved'].T[::-1].rolling(30).sum().fillna(0)[::-1].T
    #(Rolling30DayFatal/Rolling30DayResolved*100).to_csv('aaa.csv')


    #########################################################################
    #------------------------------------------------------------------------
    #School cases by week day

    dfschools = pd.read_csv('https://data.ontario.ca/dataset/b1fef838-8784-4338-8ef9-ae7cfd405b41/resource/7fbdbb48-d074-45d9-93cb-f7de58950418/download/schoolcovidsummary.csv')

    dfschools['reported_date'] = pd.to_datetime(dfschools['reported_date'])
    dfschools['cumulative_school_related_cases']
    dfschools.set_index('reported_date')
    dfschools = dfschools.set_index('reported_date')
    df = dfschools['new_total_school_related_cases']
    for dayWeek in set((df.index.dayofweek)):
        print(dayWeek,round(df[df.index.dayofweek == dayWeek].mean(),2))

    print(time.time()-starttime)

    #########################################################################
    #------------------------------------------------------------------------
    #Std deviation of day of week

    df = pd.read_pickle('Pickle/OntarioCaseStatus.pickle')
    df = df.fillna(0)
    df = df.sort_index(ascending = True)
    df = df.fillna(0)
    #df['Day new cases'].rolling(6).mean()
    df['Weekly ratio'] = df['Day new cases']/df['Day new cases'].shift(1).rolling(6).mean().fillna(1)
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.fillna(1)
    df = df[df.index >= datetime.datetime(2020,4,1)]
    for i in range (7):
        print(calendar.day_name[i],round(df[df.index.dayofweek == i]['Weekly ratio'].mean(),3),round(df[df.index.dayofweek == i]['Weekly ratio'].std()/np.sqrt(df[df.index.dayofweek == i]['Weekly ratio'].count()),4))

    #########################################################################
    #------------------------------------------------------------------------
    #Std deviation of day of week - old
    # df = pd.read_pickle('Pickle/ChangeInCasesByPHU.pickle')
    # df = df.fillna(0).astype(int).T
    # dfa=df.sum(axis=1)
    # df2 = dfa[::-1].shift(1).rolling(6).mean()[::-1]
    # calc = df.sum(axis=1)/df2

    # for i in range (7):
    #     print(calc[calc.index.dayofweek == i].mean(),(calc[calc.index.dayofweek == i].std())/np.sqrt(calc[calc.index.dayofweek == i].count()))


    #########################################################################
    #------------------------------------------------------------------------
    #Population used in VaccineDF
    abc = pd.pivot_table(VaxDF_PHU[VaxDF_PHU['Agegroup']=='Ontario_12plus'][['PHU name','Total population','Date']],index = 'PHU name',columns = 'Date')
    abc = abc.reindex(columns=sorted(abc.columns,reverse = True))


def OntarioZones():
    df = pd.read_csv('https://data.ontario.ca/dataset/cbb4d08c-4e56-4b07-9db6-48335241b88a/resource/ce9f043d-f0d4-40f0-9b96-4c8a83ded3f6/download/response_framework.csv')
    df.to_csv('SourceFiles/OntarioZones-response_framework.csv')
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['end_date'] = pd.to_datetime(df['end_date'])
    df = df.sort_values(by='end_date', ascending=False)
    df = df.set_index('Reporting_PHU_id')

    df['Status_PHU'] = df['Status_PHU'].replace(['Prevent'], ['Green'])
    df['Status_PHU'] = df['Status_PHU'].replace(['Protect'], ['Yellow'])
    df['Status_PHU'] = df['Status_PHU'].replace(['Restrict'], ['Orange'])
    df['Status_PHU'] = df['Status_PHU'].replace(['Control'], ['Red'])
    # df['Status_PHU'] = df['Status_PHU'].replace(['Lockdown'], ['[Grey/Gray](https://fast-poll.com/poll/3e71c5ce)'])
    df['Status_PHU'] = df['Status_PHU'].replace(['Lockdown'], ['Grey'])
    df['Status_PHU'] = df['Status_PHU'].replace(['Stay-at-home'], ['Home'])

    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Toronto Public Health'], 'Toronto PHU')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Peel Public Health'], 'Peel')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['York Region Public Health Services'], 'York')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Region of Waterloo,  Public Health'], 'Waterloo Region')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Durham Region Health Department'], 'Durham')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Hamilton Public Health Services'], 'Hamilton')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Middlesex-London Health Unit'], 'London')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Halton Region Health Department'], 'Halton')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Simcoe Muskoka District Health Unit'], 'Simcoe-Muskoka')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Niagara Region Public Health Department'], 'Niagara')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Windsor-Essex County Health Unit'], 'Windsor')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Wellington-Dufferin-Guelph Public Health'], 'Wellington-Guelph')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Kingston,  Frontenac and Lennox & Addington Public Health'], 'Kingston')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Southwestern Public Health'], 'Southwestern')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Chatham-Kent Health Unit'], 'Chatham-Kent')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Ottawa Public Health'], 'Ottawa')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Algoma Public Health Unit'], 'Algoma')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Thunder Bay District Health Unit'], 'Thunder Bay')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Timiskaming Health Unit'], 'Timiskaming')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Porcupine Health Unit'], 'Porcupine')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Sudbury & District Health Unit'], 'Sudbury')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Brant County Health Unit'], 'Brant')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Eastern Ontario Health Unit'], 'Eastern Ontario')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Leeds,  Grenville and Lanark District Health Unit'], 'Leeds,  Grenville,  Lanark')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Haldimand-Norfolk Health Unit'], 'Haldimand-Norfolk')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Lambton Public Health'], 'Lambton')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Haliburton,  Kawartha,  Pine Ridge District Health Unit'], 'Haliburton,  Kawartha')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Grey Bruce Health Unit'], 'Grey Bruce')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Huron Perth District Health Unit'], 'Huron Perth')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Peterborough Public Health'], 'Peterborough')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Renfrew County and District Health Unit'], 'Renfrew')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Hastings and Prince Edward Counties Health Unit'], 'Hastings')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['Northwestern Health Unit'], 'Northwestern')
    df['Reporting_PHU'] = df['Reporting_PHU'].replace(['North Bay Parry Sound District Health Unit'], 'North Bay')

    dfnew = pd.DataFrame()
    for PHU in set(df['Reporting_PHU']):
        dfnew = dfnew.append(df[df['Reporting_PHU']==PHU].iloc[0])
    dfnew = dfnew[['Reporting_PHU','Status_PHU']]
    dfnew = dfnew.set_index('Reporting_PHU')
    dfnew.to_pickle('Pickle/OntarioZones.pickle')

    del df
    del dfnew

def PHUWebsiteReplacements(df):
    df.rename(index={'Toronto PHU': '[Toronto PHU](https://www.toronto.ca/home/covid-19/covid-19-latest-city-of-toronto-news/covid-19-status-of-cases-in-toronto/)'},inplace=True)
    df.rename(index={'Waterloo Region': '[Waterloo Region](https://www.regionofwaterloo.ca/en/health-and-wellness/positive-cases-in-waterloo-region.aspx)'},inplace=True)
    df.rename(index={'Peel': '[Peel](https://www.peelregion.ca/coronavirus/case-status/)'},inplace=True)
    df.rename(index={'York': '[York](https://www.york.ca/wps/portal/yorkhome/health/yr/covid-19/covid19inyorkregion/01covid19inyorkregion/!ut/p/z1/tZJLT-MwFIV_C4suI187SW0vTeg0CTQtjz7iTZVJ09RMk5SMKTC_fhxUJISgMGLshV-6Plfn80ESLZCss70qM62aOtuacyr7y0gMozA8h3jssQAEjEVMKIMBx2j-XAAfDAFIfuX9kQJ5XH6GJJK7XK1QStyCspwzh4KfO16W9x2Of67MxNc-5-uMd3JI5rXe6Q1Kn9pl3tS6qHUPnpr2lzn81krfP19smqowc5Ft9aYHebNXKwfzww5zVXcv2qI0mHoA-J1rFH_m3cAl7SgYlcZBpjeOqtcNWrz0OuzeiC7e72Wk1O3dnRTGXufpUaOFbX_zDuZrh8Nr5kE0i6mY4TF4kXsoIMTrhziAGMIxg-gHnfhnLMRwTg4FR_43NfmgH0K8Imi-V8UDmtZNW5m8Xv9jHMKXDpQFIhRDmMDNlMLlgHqsfzGaXFzhb3b4xIBledeqPAW78sSu_P-BE0cQYNHF3x24IEgUsFM3Zklil31il31il31iN_ez78LZVdNpxVx_WzLNo1u_rJZnp4mTxvs_R5eRODn5Cx0G6fA!/dz/d5/L2dBISEvZ0FBIS9nQSEh/)'},inplace=True)
    df.rename(index={'Ottawa': '[Ottawa](https://www.ottawapublichealth.ca/en/reports-research-and-statistics/daily-covid19-dashboard.aspx)'},inplace=True)
    df.rename(index={'Leeds, Grenville, Lanark': '[Leeds, Grenville, Lanark](https://healthunit.org/health-information/covid-19/local-cases-and-statistics/dashboard/)'},inplace=True)
    df.rename(index={'Durham': '[Durham](https://www.durham.ca/en/shared-content/covid-19-durham-region-case-status.aspx)'},inplace=True)
    df.rename(index={'Hamilton': '[Hamilton](https://www.hamilton.ca/coronavirus/status-cases-in-hamilton)'},inplace=True)
    df.rename(index={'Halton': '[Halton](https://www.halton.ca/For-Residents/Immunizations-Preventable-Disease/Diseases-Infections/New-Coronavirus/Status-of-COVID-19-Cases-in-Halton)'},inplace=True)
    df.rename(index={'Niagara': '[Niagara](https://niagararegion.ca/health/covid-19/statistics/statistics.aspx)'},inplace=True)
    df.rename(index={'Windsor': '[Windsor](https://www.wechu.org/cv/local-updates)'},inplace=True)
    df.rename(index={'Hastings': '[Hastings](https://hpepublichealth.ca/covid-19-cases/)'},inplace=True)
    # df.rename(index={'Porcupine':'[Porcupine](https://animals.sandiegozoo.org/sites/default/files/2016-09/animals_hero_porcupine.jpg)'},inplace=True)

    return (df)


def PHUNameReplacements(df):
    df.replace({'Leeds, Grenville And Lanark District':'Leeds, Grenville, Lanark'},inplace=True)
    df.replace({'Kingston, Frontenac, Lennox & Addington':'Kingston'},inplace=True)
    df.replace({'Thunder Bay District':'Thunder Bay'},inplace=True)
    df.replace({'Halton Region':'Halton'},inplace=True)
    df.replace({'Wellington-Dufferin-Guelph':'Wellington-Guelph'},inplace=True)
    df.replace({'Haliburton, Kawartha, Pine Ridge':'Haliburton, Kawartha'},inplace=True)
    df.replace({'Niagara Region':'Niagara'},inplace=True)
    df.replace({'Hastings & Prince Edward Counties':'Hastings'},inplace=True)
    df.replace({'North Bay Parry Sound District':'North Bay'},inplace=True)
    df.replace({'Renfrew County And District':'Renfrew'},inplace=True)

    df.replace(['Algoma_District','Algoma District'],'Algoma',inplace=True)
    df.replace({'Brant_County':'Brant'},inplace=True)
    df.replace({'Chatham_Kent':'Chatham-Kent'},inplace=True)
    df.replace({'Durham_Region':'Durham'},inplace=True)
    df.replace({'Eastern_Ontario':'Eastern Ontario'},inplace=True)
    df.replace({'Grey_Bruce':'Grey Bruce'},inplace=True)
    df.replace({'Haldimand_Norfolk':'Haldimand-Norfolk'},inplace=True)
    df.replace({'Haliburton_Kawartha_Pine_Ridge':'Haliburton, Kawartha'},inplace=True)
    df.replace({'Halton_Region':'Halton'},inplace=True)
    df.replace({'City_of_Hamilton':'Hamilton'},inplace=True)
    df.replace({'Hastings_Prince_Edward':'Hastings'},inplace=True)
    df.replace({'Huron_Perth':'Huron Perth'},inplace=True)
    df.replace({'KFLA':'Kingston'},inplace=True)
    df.replace({'Lambton_County':'Lambton'},inplace=True)
    df.replace({'Leeds_Grenville_Lanark':'Leeds, Grenville, Lanark'},inplace=True)
    df.replace({'Middlesex_London':'London'},inplace=True)
    df.replace({'Niagara_Region':'Niagara'},inplace=True)
    df.replace({'North_Bay_Parry_Sound_District':'North Bay'},inplace=True)
    df.replace({'Northwestern':'Northwestern'},inplace=True)
    df.replace({'City_of_Ottawa':'Ottawa'},inplace=True)
    df.replace({'Peel_Region':'Peel'},inplace=True)
    df.replace({'Peel Region':'Peel'},inplace=True)

    df.replace(['Peterborough_County_City','Peterborough County-City'],'Peterborough',inplace=True)
    df.replace({'Porcupine':'Porcupine'},inplace=True)
    df.replace({'Waterloo_Region':'Waterloo Region'},inplace=True)
    df.replace({'Renfrew_County_and_District':'Renfrew'},inplace=True)
    df.replace({'Simcoe_Muskoka_District':'Simcoe-Muskoka'},inplace=True)
    df.replace({'Southwestern':'Southwestern'},inplace=True)
    df.replace(['Sudbury_and_District','Sudbury And District'],'Sudbury',inplace=True)
    df.replace({'Thunder_Bay_District':'Thunder Bay'},inplace=True)
    df.replace({'Timiskaming':'Timiskaming'},inplace=True)
    df.replace({'Toronto':'Toronto PHU'},inplace=True)
    df.replace({'Wellington_Dufferin_Guelph':'Wellington-Guelph'},inplace=True)
    df.replace({'Windsor_Essex_County':'Windsor'},inplace=True)
    df.replace({'York_Region':'York'},inplace=True)
    df.replace({'York Region':'York'},inplace=True)


    df.replace({'Windsor-Essex County':'Windsor'},inplace=True)
    df.replace({'Lambton County':'Lambton'},inplace=True)
    df.replace({'Simcoe Muskoka District':'Simcoe-Muskoka'},inplace=True)
    df.replace({'City of Ottawa':'Ottawa'},inplace=True)
    df.replace({'Middlesex-London':'London'},inplace=True)
    df.replace({'Durham Region':'Durham'},inplace=True)

    return (df)


def RankCase():
    abcd = pd.DataFrame()

    df = pd.read_csv('https://data.ontario.ca/dataset/f4f86e54-872d-43f8-8a86-3892fd3cb5e6/resource/8a88fe6d-d8fb-41a3-9d04-f0550a44999f/download/daily_change_in_cases_by_phu.csv')
    df = df.T
    df.columns = df.iloc[0]
    df = df.loc[:,~df.columns.duplicated()]
    df = df.drop(['Date','Total'])
    df = df.dropna(axis=1)
    for columns in df.columns:
        abcd[columns] = df[columns].rank(method='min',ascending = False)

    xyz= pd.DataFrame(index = abcd.index,columns = range(1,35))
    xyz=xyz.fillna(0)

    for PHU in abcd.index:
        for date in abcd.columns:
            #print(PHU,date,abcd.loc[PHU][date])
            #xyz.loc[PHU][abcd.loc[PHU][date]] = xyz.loc[PHU][abcd.loc[PHU][date]]+1
            xyz.at[PHU,abcd.loc[PHU][date]]=xyz.at[PHU,abcd.loc[PHU][date]]+1
    print(len(abcd.columns))
    xyz = xyz.sort_values(by=[1,2,3,4,5,6],ascending = False)
    xyz.to_csv('Rank.csv')


def PostalCodeData():
    starttime = time.time()

    TextFileName = 'TextOutput/PostalCodeData.txt'
    ConsoleOut = sys.stdout

    abcn = pd.read_excel('https://www.ices.on.ca/~/media/Files/COVID-19/ICES-COVID19-Testing-Data-FSA-percent-positivity.ashx?la=en-CA', sheet_name='N', header = 28)
    abck = pd.read_excel('https://www.ices.on.ca/~/media/Files/COVID-19/ICES-COVID19-Testing-Data-FSA-percent-positivity.ashx?la=en-CA', sheet_name='K', header = 28)
    abcl = pd.read_excel('https://www.ices.on.ca/~/media/Files/COVID-19/ICES-COVID19-Testing-Data-FSA-percent-positivity.ashx?la=en-CA', sheet_name='L', header = 28)
    abcm = pd.read_excel('https://www.ices.on.ca/~/media/Files/COVID-19/ICES-COVID19-Testing-Data-FSA-percent-positivity.ashx?la=en-CA', sheet_name='M', header = 28)
    abcp = pd.read_excel('https://www.ices.on.ca/~/media/Files/COVID-19/ICES-COVID19-Testing-Data-FSA-percent-positivity.ashx?la=en-CA', sheet_name='P', header = 28)
    df = pd.DataFrame()
    df = df.append(abck)
    df = df.append(abcl)
    df = df.append(abcm)
    df = df.append(abcn)
    df = df.append(abcp)
    df = df.set_index('FSA')
    df = df.replace('Suppressed',0)

    df['Change in week rate'] = df['Overall - % positivity'] - df['Overall - % positivity'].shift(1)
    TodaysDF = df[(df['End date of week']==df['End date of week'].max())]
    #TodaysDF = TodaysDF[TodaysDF['Change in week rate']>0]
    TodaysDF = TodaysDF.sort_values(by='Overall - % positivity',ascending = False)

    PostalCodeVax = pd.read_excel('https://www.ices.on.ca/~/media/Files/COVID-19/ICES-COVID19-Vaccination-Data-by-FSA.ashx?la=en-CA', sheet_name='At least 1 Dose  by FSA', header = 25)
    PostalCodeVax = PostalCodeVax.set_index('FSA')
    PostalCodeVax['FirstDose'] = PostalCodeVax['% Vaccinated with at least 1 dose\n(All ages\nincluding <5 and undocumented age)']

    PostalCodeVax_2nd = pd.read_excel('https://www.ices.on.ca/~/media/Files/COVID-19/ICES-COVID19-Vaccination-Data-by-FSA.ashx?la=en-CA', sheet_name='2 doses by FSA', header = 30)
    PostalCodeVax_2nd = PostalCodeVax_2nd.set_index('FSA')
    PostalCodeVax_2nd['SecondDose'] = PostalCodeVax_2nd['% Fully vaccinated\n(All ages including <5 and undocumented age)']
    PostalCodeVax = PostalCodeVax.merge(PostalCodeVax_2nd,left_index=True,right_index = True,how ='left')

    sys.stdout = open(TextFileName,'w')
    print('**Postal Code Data** - [Source](https://www.ices.on.ca/DAS/AHRQ/COVID-19-Dashboard) - latest CASE data as of',
          TodaysDF['End date of week'].max().strftime('%B %d'), '- updated weekly')
    print()
    # print('This list is postal codes with **increases** in positive rates over last week')
    # for i in range(28):
    #     if (i % 7 == 0):
    #         print()
    #         print('* ',end = '')
    #     tempdf = TodaysDF[TodaysDF['Change in week rate']>0]
    #     if  not np.isnan(tempdf.iloc[i]['Overall - % positivity']):
    #         print('[',tempdf.index[i],'](https://www.bing.com/maps?q=',tempdf.index[i],'+postal+code)',': ',"{:,.1%}".format(tempdf.iloc[i]['Overall - % positivity']),'  ',sep = '',end='')
    # print()
    print()
    print('This list is postal codes with the **highest** positive rates')
    for i in range(105):
        if (i % 7 == 0):
            print()
            print('* ',end = '')
        tempdf = TodaysDF
        if  not np.isnan(tempdf.iloc[i]['Overall - % positivity']):
            #print('[',tempdf.index[i],'](https://www.bing.com/maps?q=',tempdf.index[i],'+postal+code)',': ',"{:,.1%}".format(tempdf.iloc[i]['Overall - % positivity']/1),'  ',sep = '',end='')
            print(tempdf.index[i],': ',"{:,.1%}".format(tempdf.iloc[i]['Overall - % positivity']/1),'  ',sep = '',end='')
    #"""
    print()
    print()
    print('This list is a list of **most vaccinated** postal codes (% of total population at least 1 dosed)')
    PostalCodeVax = PostalCodeVax.sort_values(by = 'FirstDose',ascending = False)
    for i in range(50):
        if (i % 5 == 0):
            print()
            print('* ',end = '')
        if  not np.isnan(PostalCodeVax.iloc[i]['FirstDose']):
            #print('[',PostalCodeVax.index[i],'](https://www.bing.com/maps?q=',PostalCodeVax.index[i],'+postal+code)',': ',"{:,.1%}".format(PostalCodeVax.iloc[i]['% Vaccinated with at least 1 dose\n(All ages, including <12 and undocumented age)']),'/',"{:,.1%}".format(PostalCodeVax.iloc[i]['SecondDose']),'  ',sep = '',end='')
            print(PostalCodeVax.index[i],': ',"{:,.1%}".format(PostalCodeVax.iloc[i]['FirstDose']),'/',"{:,.1%}".format(PostalCodeVax.iloc[i]['SecondDose']),'  ',sep = '',end='')

    print()
    print()
    print('This list is a list of **least vaccinated** postal codes (% of total population at least 1 dosed)')
    PostalCodeVax = PostalCodeVax.sort_values(by = 'FirstDose',ascending = True)
    for i in range(50):
        if (i % 5 == 0):
            print()
            print('* ',end = '')
        if  not np.isnan(PostalCodeVax.iloc[i]['FirstDose']):
            print(PostalCodeVax.index[i],': ',"{:,.1%}".format(PostalCodeVax.iloc[i]['FirstDose']),'/',"{:,.1%}".format(PostalCodeVax.iloc[i]['SecondDose']),'  ',sep = '',end='')

    sys.stdout = ConsoleOut
    print('------------------------------------------------------------------------')
    print('PostalCodeData: ',round(time.time()-starttime,2),'seconds')
    print('------------------------------------------------------------------------')


def UKData():
    import json
    import requests
    import ast

    starttime = time.time()
    d = json.loads(requests.get('https://coronavirus.data.gov.uk/api/v1/data?filters=areaType=nation;areaName=England&structure=%7B%22areaType%22:%22areaType%22,%22areaName%22:%22areaName%22,%22areaCode%22:%22areaCode%22,%22date%22:%22date%22,%22newCasesBySpecimenDateAgeDemographics%22:%22newCasesBySpecimenDateAgeDemographics%22%7D&format=json').text)
    OutputDF = pd.DataFrame()
    df1 = pd.json_normalize(d, max_level=9, record_path=['data'])

    UKCaseDF = pd.DataFrame()
    UKCase_RollingDF = pd.DataFrame()
    for i in range(len(df1)):
        x = pd.DataFrame(df1['newCasesBySpecimenDateAgeDemographics'][i]).T
        x.columns = x.iloc[0]
        x = x.drop('age')
        UKCaseDF = UKCaseDF.append(x.iloc[0])
        UKCase_RollingDF = UKCase_RollingDF.append(x.iloc[2])
    UKCaseDF['Total'] = UKCaseDF['00_59'] + UKCaseDF['60+']
    UKCase_RollingDF['Total'] = UKCase_RollingDF['00_59'] + UKCase_RollingDF['60+']

    # UKCaseDF = UKCaseDF.drop('00_59',axis=1)
    # UKCaseDF = UKCaseDF.drop('60+',axis=1)
    UKCaseDF.index = df1['date']
    UKCase_RollingDF.index = df1['date']
    UKCase_RollingDF.to_pickle('Pickle/EngCaseAvgPer100k.pickle')

    UKCaseDF = UKCaseDF.sort_values(by='date', ascending=True)
    for columns in UKCaseDF.columns:
        OutputDF[columns + '_Cases_SMA14-28'] = UKCaseDF[columns].shift(14).rolling(14).mean()
        OutputDF[columns + '_Cases_SMA7'] = UKCaseDF[columns].rolling(7).mean()
    UKCaseDF = UKCaseDF.sort_values(by='date', ascending=False)
    OutputDF = OutputDF.sort_values(by='date', ascending=False)
    OutputDF.index = UKCaseDF.index

    # UKCaseDF.to_csv('aaa.csv')
    print('------------------------------------------------------------------------')
    print('EnglandData - cases: ', round(time.time() - starttime, 2), 'seconds')

    d = pd.read_csv('https://coronavirus.data.gov.uk/api/v1/data?filters=areaType=nation;areaName=England&structure=%7B%22areaType%22:%22areaType%22,%22areaName%22:%22areaName%22,%22areaCode%22:%22areaCode%22,%22date%22:%22date%22,%22newDeaths28DaysByDeathDateAgeDemographics%22:%22newDeaths28DaysByDeathDateAgeDemographics%22%7D&format=csv')
    d.to_csv('SourceFiles/UKData-EnglandDeaths.csv')

    UKDeathDF = pd.DataFrame()

    for i in range(len(d)):
        x = pd.DataFrame(ast.literal_eval(d['newDeaths28DaysByDeathDateAgeDemographics'][i])).T
        x.columns = x.iloc[0]
        x = x.drop('age')
        UKDeathDF = UKDeathDF.append(x.iloc[0])
    UKDeathDF['Total'] = UKDeathDF['00_59'] + UKDeathDF['60+']
    # UKDeathDF = UKDeathDF.drop('00_59',axis=1)
    # UKDeathDF = UKDeathDF.drop('60+',axis=1)
    UKDeathDF.index = d['date']

    OutputDF = OutputDF.sort_values(by='date', ascending=True)
    UKDeathDF = UKDeathDF.sort_values(by='date', ascending=True)
    for columns in UKDeathDF.columns:
        OutputDF[columns + 'Deaths_SMA14'] = UKDeathDF[columns].rolling(14).mean()
    UKDeathDF = UKDeathDF.sort_values(by='date', ascending=False)
    OutputDF = OutputDF.sort_values(by='date', ascending=True)

    # UKDeathDF.to_csv('aaa1.csv')
    # OutputDF.to_csv('UKData.csv')

    print('EnglandData - deaths: ', round(time.time() - starttime, 2), 'seconds')
    print('------------------------------------------------------------------------')


def COVIDCharts():
    starttime = time.time()
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import datetime
    import matplotlib.ticker as ticker
    from matplotlib.axis import Axis
    import matplotlib
    import math
    plt.rcdefaults()

    chartSize = (9, 5)
    # plt.legend(fontsize="small")
    dfCaseStatus = pd.read_pickle('Pickle/OntarioCaseStatus.pickle')
    dfCaseStatus = dfCaseStatus.reset_index()
    df = dfCaseStatus
    plt.xkcd(0.5, 5, 0.1)
    plt.rcParams.update({'font.family': 'sans-serif'})

    # plt.rcParams['ytick.major.width'] = 1
    # plt.rcParams['ytick.major.size'] = 8
    # plt.rcParams['axes.linewidth'] = 1.5
    # plt.rcParams['lines.linewidth'] = 0.1
    plt.rcParams['path.effects'] = [matplotlib.patheffects.withStroke(linewidth=1, foreground="w")]

    ################################################################################################
    ################################################################################################
    # Vaccine progress chart
    dfMaster = pd.read_pickle('Pickle/VaccineAgeAll.pickle')
    dfMaster = dfMaster.reset_index()

    df = dfMaster[dfMaster['Agegroup'] == 'Ontario_12plus'].copy()
    df['Date'] = pd.to_datetime(df['Date'])
    TodaysDate = df['Date'].max()

    df['TwoDose'] = df['Percent_fully_vaccinated']
    df['OneDose'] = df['Percent_at_least_one_dose'] - df['TwoDose']
    df['ZeroDose'] = 1 - df['Percent_at_least_one_dose']
    df = df.fillna(0)

    plt.figure(figsize=chartSize)
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))

    formatter = (ticker).PercentFormatter()
    plt.gca().yaxis.set_major_formatter(formatter)
    plt.grid(linewidth=0.55)
    plt.ylim(0, 100 + 1)
    plt.xlim(df['Date'].min() - datetime.timedelta(days=1),
             df['Date'].max() + datetime.timedelta(days=1))
    print(df['Date'].max())

    plt.stackplot(df['Date'], 100 * df['TwoDose'], df['OneDose'] * 100, df['ZeroDose'] * 100,
                  labels=['Two dose', 'One Dose', 'Unvax'])
    plt.legend(loc=(0.07, 0.05))
    ax = plt.gca()
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles[::-1], labels[::-1], loc='upper left')  # reverse legend order

    plt.title(f"Ontario vaccinated rates - 12+ as at {TodaysDate:%B %d}")
    plt.tight_layout()
    plt.savefig('ChartImages/2-OntarioVaccineProgressAll.png')
    plt.show()

    df = df.head(100).copy()
    plt.figure(figsize=chartSize)

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))

    formatter = (ticker).PercentFormatter()
    plt.gca().yaxis.set_major_formatter(formatter)
    plt.grid(linewidth=0.55)
    plt.ylim(0, 100 + 1)
    plt.xlim(df['Date'].min() - datetime.timedelta(days=3),
             df['Date'].max() + datetime.timedelta(days=3))

    plt.stackplot(df['Date'], 100 * df['TwoDose'], df['OneDose'] * 100, df['ZeroDose'] * 100,
                  labels=['Two dose', 'One Dose', 'Unvax'])
    plt.legend(loc=(0.07, 0.05))
    ax = plt.gca()
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles[::-1], labels[::-1], loc='upper left')

    plt.title(f"Ontario vaccinated rates - 12+ as at{TodaysDate:%B %d} - \n Last 100 days")
    plt.tight_layout()
    plt.savefig('ChartImages/3-OntarioVaccineProgress_100Days.png')
    plt.show()

    ################################################################################################
    ################################################################################################
    # Vaccines - % of remaining vaxxed each week
    df = dfMaster[dfMaster['Agegroup'] == 'Ontario_12plus'].copy()
    df = df.reset_index()
    Color_Cases = 'blue'
    Color_ICU = 'red'
    fig, ax1 = plt.subplots(figsize=(chartSize))
    # ax2=ax1.twinx()
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    # ax1.set_ylim(0,df['PercentOfRemaining_First - in last week'].max()*1)
    ax1.set_ylim(0, 0.2)
    # print(df['PercentOfRemaining_First - in last week'].max())
    print(df['PercentOfRemaining_SecondElig - in last week'].max())

    formatter = (ticker).PercentFormatter(xmax=1, decimals=0)
    ax1.yaxis.set_major_formatter(formatter)
    # ax2.yaxis.set_major_formatter(formatter)

    # ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    # ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))

    # ax2.set_ylim(0,ax1.get_ylim()[1]/4)
    Max_AX1 = (int)(math.ceil(ax1.get_ylim()[1] / 1000) * 1000)

    ax1.grid(linewidth=0.5, color='grey')
    # ax1.set_yticks(list(range(0,Max_AX1,Max_AX1//5)))
    # ax2.set_yticks(list(range(0,Max_AX1//4,Max_AX1//(5*4))))

    ax1.plot(df['Date'], df['PercentOfRemaining_First - in last week'], color=Color_Cases,
             label='First doses % of unvax')
    SecondDoseDF = df[df['Date'] >= datetime.datetime(2021, 3, 1)]
    ax1.plot(SecondDoseDF['Date'], SecondDoseDF['PercentOfRemaining_SecondElig - in last week'],
             color='red', label='Second doses % of eligible')
    ax1.legend(loc=(0.05, 0.9), fontsize='small')
    # ax2.legend(loc=(0.05,0.83),fontsize='small')
    # ax1.set_ylabel('Cases',color = Color_Cases)
    # ax2.set_ylabel('ICUs',color = Color_ICU)
    ax1.set_xlim(df['Date'].min() - datetime.timedelta(days=5),
                 df['Date'].max() + datetime.timedelta(days=5))
    # ax2.grid(color='black')
    ax1.annotate(f"This week:\n{df['PercentOfRemaining_First - in last week'][0]:.2%}",
                 xy=(mdates.date2num(df['Date'][0]), df['PercentOfRemaining_First - in last week'][0]),
                 xytext=(-30, -40), textcoords='offset points', arrowprops=dict(arrowstyle='-|>'),
                 fontsize='x-small', horizontalalignment='center')

    df = df.set_index('Date')
    SecondDoseDF = SecondDoseDF.set_index('Date')
    VaxPassDate = datetime.datetime(2021, 9, 1)
    ax1.annotate(f"Vaccine Passport\n announced:\n{datetime.datetime(2021,9,1):%b %d}",
                 xy=(mdates.date2num(datetime.datetime(2021, 9, 1)),
                     df.loc[VaxPassDate]['PercentOfRemaining_First - in last week']),
                 xytext=(-30, 30), textcoords='offset points', arrowprops=dict(arrowstyle='-|>'),
                 fontsize='x-small', horizontalalignment='center')
    ax1.annotate(f". ", xy=(mdates.date2num(datetime.datetime(2021, 9, 1)),
                            SecondDoseDF.loc[VaxPassDate]['PercentOfRemaining_SecondElig - in last week']),
                 xytext=(-30, -30), textcoords='offset points', arrowprops=dict(arrowstyle='-|>'),
                 fontsize='x-small', horizontalalignment='center')

    # ax1.annotate(f"OP gets vaxxed \n {datetime.datetime(2021,5,18):%b %d}",xy=(mdates.date2num(datetime.datetime(2021,5,18)), df.loc[datetime.datetime(2021,5,18)]['PercentOfRemaining_First - in last week']),xytext = (-60,-20),
    #         textcoords='offset points', arrowprops=dict(arrowstyle='-|>'),fontsize='x-small',horizontalalignment='center')
    ax1.axvline(x=10, color='r', linestyle='-', linewidth=3)

    fig.suptitle(f"% of unvaxed pop'n first vaxxed and \n % of eligib pop'n second vaxxed this week\n (to {TodaysDate: %B %d})",
                 fontsize=12)

    ax1.set_facecolor('0.95')
    plt.tight_layout()
    plt.savefig('ChartImages/6-Vaccine_RemainingPctWeek.png')
    plt.show()

    ##############################################################
    ##############################################################
    # Cases and ICUs chart

    df = dfCaseStatus.copy()
    TodaysDate = df['Reported Date'].max()

    Color_Cases = 'blue'
    Color_ICU = 'red'
    Color_Hosp = 'grey'
    fig, ax1 = plt.subplots(figsize=(chartSize))
    ax2 = ax1.twinx()
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax1.set_ylim(0, df['7 day SMA'].max() * 1)

    formatter = (ticker).StrMethodFormatter('{x:,.0f}')
    ax1.yaxis.set_major_formatter(formatter)
    ax2.yaxis.set_major_formatter(formatter)

    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))

    ax2.set_ylim(0, ax1.get_ylim()[1] / 4)
    Max_AX1 = (int)(math.ceil(ax1.get_ylim()[1] / 1000) * 1000)

    ax1.grid(linewidth=0.5, color='grey')
    ax1.set_yticks(list(range(0, Max_AX1 + 1, Max_AX1 // 5)))
    ax2.set_yticks(list(range(0, Max_AX1 // 4, Max_AX1 // (5 * 4))))

    ax1.plot(df['Reported Date'], df['7 day SMA'], color=Color_Cases,
             label='Cases - 7 day avg')
    ax2.plot(df['Reported Date'], df['Number of patients in ICU due to COVID-19'],
             color=Color_ICU, label='ICU count')
    ax1.legend(loc=(0.05, 0.9), fontsize='small')
    ax2.legend(loc=(0.05, 0.83), fontsize='small')
    ax1.set_ylabel('Cases', color=Color_Cases)
    ax2.set_ylabel('ICUs', color=Color_ICU)
    ax1.set_xlim(df['Reported Date'].min() - datetime.timedelta(days=5),
                 df['Reported Date'].max() + datetime.timedelta(days=5))
    # ax2.grid(color='black')

    fig.suptitle(f"Cases and ICUs - to {TodaysDate: %B %d}", fontsize=18)

    ax1.set_facecolor('0.95')
    plt.tight_layout()
    plt.savefig('ChartImages/0-7 day average.png')
    plt.show()

    ##############################################################
    ##############################################################
    # Year over year chart of 7 day average

    # piv = pd.pivot_table(df, index=['doy'],columns=['Year'], values=['7 day SMA'])
    # piv.index = pd.date_range(start = '2020-01-01',end='2020-12-31')
    # piv.plot()
    fig, ax1 = plt.subplots(figsize=(chartSize))
    # ax2=ax1.twinx()
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax1.set_ylim(0, df['7 day SMA'].max() * 1)

    formatter = (ticker).StrMethodFormatter('{x:,.0f}')
    ax1.yaxis.set_major_formatter(formatter)
    # ax2.yaxis.set_major_formatter(formatter)

    # ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    # ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))

    # ax2.set_ylim(0,ax1.get_ylim()[1]/4)
    Max_AX1 = (int)(math.ceil(ax1.get_ylim()[1] / 1000) * 1000)

    ax1.grid(linewidth=0.5, color='grey')
    ax1.set_yticks(list(range(0, Max_AX1 + 1, Max_AX1 // 5)))
    # ax2.set_yticks(list(range(0,Max_AX1//4,Max_AX1//(5*4))))
    # df['doy'] = pd.to_datetime('2020' + df['doy'].astype(str), format='%Y%j')
    # df['doy'] = pd.to_datetime('2020' + df['doy'].astype(str), format='%Y%j')

    df2020 = df[df['Year'] == 2020]
    df2021 = df[df['Year'] == 2021]
    df2022 = df[df['Year'] == 2022]

    # ax1.plot(CustomXAxis,df2020['7 day SMA'],color = 'blue',label = '2020')
    ax1.plot(df2020['doy'], df2020['7 day SMA'], color='blue', label='2020')
    ax1.plot(df2021['doy'], df2021['7 day SMA'], color='red', label='2021')
    ax1.plot(df2022['doy'], df2022['7 day SMA'], color='orange', label='2022')

    ax1.legend(loc=(0.05, 0.8), fontsize='small')
    ax1.set_ylabel('Cases', color=Color_Cases)
    # ax2.set_ylabel('ICUs',color = Color_ICU)
    ax1.set_xlim(left=df['doy'].min(), right=df['doy'].max())
    # ax1.set_xticks((2,23))

    DateRange = pd.date_range(start='2020-01-01', end='2020-12-31')

    print(ax1.xaxis.get_data_interval())
    # ax2.grid(color='black')

    fig.suptitle(f"Cases YoY 7 day avg. - to {TodaysDate:%B %d}", fontsize=18)

    ax1.set_facecolor('0.95')
    plt.tight_layout()
    plt.savefig('ChartImages/0-YoY 7 day average.png')
    plt.show()

    ##############################################################
    ##############################################################
    # Cases and ICUs chart - last 100 days
    df = df.head(100)
    df = df.reset_index()
    Color_Cases = 'blue'
    Color_ICU = 'red'
    fig, ax1 = plt.subplots(figsize=(chartSize))
    ax2 = ax1.twinx()
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=1))

    ax1.set_ylim(0, df['7 day SMA'].max() * 1)

    formatter = (ticker).StrMethodFormatter('{x:,.0f}')
    ax1.yaxis.set_major_formatter(formatter)
    ax2.yaxis.set_major_formatter(formatter)

    Max_AX1 = (int)(math.ceil((df['7 day SMA'].max() / 100)) * 100) + 1

    Max_AX2 = (int)(math.ceil((df['Number of patients in ICU due to COVID-19'].max() / 100)) * 100) + 1

    ax1.set_ylim(0, Max_AX1)
    ax2.set_ylim(0, Max_AX2)

    ax1.grid(linewidth=0.5, color='grey')
    ax1.set_yticks(list(range(0, Max_AX1 + 1, Max_AX1 // 4)))
    ax2.set_yticks(list(range(0, Max_AX2, Max_AX2 // 4)))

    ax1.plot(df['Reported Date'], df['7 day SMA'], color=Color_Cases, label='Cases - 7 day avg')
    ax2.plot(df['Reported Date'], df['Number of patients in ICU due to COVID-19'], color=Color_ICU,
             label='ICU count')
    ax1.legend(loc=(0.05, 0.9), fontsize='small')
    ax2.legend(loc=(0.05, 0.83), fontsize='small')
    ax1.set_ylabel('Cases', color=Color_Cases)
    ax2.set_ylabel('ICUs', color=Color_ICU)
    ax1.set_xlim(df['Reported Date'].min() - datetime.timedelta(days=3), df['Reported Date'].max()
                 + datetime.timedelta(days=3))

    # ax2.grid(color='black')

    fig.suptitle(f"Cases and ICUs to {TodaysDate:%B %d} \n Last 100 days", fontsize=18)

    ax1.set_facecolor('0.95')
    plt.tight_layout()
    plt.savefig('ChartImages/1-7 day average_Last100.png')
    plt.show()

    ##############################################################
    ##############################################################
    # Rt estimate
    # plt.rcdefaults()

    df = dfCaseStatus
    df = df.iloc[0:df.shape[0] - 70]
    df = df.head(100)

    plt.figure(figsize=chartSize)
    plt.xlim(df['Reported Date'].min() - datetime.timedelta(days=3),
             df['Reported Date'].max() + datetime.timedelta(days=3))

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))

    plt.rcParams.update({'axes.facecolor': '0.8'})

    # plt.fill_between(df['Reported Date'],df['Rt estimate'])
    plt.plot(df['Reported Date'], df['Rt estimate'], color='steelblue')
    plt.axhline(y=1, color='r', linestyle='-', linewidth=3)
    # plt.gca().xaxis.set_facecolor('0.95')
    ax = plt.gca()
    ax.annotate(f"Today's Rt:\n{df['Rt estimate'][0]:.2f}",
                xy=(mdates.date2num(df['Reported Date'][0]), df['Rt estimate'][0]),
                xytext=(-30, -40), textcoords='offset points', arrowprops=dict(arrowstyle='-|>'),
                fontsize='x-small', horizontalalignment='center')
    Max_Ax = (int)((math.ceil(ax.get_ylim()[1] / 0.1)))
    Min_Ax = (int)((math.floor(ax.get_ylim()[0] / 0.1)))
    # print(ax.get_ylim(),Max_Ax,Min_Ax)
    # print(list(range(Min_Ax-1,Max_Ax+1,1)))
    ytickRange = []
    for i in range(Min_Ax - 1, Max_Ax + 1, 1):
        ytickRange.append(i / 10)

    ax.set_yticks(ytickRange)

    plt.title(f"Unofficial Rt estimate at {TodaysDate:%B %d} \n Last 100 days")
    plt.tight_layout()

    plt.grid(linewidth=0.5)
    plt.savefig('ChartImages/4-Rtestimate_Last100.png')

    ##############################################################
    ##############################################################
    # Year over year chart of Rt estimate

    df = dfCaseStatus.copy()
    df = df.iloc[0:df.shape[0] - 70]

    fig, ax1 = plt.subplots(figsize=(chartSize))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=1))

    formatter = (ticker).StrMethodFormatter('{x:,.1f}')
    ax1.yaxis.set_major_formatter(formatter)

    Max_AX1 = (int)(math.ceil(ax1.get_ylim()[1] / 1) * 1)

    ax1.grid(linewidth=0.5, color='grey')

    df['doy'] = pd.to_datetime('2020' + df['doy'].astype(str), format='%Y%j')

    df2020 = df[df['Year'] == 2020]
    df2021 = df[df['Year'] == 2021]
    df2022 = df[df['Year'] == 2022]

    ax1.plot(df2020['doy'], df2020['Rt estimate'], color='tab:brown', label='2020')
    ax1.plot(df2021['doy'], df2021['Rt estimate'], color='tab:blue', label='2021')
    ax1.plot(df2022['doy'], df2022['Rt estimate'], color='tab:orange', label='2022')
    plt.axhline(y=1, color='red', linestyle='-', linewidth=3)

    ax1.legend(loc=(0.05, 0.8), fontsize='small')
    ax1.set_ylabel('Cases', color=Color_Cases)
    ax1.set_xlim(left=df['doy'].min(), right=df['doy'].max())

    DateRange = pd.date_range(start='2020-01-01', end='2020-12-31')

    print(ax1.xaxis.get_data_interval())

    fig.suptitle(f"Rt estimate - YoY - to {TodaysDate:%B %d}", fontsize=18)

    ax1.set_facecolor('0.95')
    plt.tight_layout()
    plt.savefig('ChartImages/7-YoY Rt Estimate.png')
    plt.show()

    ##############################################################
    ##############################################################
    # New hospitalization and ICU chart
    df = pd.read_pickle('Pickle/OntarioCaseStatus.pickle')
    df = df.reset_index()
    # df = df.head(100)

    Color_ICU = 'red'
    Color_Hosp = 'grey'
    df = df.sort_values(by='Reported Date', ascending=True)
    df['NewHosp_7SMA'] = df['NewHosp'].rolling(7).mean()
    df['NewICU_7SMA'] = df['NewICU'].rolling(7).mean()
    df = df.sort_values(by='Reported Date', ascending=False)

    fig, ax1 = plt.subplots(figsize=(chartSize))
    ax2 = ax1.twinx()
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax1.set_ylim(0, df['7 day SMA'].max() * 1)
    ax2.set_ylim(0, df['NewHosp_7SMA'].max() * 1)
    # ax2.set_ylim(0,222)

    formatter = (ticker).StrMethodFormatter('{x:,.0f}')
    ax1.yaxis.set_major_formatter(formatter)
    ax2.yaxis.set_major_formatter(formatter)

    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))

    Max_AX1 = (int)(math.ceil(ax1.get_ylim()[1] / 1000) * 1000)
    Max_AX2 = (int)(math.ceil(ax2.get_ylim()[1] / 100) * 100)

    ax1.grid(linewidth=0.5, color='grey')
    ax1.set_yticks(list(range(0, Max_AX1 + 1, Max_AX1 // 5)))
    ax2.set_yticks(list(range(0, Max_AX2, Max_AX2 // 5)))

    ax1.plot(df['Reported Date'], df['7 day SMA'], color=Color_Cases, label='Cases - 7 day avg')
    ax2.plot(df['Reported Date'], df['NewICU_7SMA'], color=Color_ICU, label='New ICUs')
    ax2.plot(df['Reported Date'], df['NewHosp_7SMA'], color=Color_Hosp,
             label='New Hospitalizations')
    ax1.legend(loc=(0.05, 0.9), fontsize='small')
    ax2.legend(loc=(0.05, 0.73), fontsize='small')
    ax1.set_ylabel('Cases', color=Color_Cases)
    ax2.set_ylabel('New ICUs', color=Color_ICU)
    ax1.set_xlim(df['Reported Date'].min() - datetime.timedelta(days=5),
                 df['Reported Date'].max() + datetime.timedelta(days=5))
    #ax2.grid(color='black')

    fig.suptitle(f"New cases, hospitalizations and ICU admits \n- to {TodaysDate: %B %d}",
                 fontsize=18)

    ax1.set_facecolor('0.95')
    plt.tight_layout()
    plt.savefig('ChartImages/5-CasesNewHospICU.png')

    ##############################################################
    ##############################################################
    from ImgurScripts import ImgurImplementation
    import os
    albumID = 'qI0P0X5'

    imgurClient = ImgurImplementation.authenticate()
    # ImgurImplementation.RemoveAllImages_Album(imgurClient, albumID)
    ImgurImplementation.DeleteAllImages_Album(imgurClient, albumID)

    for files in os.listdir('ChartImages'):
        print(files)
        ImgurImplementation.upload_image(imgurClient, 'ChartImages/' + files, albumID)

    print('------------------------------------------------------------------------')
    print('COVIDCharts: ', round(time.time() - starttime, 2), 'seconds')
    print('------------------------------------------------------------------------')


def RTData():
    """
    This function downloads the latest RT Report and creates a pickle out of it.

    Returns
    -------
    None.
    """
    starttime = time.time()
    print('------------------------------------------------------------------------')
    print(f'RT Report \nStarted: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}')
    pickle_location = 'Pickle/RTData.pickle'

    df = pd.read_csv('https://data.ontario.ca/dataset/8da73272-8078-4cbd-ae35-1b5c60c57796/resource/1ffdf824-2712-4f64-b7fc-f8b2509f9204/download/re_estimates_on.csv')
    df['date_end'] = pd.to_datetime(df['date_end'])
    df = df.set_index('date_end')
    df['Re'].to_pickle(pickle_location)

    endtime = datetime.datetime.now()
    print(f'Ended:   {endtime:%Y-%m-%d %H:%M:%S} {round(time.time() - starttime, 2)} seconds')
    print('------------------------------------------------------------------------')


async def PositiveCaseFileDownload_Async(filename):
    starttime = time.time()
    print('Starting PositiveCase file download')
    df = pd.read_csv('https://data.ontario.ca/dataset/f4112442-bdc8-45d2-be3c-12efae72fb27/resource/455fd63b-603d-4608-8216-7d8647f43350/download/conposcovidloc.csv')
    path = 'Async/'
    df.to_csv(os.path.join(path,(filename+' Data.csv')), index=False)
    del df
    print('File downloaded (seconds):', round(time.time()-starttime))


async def DownloadFile_Async(filename):
    starttime = time.time()
    Task_PositiveCaseFileDownload = asyncio.create_task(PositiveCaseFileDownload(filename))
    print('Working on vaccine data now')
    VaccineData()
    JailData()
    COVIDAppData()
    OutbreakData()
    LTCData()
    print('Waiting for main data file to download now:')
    await Task_PositiveCaseFileDownload
    LoadCOVIDData(filename)
    DailyReports_Individual(filename)

    DailyReports_Compile()
    print('Time taken to do all work (seconds):',round(time.time()-starttime))


def SplitMasterDataFrame():
    """
    This is not a working method - it is really just a hold for some adhoc code used to split
    the MasterDataFrame back in the day

    Returns
    -------
    None.

    """
    path = config.get('folder_location', 'indiv_pickles')
    MasterDateFrame_DateRange = pd.date_range(start=MasterDataFrame['File_Date'].min(),
                                              end=MasterDataFrame['File_Date'].max())
    for date in MasterDateFrame_DateRange:
        print(date)
        tempDF = MasterDataFrame[MasterDataFrame['File_Date'] == date].copy()
        tempDF.to_pickle(path + '\\' + date.strftime("%Y-%m-%d") + ' - Source.pickle')


def StartupImports():
    import numpy as np
    import pandas as pd
    import datetime
    import time
    import os
    import openpyxl
    from csv import writer
    import COVIDProcedures as cp
    pd.options.display.max_columns = 10
    pd.options.display.float_format = '{:,.2f}'.format
    import us_state_abbrev as stateabbrev
    import calendar
    import sys


def suffix(d):
    return 'th' if 11<=d<=13 else {1:'st',2:'nd',3:'rd'}.get(d%10, 'th')

def custom_strftime(format, t):
    return t.strftime(format).replace('{S}', str(t.day) + suffix(t.day))

def Insert_row(row_number, df, row_value):
    # Starting value of upper half
    start_upper = 0

    # End value of upper half
    end_upper = row_number

    # Start value of lower half
    start_lower = row_number

    # End value of lower half
    end_lower = df.shape[0]

    # Create a list of upper_half index
    upper_half = [*range(start_upper, end_upper, 1)]

    # Create a list of lower_half index
    lower_half = [*range(start_lower, end_lower, 1)]

    # Increment the value of lower half by 1
    lower_half = [x.__add__(1) for x in lower_half]

    # Combine the two lists
    index_ = upper_half + lower_half

    # Update the index of the dataframe
    df.index = index_

    # Insert a row at the end
    df.loc[row_number] = row_value

    # Sort the index labels
    df = df.sort_index()

    # return the dataframe
    return df