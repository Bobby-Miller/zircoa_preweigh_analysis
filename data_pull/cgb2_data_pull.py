"""
Uses data from CGB2.xlsx, and uses that information to evaluate preweigh.
(Customer-driven analysis.)
"""

import pandas as pd
from collections import Counter
import matplotlib.pyplot as plt
from matplotlib import pylab, style
import numpy as np
import datetime
import pickle

style.use('bmh')


class CGBBatchProduced:
    def __init__(self, comp=None):
        """
        Initialize class. Pull batch dataframe based on composition.
        """
        # Initialize class variables
        self._comp = comp
        
        # Initialize Excel portal
        xls = pd.ExcelFile('O:\Plant\CGB2.xls')
        
        # Pull dataframe based on comp type.
        if comp == '3077':
            excel_df = xls.parse('3077')
            self._batch_col = 'Batch_No'
            self._comp_df = excel_df[excel_df[self._batch_col].notnull()]
        elif comp == 'milled_russian':
            excel_df = xls.parse('milled Russian')
            self._batch_col = 'Batch_No.'
            self._comp_df = excel_df[excel_df[self._batch_col].notnull()]
        else:
            excel_df = xls.parse('CG mixes-Orig')
            self._batch_col = 'Batch_No'
            self._comp_df = excel_df[((excel_df['F'] == self._comp) &
                                     (excel_df[self._batch_col].notnull()))]
    
    def get_comp_list(self):
        """
        Provide a list of comps available to evaluate with this class.
        """
        
        # Initialize comp list with comp types in tabs other than the CG mixes
        # tab.
        comp_list = ['3077', 'milled_russian']
        
        # Parse the excel tab 'CG mixes=Orig'
        xls = pd.ExcelFile('O:\Plant\CGB2.xls')
        excel_df = xls.parse('CG mixes-Orig')
        cg_df = excel_df[(excel_df[self._batch_col].notnull())]
        
        # Get unique comps from the 'F' series in cg_df:
        unique_comps = cg_df['F'].unique()
        for comp in unique_comps:
            if len(str(comp)) == 4:
                comp_list.append(comp)
        return comp_list
    
    def batches_made_by_date(self, start='1/1/2010', end=None):
        """
        Returns a series with the number of batches of the specified comp made
        on a given date. If date is not given, batches made = 0
        :param start: (str) Date, formatted as (dd/mm/yyyy), indicates the
        start date for the series.
        :param end: (str) Date, formatted as (dd/mm/yyyy), indicates the
        end date for the series.
        :return: (Series) A series within the chosen start and end date, with
        the number of batches made of the class comp, on each date.
        """
        # Pull the dates from the comp_df and reformat them to match datetime
        # formatting requirements.
        date_list = []
        start_date = pd.to_datetime(start)
        if end is None:
            end_date = pd.to_datetime(datetime.date.today())
        else:
            end_date = pd.to_datetime(end)

        for batch in self._comp_df[self._batch_col]:
            if batch != 'Do not use' and len(str(batch)) == 8:
                formatted_date = '20' + str(batch)[0:6]
                try:
                    date = pd.to_datetime(formatted_date, format='%Y%m%d')
                    date_list.append(date)
                except ValueError:
                    pass

        # Use the counter function to count the number of times each date is
        # listed. This provides us with the # of batches per day.
        date_counter = Counter()
        for date in date_list:
            if start_date <= date < end_date:
                date_counter[date] += 1

        # Build a 0 series with all dates from start to end.
        zero_series = pd.Series(0, pd.date_range(start_date, end_date))

        # Convert the date_counter dict made above to a series format.
        date_counter_s = pd.Series(date_counter)

        joined_df = pd.concat([date_counter_s, zero_series], axis=1)
        joined_s = joined_df[0].fillna(value=0)
        return joined_s


def prod_time(num_batches, comp, find_lot=7.5, pull_mat=2.75,
              weigh_mat=5.0, return_mat=2.75, weigh_minor=5.0):
    """
    Calculates the time required to produce num_batches of given comp
    :param num_batches: (int) number of batches of a given comp to produce.
    :param comp: (string) Select available lot ('3077', '3001', '3004', '1968',
    '1651')
    :param find_lot: (float) time, in minutes, to find major mats. Default: 7.5
    :param pull_mat: (float) time, in minutes, to pull major mats.
    Default: 2.75
    :param weigh_mat: (float) time, in minutes, to weigh major mats.
    Default: 5.0
    :param return_mat: (float) time, in minutes, to return major mats.
    Default: 2.75
    :param weigh_minor: (float) time, in minutes, to weigh minor mats.
    Default: 5.0
    :return: (float) majors production time (hours), (float) minors production
    time (hours)
    """
    # dict representing number of lots of majors and minors, respectively
    comp_lots = {'3077': (2, 4), '3001': (4, 7), '1968': (9, 4),
                 '3004': (4, 7), '1651': (4, 3)}

    major_lots = comp_lots[comp][0]
    minor_lots = comp_lots[comp][1]

    # Time to find lots
    major_prod_time = major_lots * find_lot
    # Time to pull and return lots
    major_prod_time += major_lots * (pull_mat + return_mat)

    # Time to weigh lots
    major_prod_time += weigh_mat * num_batches * major_lots

    # Time to produce minors
    minor_prod_time = weigh_minor * num_batches * minor_lots

    # Weird way to zero out pull/return/find values if num_batches == 0.
    # Not able to deal with numpy arrays another way.
    major_prod_time[major_prod_time == major_lots * (pull_mat +
                    return_mat + find_lot)] = 0

    # Convert to hours on the return.
    return major_prod_time/60, minor_prod_time/60


def all_comp_batches_made_df(start_date, end_date):
    """
    Builds a DataFrame matrix, indexed by date, showing each comp usage by
    date.
    NOTE: Pickled as a 2015-to-date dataframe, and used in subsequent
    functions.
    :param start_date: (str) Date, formatted as (dd/mm/yyyy), indicates the
    start date for the series.
    :param end_date: (str) Date, formatted as (dd/mm/yyyy), indicates the
    end date for the series.
    :return: (dataframe) lists batches produced by date of each comp.
    """
    build_df = pd.DataFrame(index=pd.date_range(start_date, end_date))
    comp_list = ['3077', '3001', '3004', '1968', '1651', '2004', '6105',
                 '2073', '1661', '6101', '2290', '3036']
    for comp in comp_list:
        build_df[comp] = CGBBatchProduced(comp).batches_made_by_date(
            start_date, end_date)
    return build_df


def current_state_batch_example():
    """
    An example dataframe used to model our current preweigh process operation.
    :return: (dataframe) hypothetical 2-week preweigh production.
    """
    batch_dict = {pd.to_datetime('10-9-15'): [0, 0, 0, 0, 0],
                  pd.to_datetime('10-10-15'): [10, 0, 0, 0, 0],
                  pd.to_datetime('10-11-15'): [0, 5, 0, 0, 0],
                  pd.to_datetime('10-12-15'): [0, 5, 0, 0, 0],
                  pd.to_datetime('10-13-15'): [0, 0, 5, 0, 0],
                  pd.to_datetime('10-14-15'): [0, 0, 0, 0, 0],
                  pd.to_datetime('10-15-15'): [0, 0, 0, 0, 0],
                  pd.to_datetime('10-16-15'): [0, 0, 5, 0, 0],
                  pd.to_datetime('10-17-15'): [0, 0, 0, 4, 0],
                  pd.to_datetime('10-18-15'): [0, 0, 0, 4, 0],
                  pd.to_datetime('10-19-15'): [0, 0, 0, 4, 0],
                  pd.to_datetime('10-20-15'): [0, 0, 0, 4, 0],
                  }
    build_df = pd.DataFrame(batch_dict).transpose()
    build_df.columns = ['3077', '3001', '3004', '1968', '1651']
    return build_df


def batch_current_future_time_analysis(batch_df):
    """
    HIGHLY APPLICATION-SPECIFIC
    Takes a batches_produced dataframe, and builds a matplotlib chart, which
    shows a current state time analysis, a future state time analysis, and a
    bar chart showing batches produced per day. Exclusive to the 'main 5'
    comps: '3077', '3001', '3004', '1968', '1651'
    :param batch_df: (dataframe) A dataframe with the number of batches
    produced by day.  Must include the 'main 5' comps.
    :return: No return. Displays a graph.
    """
    # Create data points for current state
    time_3077_major, time_3077_minor = prod_time(batch_df['3077'], '3077')
    time_3001_major, time_3001_minor = prod_time(batch_df['3001'], '3001')
    time_3004_major, time_3004_minor = prod_time(batch_df['3004'], '3004')
    time_1968_major, time_1968_minor = prod_time(batch_df['1968'], '1968')
    time_1651_major, time_1651_minor = prod_time(batch_df['1651'], '1651')

    total_minor = (time_3077_minor + time_3001_minor + time_3004_minor +
                   time_1968_minor + time_1651_minor)
    total_major = (time_3077_major + time_3001_major + time_3004_major +
                   time_1968_major + time_1651_major)
    total_all = total_minor + total_major

    # Create data points for future state
    time_3077_major, time_3077_minor = prod_time(
        batch_df['3077'], '3077', find_lot=0, pull_mat=0.5, return_mat=0.5)
    time_3001_major, time_3001_minor = prod_time(
        batch_df['3001'], '3001', find_lot=0, pull_mat=0.5, return_mat=0.5)
    time_3004_major, time_3004_minor = prod_time(
        batch_df['3004'], '3004', find_lot=0, pull_mat=0.5, return_mat=0.5)
    time_1968_major, time_1968_minor = prod_time(
        batch_df['1968'], '1968', find_lot=0, pull_mat=0.5, return_mat=0.5)
    time_1651_major, time_1651_minor = prod_time(
        batch_df['1651'], '1651', find_lot=0, pull_mat=0.5, return_mat=0.5)

    future_total_minor = (time_3077_minor + time_3001_minor + time_3004_minor +
                          time_1968_minor + time_1651_minor)
    future_total_major = (time_3077_major + time_3001_major + time_3004_major +
                          time_1968_major + time_1651_major)
    future_total_all = future_total_minor + future_total_major

    ax1 = plt.subplot2grid((7, 1), (0, 0), rowspan=2, colspan=1)
    ax2 = plt.subplot2grid((7, 1), (2, 0), rowspan=3, colspan=1)
    ax3 = plt.subplot2grid((7, 1), (5, 0), rowspan=2, colspan=1)

    total_all.plot.line(ax=ax1, label='Total', ylim=(0, 12))
    total_major.plot.line(ax=ax1, label='Majors', ylim=(0, 12))
    total_minor.plot.line(ax=ax1, label='Minors', ylim=(0, 12))

    # First subplot build (current state time analysis)
    ax1.fill_between(total_all.index, 6, total_all, where=(total_all > 6),
                     facecolor='r', alpha=0.3)
    ax1.fill_between(total_all.index, 6, total_all, where=(total_all < 6),
                     facecolor='g', alpha=0.3)
    ax1.axhline(y=6, linewidth=2, color='k')
    ax1.legend()
    ax1.xaxis.set_visible(False)
    ax1.set_title('Hours to Complete - Current State')
    ax1.set_ylabel('Time (Hours)')

    # Second subplot build (Batches produced Column chart)
    batch_df.plot.bar(ax=ax2)
    ax2.get_yaxis().set_major_locator(pylab.MaxNLocator(integer=True))
    ax2.xaxis.set_visible(False)
    ax2.set_title('Batches Produced by Shift')
    ax2.set_ylabel('Batches')

    # Third subplot build (future state time analysis)
    future_total_all.plot.line(ax=ax3, label='Total', ylim=(0, 12))
    future_total_major.plot.line(ax=ax3, label='Majors', ylim=(0, 12))
    future_total_minor.plot.line(ax=ax3, label='Minors', ylim=(0, 12))
    ax3.fill_between(future_total_all.index, 6, future_total_all,
                     where=(future_total_all > 6), facecolor='r', alpha=0.3)
    ax3.fill_between(future_total_all.index, 6, future_total_all,
                     where=(future_total_all < 6), facecolor='g', alpha=0.3)
    ax3.axhline(y=6, linewidth=2, color='k')
    ax3.set_title('Hours to Complete - Future State')
    ax3.set_ylabel('Time (Hours)')
    ax3.legend()

    # Build main plot, and show.
    plt.legend()
    plt.subplots_adjust(left=0.08, bottom=0.07, right=0.92, top=0.95,
                        hspace=0.25)
    plt.show()


def week_batches_prod(week=2):
    """
    Builds a dataframe showing the number of batches produced by rolling weeks.
    By default, displays a rolling 2-week period, but can be changed to be any
    number of weeks (between 1 and 48).
    :param week: (int) number of rolling weeks used to build dataframe.
    :return: (dataframe) batches produced by x weeks.
    """
    def batches_used(composition, start, end):
        pickle_in = open('batch_prod_df.pickle', 'rb')
        batches_df = pickle.load(pickle_in)
        batch_per_day = batches_df[composition][start:end]
        total_batches = batch_per_day.sum()
        return total_batches

    week_use_df = pd.DataFrame()
    period_num = 48
    week_use_df['start_date'] = pd.date_range(
        '12/28/2014', periods=period_num, freq='W')
    week_use_df['end_date'] = week_use_df['start_date'].shift(-week)
    week_use_df.dropna(inplace=True)

    comp_list = ['3077', '3001', '3004', '1968', '1651', '2004', '6105',
                 '2073', '1661', '6101', '1651', '2290', '3036']

    for comp in comp_list:
        temp_list = []
        for idx in range(period_num - week):
            temp_list.append(batches_used(comp, week_use_df['start_date'][idx],
                                          week_use_df['end_date'][idx]))
        week_use_df[comp] = temp_list

    return week_use_df


def comp_df_defs(comp=None):
    """
    A place-keeper function for holding the dataframes of the comps batched in
    2015. Holds the DataFrame in a dict, then calls the dict[comp] to retrieve
    the dataframe.
    :param comp: (str) The selected composition.
    :return: (dataframe) (str) stockcodes, (str) material names, (float) pounds
    used in comp.
    """
    c_3077_stockcodes = ['000954', '00550225', '00360226', '03260291',
                         '05580496']
    c_3077_mat_names = ['Sil-Co-Sil 250', 'Mag Chem 10 -325',
                        'Russian Calcined Zirconia', '3077 Reclaim -14',
                        '3001 Reclaim -14']
    c_3077_lbs = [2.3, 9.9, 372.9, 54.0, 107.0]
    c_3077_df = pd.DataFrame(
        np.transpose([c_3077_stockcodes, c_3077_mat_names, c_3077_lbs]),
        columns=['StockCode', 'Material', 'lbs'])

    c_3001_stockcodes = ['00260225', '00370225', '00490225', '00550225',
                         '00910225', '00060225', '05580496', '06560225',
                         '04510220', '000792', '00360226']
    c_3001_mat_names = ['Clay TN #6', 'SFH Silica', 'Cereal Binder',
                        'Mag Chem 10 -325', 'A-152 SG Calcined Alumina',
                        'Zirconium Silicate Ultrox Spec', '3001 Reclaim -14',
                        'Natrosol - Screened', 'Blended Baddeleyite Milled',
                        'A-Grain for 3001', 'Russian Calcined Zirconia']
    c_3001_lbs = [6.0, 4.0, 6.0, 40.0, 10.0, 20.0, 300.0, 38.4, 192.0, 520.0,
                  908.0]
    c_3001_df = pd.DataFrame(
        np.transpose([c_3001_stockcodes, c_3001_mat_names, c_3001_lbs]),
        columns=['StockCode', 'Material', 'lbs'])

    c_3004_stockcodes = ['00260225', '00370225', '00490225', '00550225',
                         '00910225', '00060225', '05580496', '06560225',
                         '04510220', '02100207', '00360226']
    c_3004_mat_names = ['Clay TN #6', 'SFH Silica', 'Cereal Binder',
                        'Mag Chem 10 -325', 'A-152 SG Calcined Alumina',
                        'Zirconium Silicate Ultrox Spec', '3001 Reclaim -14',
                        'Natrosol - Screened', 'Blended Baddeleyite Milled',
                        'A-Grain for Coarse Grain',
                        'Russian Calcined Zirconia']
    c_3004_lbs = [6.0, 4.0, 6.0, 40.0, 10.0, 20.0, 325.0, 38.4, 138.0, 500.0,
                  957.0]
    c_3004_df = pd.DataFrame(
        np.transpose([c_3004_stockcodes, c_3004_mat_names, c_3004_lbs]),
        columns=['StockCode', 'Material', 'lbs'])

    c_1968_stockcodes = ['01330225', '01340225', '00929', '00370225',
                         '00200225', '0910225', '02100207', '05540265',
                         '02633029', '02630283', '02630304', '02630351',
                         '02630325']
    c_1968_mat_names = ['Zirconia Bubble -10+20', 'Zirconia Bubble -20+30',
                        'Calcium Carbonate Tech Grade S', 'SFH Silica',
                        'Norlig A', 'A-152 SG Calcined Alumina',
                        'A-Grain for Coarse Grain',
                        '1999 Reclaim -10 de-ironed', 'GNF -14 Reclaim',
                        'GNF -28+48', 'GNF -48+100', 'GNF -100', 'GNF -325']
    c_1968_lbs = [245.0, 245.0, 6.0, 3.5, 37.1, 3.5, 96.0, 225.0, 75.0, 262.0,
                  167.0, 48.0, 87.0]
    c_1968_df = pd.DataFrame(
        np.transpose([c_1968_stockcodes, c_1968_mat_names, c_1968_lbs]),
        columns=['StockCode', 'Material', 'lbs'])

    c_1651_stockcodes = ['02631651', '02630283', '02630304', '02630351',
                         '00150225', '02100207', '00550225', '06560225']
    c_1651_mat_names = ['1651 Preweigh Grog', 'GNF -28+48', 'GNF -48+100',
                        'GNF -100', 'Pulverized limestone R-2 Spec',
                        'A-Grain for Coarse Grain', 'Mag Chem 10 -325',
                        'Natrosol - Screened']
    c_1651_lbs = [255.2, 31.4, 94.2, 129.6, 4.5, 138.0, 0.8, 8.0]
    c_1651_df = pd.DataFrame(
        np.transpose([c_1651_stockcodes, c_1651_mat_names, c_1651_lbs]),
        columns=['StockCode', 'Material', 'lbs'])

    c_2004_stockcodes = ['00030225', '00050225', '05730275', '06560225',
                         '00370225', '00490225', '25003020']
    c_2004_mat_names = ['Zircon Flour -325', 'Zircon Sand Spec. # 20301',
                        'De-ironed Dense Zircon Grog -20',
                        'Natrosol - Screened', 'SFH Silica', 'Cereal Binder',
                        'Zircoa A -325 mesh']
    c_2004_lbs = [93.9, 59.8, 220.3, 6.2, 8.0, 1.2, 18.0]
    c_2004_df = pd.DataFrame(
        np.transpose([c_2004_stockcodes, c_2004_mat_names, c_2004_lbs]),
        columns=['StockCode', 'Material', 'lbs'])

    c_6105_stockcodes = ['01350225', '02560275', '02560283', '02560325',
                         '01440225', '06560225', '03136105']
    c_6105_mat_names = ['Calcined Alumina -325', 'Tabular Alumina -14+28',
                        'Tabular Alumina -28', 'Tabular Alumina -325',
                        'Fused Zirconia Mullite -35', 'Natrosol - Screened',
                        '6105 Preweigh Blend']
    c_6105_lbs = [62.25, 68.3, 152.7, 40.2, 20.1, 13.3, 58.25]
    c_6105_df = pd.DataFrame(
        np.transpose([c_6105_stockcodes, c_6105_mat_names, c_6105_lbs]),
        columns=['StockCode', 'Material', 'lbs'])

    c_2073_stockcodes = ['06560225', '00490225', '00550225', '02100207',
                         '03040274']
    c_2073_mat_names = ['Natrosol - Screened', 'Cereal Binder',
                        'Mag Chem 10 -325', 'A-Grain for Coarse Grain',
                        '2074 -14']
    c_2073_lbs = [8.0, 1.2, 4.0, 116.2, 280.5]
    c_2073_df = pd.DataFrame(
        np.transpose([c_2073_stockcodes, c_2073_mat_names, c_2073_lbs]),
        columns=['StockCode', 'Material', 'lbs'])

    c_1661_stockcodes = ['02631661', '000564', '06560225', '00150225',
                         '00490225', '00550225', '02100207']
    c_1661_mat_names = ['1661 Preweigh Grog', 'Alcan C-71 Unground Alumina',
                        'Natrosol - Screened', 'Pulverized limestone R-2 Spec',
                        'Cereal Binder', 'Mag Chem 10 -325',
                        'A-Grain for Coarse Grain']
    c_1661_lbs = [280.0, 6.0, 6.0, 6.8, 1.2, 0.8, 109.6]
    c_1661_df = pd.DataFrame(
        np.transpose([c_1661_stockcodes, c_1661_mat_names, c_1661_lbs]),
        columns=['StockCode', 'Material', 'lbs'])

    c_6101_stockcodes = ['00200225', '02560266', '02560275', '02560283',
                         '02560325', '01350225', '00260225']
    c_6101_mat_names = ['Norlig A', 'Tabular Alumina -8+14',
                        'Tabular Alumina -14+28', 'Tabular Alumina -28',
                        'Tabular Alumina -325', 'Calcined Alumina -325',
                        'Clay TN #6']
    c_6101_lbs = [10.0, 144.9, 74.5, 78.6, 58.0, 49.7, 7.0]
    c_6101_df = pd.DataFrame(
        np.transpose([c_6101_stockcodes, c_6101_mat_names, c_6101_lbs]),
        columns=['StockCode', 'Material', 'lbs'])

    c_2290_stockcodes = ['02100207', '01210225', '06560225',
                         '01280225', '01290225', '01300225']
    c_2290_mat_names = ['A-Grain for Coarse Grain', 'Duramax D-3005',
                        'Natrosol - Screened', 'ZY-8 -28+48', 'ZY-8 -48+100',
                        'ZY-8 -100']
    c_2290_lbs = [523.2, 0.8, 19.0, 117.5, 356.4, 489.1]
    c_2290_df = pd.DataFrame(
        np.transpose([c_2290_stockcodes, c_2290_mat_names, c_2290_lbs]),
        columns=['StockCode', 'Material', 'lbs'])

    c_3036_stockcodes = ['00370225', '00550225', '06560225', '00490225',
                         '000792', '05580811']
    c_3036_mat_names = ['SFH Silica', 'Mag Chem 10 -325',
                        'Natrosol - Screened', 'Cereal Binder',
                        'A-Grain for 3001', '3001 Reclaim -100']
    c_3036_lbs = [2.4, 4.1, 7.0, 1.2, 134.8, 260.0]
    c_3036_df = pd.DataFrame(
        np.transpose([c_3036_stockcodes, c_3036_mat_names, c_3036_lbs]),
        columns=['StockCode', 'Material', 'lbs'])

    df_dict = {'3077': c_3077_df, '3001': c_3001_df, '3004': c_3004_df,
               '1968': c_1968_df, '1651': c_1651_df, '2004': c_2004_df,
               '6105': c_6105_df, '2073': c_2073_df, '1661': c_1661_df,
               '6101': c_6101_df, '2290': c_2290_df, '3036': c_3036_df}
    if comp == None:
        return df_dict
    else:
        return df_dict[comp]


def mat_use_by_x_week(stockcode, num_weeks=2):
    """
    Evaluates material usage based on a CGB-Pull system. Multiplies the amount
    of material of (stockcode) needed for a comp (0 if not requiered) and
    multiplies the material required by the number of batches produced.
    :param stockcode: (str) Material to evaluate (multiply through) in batch
    matrix.
    :param num_weeks: (int) the rolling-week structure, as used above to build
    the batch produced dataframe.
    :return: (dataframe) amount of the given material used per comp per
    rolling-weeks.
    """
    batches_x_week = week_batches_prod(num_weeks)
    comp_list = list(batches_x_week.columns.values)[2:]
    mat_use_df = pd.DataFrame()
    mat_use_df['start_date'] = batches_x_week['start_date']
    mat_use_df['end_date'] = batches_x_week['end_date']

    for comp in comp_list:
        try:
            comp_df = comp_df_defs(comp)
            comp_weight = float(comp_df[
                comp_df['StockCode'] == stockcode]['lbs'].iloc[0])
        except IndexError:
            comp_weight = 0
        mat_use_df[comp] = batches_x_week[comp] * comp_weight
    mat_use_df['sum'] = mat_use_df.sum(axis=1)
    return mat_use_df


def material_usage_statistics(weeks):
    """
    Compiles all unique stockcodes in the comps evaluated, then applies the
    stockcodes to the mat_use_by_x_week function. A dataframe is then built,
    which maps each stock code with it's median, mean and max usage applied to
    a dataframe.
    :param weeks: (int) # of rolling weeks used to evaluate.
    :return: (dataframe):(str)(pk) all unique stock codes, (str) material name,
    (float) median usage in year, (float) mean usage in year, (max) max usage
    in year.
    """
    merged_comps = pd.concat([comp_df_defs('3077'), comp_df_defs('3001'),
                              comp_df_defs('3004'), comp_df_defs('1968'),
                              comp_df_defs('1651'), comp_df_defs('2004'),
                              comp_df_defs('6105'), comp_df_defs('2073'),
                              comp_df_defs('1661'), comp_df_defs('6101'),
                              comp_df_defs('2290'), comp_df_defs('3036')])
    unique_sc = merged_comps['StockCode'].unique()
    stockcode_list = []
    mat_name_list = []
    median_list = []
    mean_list = []
    max_list = []
    for stockcode in unique_sc:
        stockcode_list.append(stockcode)
        mat_name = (merged_comps[merged_comps['StockCode'] == stockcode]
                    ['Material'].iloc[0])
        mat_name_list.append(mat_name)

        mat_usage = mat_use_by_x_week(stockcode, num_weeks=weeks)['sum']
        median_usage = round(mat_usage.median(), 1)
        median_list.append(median_usage)

        mean_usage = round(mat_usage.mean(), 1)
        mean_list.append(mean_usage)

        max_usage = round(mat_usage.max(), 1)
        max_list.append(max_usage)

    mat_stats_df = pd.DataFrame(index=stockcode_list)
    mat_stats_df['Material'] = mat_name_list
    mat_stats_df['Median_Usage'] = median_list
    mat_stats_df['Mean_Usage'] = mean_list
    mat_stats_df['Max_Usage'] = max_list
    return mat_stats_df

# wk_1_stats = pickle.load(open('1_wk_rm_stats_rev_1.pickle', 'rb'))
# wk_2_stats = pickle.load(open('2_wk_rm_stats_rev_1.pickle', 'rb'))
#
# print(wk_1_stats)
# print(wk_2_stats)
#
# writer = pd.ExcelWriter(r'C:/rm_stats_rev_1.xlsx')
# wk_1_stats.to_excel(writer, '1Week')
# wk_2_stats.to_excel(writer, '2week')
# writer.save()

print(material_usage_statistics(1))