import pandas as pd
import numpy as np
import pyodbc
import math


class MaterialAnalyzer:
    """
    This class takes a material stockcode as an input, and then can analyze 
    the stockcode in a number of ways.
    """

    def __init__(self, stockcode: str, server: str='ZIRSYSPRO',
                 db: str ='ZIRPROD') -> object:
        """

         :rtype : object
        """
        self._stockcode = stockcode
        self._server = server
        self._db = db
        self._conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' +
                                    self._server + ';DATABASE=' + self._db +
                                    ';Trusted_Connection=yes')

    def lots_list(self, min_usage_year = 2006):
        """
        Returns a list of unique lots for a given stock code.
        """
        sql = """
            SELECT LotJob, TrnType, TrnDate, TrnQuantity
              FROM [ZIRPROD].[dbo].[LotTransactions]
              where StockCode = '{0}' and
              YEAR(TrnDate) >= {1}
              order by TrnDate
            """.format(self._stockcode, min_usage_year)
        unique_df = pd.read_sql(sql, self._conn)
        unique_lots = unique_df['LotJob'].unique().tolist()
        return unique_lots

    def lot_transactions(self, lot):
        """
        Takes in a lot as an argument, and returns the receipt and issuances of
        that lot
        """
        sql = """ 
              SELECT LotJob, TrnType, TrnDate, TrnQuantity, 
              convert(float, TrnDate) as FloatTrnDate,
              convert(int, GETDATE()) as today
              FROM [ZIRPROD].[dbo].[LotTransactions]
              where StockCode = '{0}' 
              and LotJob = '{1}' and (TrnType = 'R' or TrnType = 'I' or 
              TrnType = 'A')
              order by TrnDate
              """.format(self._stockcode, lot)
        usage_df = pd.read_sql(sql, self._conn)
        return usage_df

    def lot_usage(self, lot, series=True, column_name='ProductUsage'):
        """
        Takes a lot of the object stock code as an input, and returns a series
        listing the sequential usage events of the object. Optionally, can 
        return values as a list instead.
        """
        usage_df = self.lot_transactions(lot)
        usage_list = []
        i = 0
        first_issuance = True
        issuance_style = 1
        for value in usage_df['TrnQuantity'].tolist():
            value = int(value)
            if i == 0:
                usage_list.append(value)
                i += 1
            # Issuances used to be negative values, and then procedure changed
            # to have the issuance be positive. The following elif accounts for
            # this by looking at the first issuance and assigning a 1 or -1
            # multiplier if the issuance is negative or positive, respectively.
            elif first_issuance and usage_df['TrnType'].iget_value(i) == 'I':
                if value > 0:
                    issuance_style = -1
                else:
                    issuance_style = 1
                usage_value = usage_list[-1] + issuance_style * value
                usage_list.append(usage_value)
                first_issuance = False
                i += 1
            # Adjustments are applied as-is                
            elif (usage_df['TrnType'].iget_value(i) == 'A' or
                  usage_df['TrnType'].iget_value(i) == 'R'):
                usage_value = usage_list[-1] + value
                usage_list.append(usage_value)
                i += 1
            # all other issuances will be accrued here
            else:
                usage_value = usage_list[-1] + issuance_style * value
                usage_list.append(usage_value)
                i += 1
        # If series is false, will return the list. Otherwise, will convert the
        # list to a series and return the series.
        if not series:
            return usage_list
        else:
            usage_series = pd.Series(usage_list, name=column_name)
            return usage_series

    def trns_usage_df(self, lot):
        """
        Returns a dataframe containing transaction data combined with usage.
        """
        group_df = pd.concat([self.lot_transactions(lot), self.lot_usage(lot)],
                             axis=1)
        return group_df

    def days_receipt_to_use(self, lot):
        """
        Given a lot, calculates the number of days from receipt to first issue,
        and returns the list: [date_receipt, date_first_issue, days]. If
        material has not been issued, returns 'nan' for first_issue_date and
        days.
        """
        lot_trns = self.lot_transactions(lot)
        date_receipt = lot_trns['TrnDate'].iget_value(0)
        lot_usage_days = self.trns_usage_df(lot)['FloatTrnDate']

        i = 0
        for trntype in lot_trns['TrnType'].tolist():
            if trntype == 'I':
                days = (lot_usage_days.iget_value(i) -
                        lot_usage_days.iget_value(0))
                date_first_issue = lot_trns['TrnDate'].iget_value(i)
                i += 1
                return [date_receipt, date_first_issue, int(days)]
            elif trntype != 'I':
                if i < len(lot_trns['TrnType'].tolist()) - 1:
                    i += 1
                else:
                    return [date_receipt, np.nan, np.nan]

    def days_x_percent_issued(self, lot, percent):
        """
        Given a lot, finds the date of issue at which x% of the total lot was 
        issued or adjusted. Returns date of the first issue, date of the x% 
        issue, and number of days between in the form of a list.        
        """
        lot_usage = self.trns_usage_df(lot)['ProductUsage']
        lot_usage_dates = self.trns_usage_df(lot)['TrnDate']
        lot_usage_days = self.trns_usage_df(lot)['FloatTrnDate']
        date_first_issue = self.days_receipt_to_use(lot)[1]
        days_first_issue = self.days_receipt_to_use(lot)[2]
        if date_first_issue is np.nan:
            return [np.nan, np.nan, np.nan]
        else:
            starting_quantity = lot_usage.iget_value(0)
            i = 0
            for value in lot_usage.tolist():
                if value > starting_quantity * (1 - percent / 100):
                    i += 1
                else:
                    date_x_issue = lot_usage_dates.iget_value(i)
                    days = (lot_usage_days.iget_value(i) -
                            lot_usage_days.iget_value(0) - days_first_issue)
                    return [date_first_issue, date_x_issue, int(days)]
            return [date_first_issue, np.nan, np.nan]

    def days_total(self, lot, tolerance=100):
        """
        Given a lot, finds the date of receipt, and the date of last use, and 
        returns those values, along with the number of days between them, and
        the number of days from receipt to today. Optional: include a lower
        tolerance to reduce requirement for being used completely. 
        Returned as list: [date_receipt, date_last_use, days, 
        quantity_remaining]
        """
        # date variables
        lot_usage_dates = self.trns_usage_df(lot)['TrnDate']
        # variable to go to the end of the dataframe
        table_len = len(lot_usage_dates.tolist()) - 1
        date_receipt = lot_usage_dates.iget_value(0)
        date_last_use = lot_usage_dates.iget_value(table_len)

        # days variables and calculation
        lot_usage_days = self.trns_usage_df(lot)['FloatTrnDate']
        today = self.lot_transactions(lot)['today'].iget_value(0)

        days_use = (lot_usage_days.iget_value(table_len) -
                    lot_usage_days.iget_value(0))
        # returns total days as days use if the lot is completely consumed
        # (within the set tolerance, default: 100%)
        if self.material_total_remain_percent(lot)[2] >= tolerance:
            days_total = days_use
        else:
            days_total = today - lot_usage_days.iget_value(0)

        return [date_receipt, date_last_use, int(days_use), int(days_total)]

    def material_total_remain_percent(self, lot):
        """
        Given a lot, calculates the percent of material currently issued for
        the lot, and also gives the original quantity and current quantity.
        Returned as list: [quantity_original, quantity_remaining, percent_used]
        """

        # material quantity variables and calculation
        lot_usage = self.trns_usage_df(lot)['ProductUsage']
        # variable to go to the end of the dataframe
        table_len = len(lot_usage.tolist()) - 1
        quantity_remaining = lot_usage.iget_value(table_len)
        quantity_original = lot_usage.iget_value(0)
        # In case 0 material received, return nan for all values
        if quantity_original == 0:
            return [np.nan, np.nan, np.nan]
        else:
            percent_used = math.ceil(
                ((quantity_original - quantity_remaining) /
                 quantity_original) * 100)
            return [quantity_original, quantity_remaining, percent_used]


stockcode_list = ['00060225',
                  '000656',
                  '000954',
                  '000981',
                  '00150225',
                  '00200225',
                  '00260225',
                  '00330226',
                  '00370225',
                  '00490225',
                  '00550225',
                  '00630225',
                  '00910225',
                  '01260225',
                  '01270225',
                  '01280225',
                  '01290225',
                  '01300225',
                  '01330225',
                  '01340225',
                  '02400225',
                  '02630266',
                  '02630275',
                  '02630283',
                  '02630304',
                  '02630325',
                  '02630351',
                  '02750100',
                  '02750101',
                  '02780225',
                  '0370225',
                  '0910225',
                  '00360226',
                  '00030225',
                  '00050225',
                  ]