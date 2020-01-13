from pyspark.sql.functions import (month, mean, max, min,
                                   count, dayofmonth, desc, col)
import os
from config import sqlContext, data_file

df = sqlContext.read.csv(os.path.abspath(data_file), header=True)


def first_task():
    """
    Logic:
        - Make grouping by city and month
        - Apply aggregation(max, min, mean)
        - Sort for better visualisation
        - Convert to Pandas df and after convert to csv save.
        (For some reason on my Windows10 regular .to_csv() doesn't work)
    """
    df_by_month = (df.groupby('City', month('date'))
                   .agg(mean('temperature').alias('mean'),
                        max('temperature').alias('max'),
                        min('temperature').alias('min'))
                   .sort(desc('City')))
    pandas_df = df_by_month.toPandas()
    pandas_df.to_csv('1.csv')


def second_task(number_of_month: int, amount_of_rows: int):
    """
    Logic: If I'm not mistaken, I have to check if here enough observations for specific month.
        So:
            -  Make grouping by month and calculate amount of observations.
            - Check if enough.
            - If enough make grouping from previous task.
            - If not enough exit.
    :param number_of_month: number from 1 up to 12.
    :param amount_of_rows: number of observations for chosen month.
    """
    to_check = df.groupby(month('date').alias('month')).agg(count('_c0').alias('amount'))
    if not to_check.filter((to_check.month == number_of_month) & (to_check.amount > amount_of_rows)).head(1):
        print("Nothing to show!")
    else:
        answer = (df
                  .groupby('City', month('date').alias('month'))
                  .agg(mean('temperature').alias('mean'),
                       max('temperature').alias('max'),
                       min('temperature').alias('min')))
        pandas_df = answer.toPandas()
        pandas_df.to_csv('2.csv')


def third_task():
    """
    Goal: Get the difference between mean value for day per month per City and mean value for month and City.
    Logic:
        - Make grouping by city, month and day for getting average value for each day per month per City.
        - Get df from first task.
        - Join them by month and city.
        - Calculate difference between mean values and save as new column 'diff'
        - Drop useless columns.
        - Save to csv.
    """
    city_month_day_df = (df
                         .groupby(df.City.alias('cmd_city'),
                                  month('date').alias('cmd_month'),
                                  dayofmonth('date').alias('cmd_day'))
                         .agg(mean('temperature').alias('cmd_mean'),
                              max('temperature').alias('cmd_max'),
                              min('temperature').alias('cmd_min')))

    first_task_answer_df = (df
                            .groupby('City', month('date').alias('month'))
                            .agg(mean('temperature').alias('mean'),
                                 max('temperature').alias('max'),
                                 min('temperature').alias('min')))

    answer = city_month_day_df.join(first_task_answer_df,
                                    (city_month_day_df.cmd_month == first_task_answer_df.month)
                                    & (city_month_day_df.cmd_city == first_task_answer_df.City))
    answer = answer.withColumn('diff_mean', answer.cmd_mean - answer.mean)
    answer = answer.withColumn('diff_max', answer.cmd_max - answer.max)
    answer = answer.withColumn('diff_min', answer.cmd_min - answer.min)

    columns_to_delete = ['City', 'month', 'cmd']
    answer = answer.drop(*columns_to_delete)

    pandas_df = answer.toPandas()
    pandas_df.to_csv('3.csv')


def forth_task(cities_to_show: list):
    """
    Goal: get data only for given cities.
    Logic:
        - Make grouping by City and month.
        - Filter out rows with given city.
        - save to csv.
    :param cities_to_show: list with city names, ex: ['Kiev', 'Paris']
    """
    answer = (df
              .groupby('City', month('date').alias('month'))
              .agg(mean('temperature').alias('mean'),
                   max('temperature').alias('max'),
                   min('temperature').alias('min'))
              .filter(col('City').isin(cities_to_show)))

    pandas_df = answer.toPandas()
    pandas_df.to_csv('4.csv')


if __name__ == "__main__":
    first_task()
    second_task(number_of_month=5, amount_of_rows=1000)
    third_task()
    forth_task(cities_to_show=['Kiev', 'Lvov', 'Kherson', 'Paris'])
