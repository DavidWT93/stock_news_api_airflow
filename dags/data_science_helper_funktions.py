import pandas as pd
import numpy as np

def convert_col_to_time_index(df, col_name, time_format):
    # Convert the column to a datetime object with the specified format
    df[col_name] = pd.to_datetime(df[col_name], format=time_format,utc=True)

    # Set the column as the index
    df.set_index(col_name, inplace=True)

    return df


def get_delta_of_timeinterval(timeInterval, deltaTime):
    """

    Parameters
    ----------
    timeInterval : (string) kategorie -> hours,days,minutes,seconds
    deltaTime :(float) -> time you want to go forward

    Returns
    -------

    """
    if timeInterval == "days":
        deltaTime = pd.Timedelta(days=deltaTime)
    elif timeInterval == "hours":
        deltaTime = pd.Timedelta(hours=deltaTime)
    elif timeInterval == "minutes":
        deltaTime = pd.Timedelta(minutes=deltaTime)
    elif timeInterval == "seconds":
        deltaTime = pd.Timedelta(seconds=deltaTime)
    else:
        print("chose wrong time interval!")
        deltaTime = 0
    return deltaTime


def get_time_interval_from_df(df, startTime, deltaTime, timeInterval):
    """

    Parameters
    ----------
    df : (dataframe)
    timeInterval : (string) kategorie -> hours,days,minutes,seconds
    startTime : (timestamp) ex.: '2022-04-05 16:30:00'
    deltaTime : (int)

    Returns
    -------

    """
    start = pd.Timestamp(startTime)
    deltaTime = get_delta_of_timeinterval(timeInterval, deltaTime)
    end = start + deltaTime

    resultDf = df[(df.index >= start) & (df.index <= end)]
    return resultDf


def correlation_of_column_in_df(df, column):
    # correlation on a random day
    corrDf = df.corr()
    correlatedColumnOfInterest = pd.DataFrame(corrDf[column]).T
    return correlatedColumnOfInterest


def loop_thru_time_and_use_funktion_on_df(df, startTime, endTime, deltaTime, timeInterval,funktion,funktionParameters):
    """
    The function can be utilized to apply a function to a specific interval within a DataFrame.
    This feature enables users to easily analyze data at a higher frequency or a different time scale.
    For example, if a DataFrame contains entries for every minute of the day,
    the function can be applied to aggregate the data into hourly intervals.
    This allows users to gain insights into hourly trends and patterns in the data,
     rather than being limited to minute-by-minute analysis. The funktion reduces dimensionality of the dataframe to
     another time interval or toi the same, it depends on the funktion you apply to the df.

    Parameters
    ----------
    df : (dataframe)
    timeInterval : (string) kategorie -> hours,days,minutes,seconds
    startTime : (timestamp) ex.: '2022-04-05 16:30:00'
    deltaTime : (int)
    endTime : time until the loop should go
    deltaTime
    timeInterval
    funktion : funktion applied to th dataframe
    funktionParameters : parameters, besides the df which should get applied to the df

    Returns
    -------

    """
    currentTime = pd.Timestamp(startTime)
    endTime = pd.Timestamp(endTime)
    i = 0
    while currentTime <= endTime:
        currentTime += get_delta_of_timeinterval(timeInterval,deltaTime)
        resDf = get_time_interval_from_df(df, currentTime, deltaTime, timeInterval)
        try:
            funktionAppliedToDF = funktion(resDf,**funktionParameters)
            funktionAppliedToDF["time_stamp"] = currentTime
            if i == 0:
                Df = funktionAppliedToDF
                i = 1
            else:
                Df = pd.concat([Df,funktionAppliedToDF])
        except:
            pass
    Df.set_index("time_stamp",inplace=True)
    return Df


def approximated_polynomial_and_derivative_of_df_column(df,toAppoxColumn,polynomDegree=70):
    y = df[toAppoxColumn]
    x = range(len(y))

    # Fit a polynomial of degree 3 to the data
    p = np.polyfit(x, y, polynomDegree)

    # Evaluate the polynomial and its derivative at the same x values
    polynomialValues = np.polyval(p, x)
    derivativeValues = np.polyval(np.polyder(p), x)
    return polynomialValues,derivativeValues


def add_data_to_df(df,dataToAdd, newColumnName):
    df[newColumnName] = dataToAdd
    return df


def ad_approximated_polynom_derivation_to_df(df,columnToApproximate,newPolynomialColumnName="polynomial_approximations",newDerivationColumnName="derivative_approximations"):
    polynomialValues,derivativeValues = approximated_polynomial_and_derivative_of_df_column(df,columnToApproximate)
    df = add_data_to_df(df,polynomialValues,newPolynomialColumnName)
    df = add_data_to_df(df,derivativeValues,newDerivationColumnName)
    return df


def inner_merge_2_dfs_on_index(DF1, Df2):
    """
    funktion that merges the news with the bars on the index
    """
    mergedDf = pd.merge(DF1, Df2, left_index=True, right_index=True, how='outer')
    return mergedDf





