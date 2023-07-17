# -*- coding: utf-8 -*-
"""
Created on Sat Feb 12 11:35:28 2022

@author: david
"""

from sql_master import SQLHandling
import re


def log_to_txt(file_name, words):
    """
    You can use the Funktion to write text into a new text file or append text
    to an already existing text file.

    Parameters
    ----------
    file_name : string
        name of the file you want to create 
        or the name of an existing file you want to append text to 

    words : string
        Text you want to write in to file named file_name

    Returns
    -------
    None.

    """
    f = open(file_name, "a")
    f.write(words)
    f.close()


def save_DF_to_CSV(DF, save_name):
    """
    Saves DF to CSV

    Parameters
    ----------
    DF : DF
        Dataframe witch should get saved
    save_name : string
        path to witch the DF should get saved to 

    Returns
    -------
    None.

    """
    DF.to_csv(save_name)


def turn_column_with_lists_in_df_to_column_with_strings(DF, column):
    def list_to_string(lst):
        if isinstance(lst, list):
            return ",".join(lst)
        else:
            return lst

    DF[column] = DF[column].apply(list_to_string)
    return DF


def insert_df_to_sql_db(df, database, password, host, user, port, tableName):
    sh = SQLHandling(database, password, host, user, port)
    tableName = tableName.lower()
    sh.create_table_of_df(df, tableName)


def print_log(message, saveAs, printToConsole=True):
    if printToConsole:
        print(message)
    log_to_txt(saveAs, message)


def find_biggest_number_in_list_of_strings(listOfStrings):
    biggestNumber = 0
    for string in listOfStrings:
        if bool(re.search(r'\d', string)):
            number = int(re.findall(r'\d+', string)[0])
            if number > biggestNumber:
                biggestNumber = number
    return biggestNumber
