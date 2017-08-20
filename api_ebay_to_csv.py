"""                    ~~~Get data from eBay API~~~

This script pulls data from the eBay API. It accepts calls to the
findCompletedItems and findItemsAdvanced endpoints with a search term. It gets
the data, performs some basic transformations before exporting a csv file.

Dependencies:

- Pandas
- ebaysdk
"""
from __future__ import print_function, division
from ebaysdk.finding import Connection as Finding
from ebaysdk.exception import ConnectionError
from sys import argv
import pandas as pd
import time

# =============================================================================
#    Config
# =============================================================================

script, target_dir, filename, call_type, search_term = argv

app_id = '<YOUR APP ID>'
input_pages = 100

# The details of the API request
request = {'keywords': search_term, 'paginationInput': {'pageNumber': 1}}

# These columns have their nested key-value pairs expanded
dict_cols = ['condition', 'listingInfo', 'primaryCategory',
             'sellingStatus', 'sellingStatus_convertedCurrentPrice',
             'sellingStatus_currentPrice', 'shippingInfo',
             'shippingInfo_shippingServiceCost']


# Columns to drop, as they don't appear often enough to be useful
drop_cols = ['secondaryCategory']


# =============================================================================
#    Functions
# =============================================================================


def get_response(app_id, call_type, request):
    """Gets the response from the Finding API.

    Args:
        app_id (str): The App ID from eBay
        request (dict): The request structure

    Returns:
        response (ebaysdk response object)
        pages (str): The number of pages in the response
    """
    try:
        api = Finding(appid=app_id, config_file=None)
        response = api.execute(call_type, request)
        pages = response.dict()['paginationOutput']['totalPages']
        return api, response, pages

    except ConnectionError as e:
        print(e)
        # print(e.response.dict())
        return None, None, None


def cols_from_dict(df, col):
    """Expands the dict column in the DataFrame and merges it into the df.

    Args:
    df (DataFrame): The full dataset
    col (str): The column name

    Return:
    df
    """
    expanded = df[col].apply(pd.Series)
    expanded.columns = [col + '_' + str(c) for c in expanded.columns]
    expanded.columns = [cc.replace('__', '_') for cc in expanded.columns]
    return pd.concat([df.drop([col], axis=1), expanded], axis=1)


def get_data(response, drop_cols, dict_cols):
    """Collects the data from the response.

    Args:
        response (ebaysdk response object)
        drop_cols (list): Columns that we drop as they don't appear often
        dict_cols (list): Cols that have dict objects as values

    Returns:
        df (DataFrame): DataFrame of appended
    """
    data = response.dict()['searchResult']['item']
    df = pd.DataFrame(data)

    # Drop unnecessary cols
    for k in range(len(drop_cols)):
        if drop_cols[k] in df.columns:
            df.drop([drop_cols[k]], axis=1, inplace=True)

    # Expand dict cols
    for l in range(len(dict_cols)):
        df = cols_from_dict(df, dict_cols[l])

    return df


def main():

    # Collect the API response
    api, response, pages = get_response(app_id, call_type, request)

    # Loop through 100 pages or the total number of pages and collect data
    print(pages)
    for j in range(min(int(pages), int(input_pages))):
        if j == 0:
            print(j)
            df = get_data(response, drop_cols, dict_cols)
        else:
            # Next page of API response
            print(j, "*")
            request['paginationInput']['pageNumber'] += 1
            new_response = api.execute(call_type, request)
            df_new = get_data(new_response, drop_cols, dict_cols)
            df = df.append(df_new, ignore_index=True)

    # Write the data to csv
    f = target_dir + '/' + filename + '_' + call_type + '_' + search_term + \
        '_' + time.strftime('%Y%m%d%H%M%S') + '.csv'
    df.to_csv(f, index=False, encoding='utf-8')


if __name__ == '__main__':
    main()
