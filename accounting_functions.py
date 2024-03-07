import payout_functions_utilits as pfu

def read_payout_data():
    """main function to apply everything

    Returns:
        pd.DataFrame: _description_
    """
    df = pfu.read_data()
    df = pfu.clean_data(df)
    dfs = pfu.group_data(df)
    return dfs
