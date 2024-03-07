# %%
import pandas as pd
import os


# %%
def read_data(directory_path='inputs/'):
    """This reads the data from the excel sheet if uploaded form the gocardless, payouts transactions option. It will accept a single payout or combination of mutiple in one file or several

    Args:
        directory_path (str, optional): teh path to teh input folder. all excels in teh file will be read. Defaults to 'inputs/'.

    Returns:
       pd.Dataframe: a dataframe with all the required data
    """

    # Get a list of all files in the directory
    all_files = os.listdir(directory_path)

    # Filter only the .csv files
    csv_files = [file for file in all_files if file.endswith('.csv')]

    dfs = []

    # Iterate through each CSV file and read it into a DataFrame
    for csv_file in csv_files:
        file_path = os.path.join(directory_path, csv_file)
        df = pd.read_csv(file_path)
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)
    
    columns_to_keep = ['resources.description', 'gross_amount', 'gocardless_fees', 'app_fees', 'net_amount', 'payouts.arrival_date', 'payments.metadata.Member', 'payments.metadata.References']
    df = df[columns_to_keep]
    return df


# %%
def split_years(df):
    """this extracts any year data from the payment schedule or payment name 

    Args:
        df (pd.DataFrame):

    Returns:
        pd.Dataframe: 
    """
    df['year'] = df['resources.description'].str.extract(r'(\d{4})')
    df['resources.description'] = df['resources.description'].str.replace(r'\d{4}', '', regex=True)
    df['year'] = df['year'].fillna(0).astype(int)

    df['year_2'] = df['resources.description'].str.extract(r'(\d{2})')
    df['resources.description'] = df['resources.description'].str.replace(r'\d{2}', '', regex=True)
    df['year_2'] = pd.to_datetime(df['year_2'], format='%y').dt.strftime('%Y')
    df['year_2'] = df['year_2'].fillna(0).astype(int)

    df['year'] = df['year'] + df['year_2']

    df = df.drop(['year_2'], axis=1)

    return df


# %%
def clean_subscriptions_data(subset, schedule):
    """thsi cleans specificly data on subscriptions ensuring they payment month and year are extracted

    Args:
        subset (pd.Dataframe)): a subset of teh main df with just the subscription rows
        schedule (_type_): the name of the schedule which should be 'subscriptions'

    Returns:
         pd.DataFrame:
    """
    subset['schedule'] = schedule
    subset['resources.description'] = subset['resources.description'].replace({'\\(|\\)': '', '  ': ' '}, regex=True)
    subset['resources.description'] = subset['resources.description'].str.split(':', expand=False)
    subset['section_month'] = subset['resources.description'].str[1]
    subset['section_month'] = subset['section_month'].str.split(' ', expand=False)
    subset['section_month'] = subset['section_month'].apply(lambda x: [item for item in x if item])
    subset['section'] = subset['section_month'].str[0]
    subset['payment_name'] = subset['section_month'].str[1]
    subset = subset.drop(['section_month'], axis=1)

    return subset


# %%
def clean_activities_data(subset, schedule):
    """ this cleans specificly data on activities schedule ensuring they payment name is extracted

    Args:
        subset (pd.Dataframe)): a subset of the main df with just the activities rows
        schedule (_type_): the name of the schedule which should be 'subscriptions'

    Returns:
        pd.DataFrame:
    """
    subset['schedule'] = schedule
    subset['resources.description'] = subset['resources.description'].replace({'\\(|\\)': '', '  ': ' '}, regex=True)
    subset['resources.description'] = subset['resources.description'].str.split(':', expand=False)
    subset['payment_name'] = subset['resources.description'].str[1]
    subset['section'] = subset['payment_name'].str.split().str[0]
    subset['payment_name'] = subset['payment_name'].str.strip()

    return subset

# %%
def strip_metadata(df):
    """This extracts teh metadata into seprate columns

    Args:
        df (pd.DataFrame): 

    Returns: 
        pd.DataFrame:
    """
    df[['payment_code', 'schedule_code', 'section_code']] = df['payments.metadata.References'].str.split('-', expand=True)
    df[['payment_code', 'schedule_code', 'section_code']] = df[['payment_code', 'schedule_code', 'section_code']].apply(lambda x: x.str.strip())
    return df
    # split out names

# %%
def clean_member_names(df):
    """Removed teh member number from the names

    Args:
        df (pd.DataFrame): 

    Returns: 
        pd.DataFrame:
    """
    df['payments.metadata.Member'] = df['payments.metadata.Member'].str.split('(', expand=False)
    df['member'] = df['payments.metadata.Member'].str[0]
    return df

# %%
def clean_data(df):
    """main function to apply all cleaning functions to the datafram to convert it into what is required

    Args:
        df (pd.DataFrame): 

    Returns: 
        pd.DataFrame:
    """

    df = split_years(df)
   
    payment_schedules = ['Subscriptions', 'Activities']

    dfs = []

    # for loops through teh schedules and treats each differently
    for schedule in payment_schedules:
        subset = df[df['resources.description'].str.contains(schedule)].copy()
        if schedule == 'Subscriptions':
            subset = clean_subscriptions_data(subset=subset, schedule=schedule)
        elif schedule == 'Activities':
            subset = clean_activities_data(subset=subset, schedule=schedule)
        dfs.append(subset)

    df = pd.concat(dfs, ignore_index=True)
    
    df = strip_metadata(df)
    df = clean_member_names(df)
    df['total_fees'] = df['gocardless_fees'] + df['app_fees']
    df['payment_name'] = df['payment_name'].str.split(n=1).str[1]
        
    # General tidy up
    df = df.drop(['resources.description','payments.metadata.References', 'gocardless_fees', 'app_fees', 'payments.metadata.Member'], axis=1)
    column_order = ['section', 'schedule', 'year', 'payment_name', 'gross_amount', 'total_fees', 'net_amount', 'member',
       'payouts.arrival_date', 'section_code', 'schedule_code', 'payment_code']
    
    df = df[column_order]

    return df

# %%
def create_metadata(index, subset, date):
    """create a metadata string to accompany each dataframe breackdown

    Args:
        df (pd.DataFrame): 

    Returns: 
        pd.DataFrame:
    """
        
    id = index+1
    payments_num = subset.shape[0]+1
    date = pd.to_datetime(date).strftime('%d-%b-%Y')
    gross_amount = round(subset['gross_amount'].sum(),2)
    fee_amount = round(subset['total_fees'].sum(),2)
    net_amount = round(gross_amount - fee_amount,2)

    output_str = f"""
    ID: {id}
    Number of Payments: {payments_num}
    Date of Payout: {date}
    Payout Amount: £{gross_amount}
    Fees Paid: £{fee_amount}
    Net Amount: £{net_amount}
    """
    return output_str

# %%
def group_data(df):
    """Group all teh data by date and schedule with each secion its own row and each activity a row

    Args:
        df (pd.DataFrame): 

    Returns: 
        pd.DataFrame:
    """

    dfs = []

    for idx, date in enumerate(df['payouts.arrival_date'].unique()):
        subsets = []
        subset_date = df[df['payouts.arrival_date'] == date]
        meta_data = create_metadata(index=idx, subset=subset_date, date=date)

        print(meta_data)
        for schedule in ['Subscriptions', 'Activities']:
            subset_schedule = subset_date[subset_date['schedule'] == schedule]
            if schedule == 'Subscriptions':
                subset_subs = subset_schedule.groupby(['section', 'schedule'])[['gross_amount', 'total_fees', 'net_amount']].sum().reset_index()
                subsets.append(subset_subs)

            elif schedule == 'Activities':
                subset_subs = subset_schedule.groupby(['section', 'schedule', 'payment_name'])[['gross_amount', 'total_fees', 'net_amount']].sum().reset_index()
                subsets.append(subset_subs)
        subset_date = pd.concat(subsets, ignore_index=False)
        subset_date = subset_date.fillna('')
        row_order = {
                    'Squirrels' : 1,
                    'Beavers' : 2,
                    'Cubs' : 3,
                    'Scouts' : 4
                    }
        subset_date['no.'] = subset_date['section'].map(row_order)

        column_order = ['no.', 'schedule', 'section', 'payment_name', 'gross_amount', 'total_fees', 'net_amount']
        subset_date = subset_date[column_order]

        subset_date = subset_date.sort_values(by=['schedule', 'no.', 'payment_name'], ascending=[False, True, True]).set_index(keys=['schedule', 'section', 'payment_name'])
        subset_date = subset_date.drop(columns='no.', axis=1)

        dfs.append(subset_date)
        display(subset_date)
    return dfs

# %%
def read_payout_data():
    """main function to apply everything

    Returns:
        pd.DataFrame: _description_
    """
    df = read_data()
    df = clean_data(df)
    dfs = group_data(df)
    return dfs

# file_path='inputs/payout_transactions_reconciliation-export-EX00036AZABPPE.csv'
