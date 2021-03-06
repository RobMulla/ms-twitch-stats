import pandas as pd
from glob import glob

def process_stream_sessions(raw_dir='../data/raw/Stream Session*.csv',
                            save_dir=None):
    ss_files = glob(raw_dir)
    ds = []
    for s_id, file in enumerate(ss_files):
        fn = file.split('/')[-1]
        d = pd.read_csv(file)
        d['filename'] = fn
        d['session_id'] = s_id
        ds.append(d)
    ss = pd.concat(ds)
    ss = ss.reset_index(drop=True)

    ss['filename'] = ss['filename'].str.replace(' (1)','', regex=False)
    ss['end_date'] = ss['filename'].str.split(' ').str[-1].str.replace('.csv','', regex=False)
    ss['start_date'] = ss['filename'].str.split(' ').str[-3]

    ss['start_date'] = pd.to_datetime(ss['start_date'].str.replace('_','-'))
    ss['end_date'] = pd.to_datetime(ss['end_date'].str.replace('_','-'))
    ss['start_date'] = pd.to_datetime(ss['start_date'])
    ss['end_date'] = pd.to_datetime(ss['end_date'])

    num_cols = ['Viewers', 'Live Views', 'New Followers',
           'Chatters', 'Chat Messages', 'Ad Breaks', 'Subscriptions',
           'Clips Created', 'All Clip Views']
    ss[num_cols] = ss[num_cols].fillna(0).astype('int')

    ss['overnight'] = ss['start_date'] != ss['end_date']

    ss['hour'] = pd.to_datetime(ss['Timestamp']).dt.hour
    ss['minute'] = pd.to_datetime(ss['Timestamp']).dt.minute

    ss['date'] = ss.apply(lambda row: row['start_date'] if row['hour'] > 12 else row['end_date'], axis=1)
    ss['datetime'] = pd.to_datetime(ss['date'].astype('str') + ' ' + ss['Timestamp'])

    ss['session'] = ss['filename'].str.strip('.csv')

    keep_cols = ['session','datetime', 'Viewers',
                 'Live Views', 'New Followers',
                 'Chatters', 'Chat Messages',
                 'Ad Breaks', 'Subscriptions',
                 'Clips Created', 'All Clip Views', 'session_id']
    if save_dir is not None:
        ss[keep_cols].to_csv('../data/processed/StreamSession.csv', index=False)
    return ss[keep_cols]


def process_channel_analytics(raw_dir="../data/raw/Channel Analytics*.csv"):
    ca_files = glob(raw_dir)

    ds = []
    for file in ca_files:
        fn = file.split("/")[-1]
        d = pd.read_csv(file)
        ds.append(d)
    ca = pd.concat(ds)
    ca["Date"] = pd.to_datetime(ca["Date"])
    ca = ca.loc[~ca.duplicated(subset=["Date"], keep="first")].reset_index(drop=True)
    assert (ca["Date"].value_counts() > 1).sum() == 0
    ca.columns = ["_".join(c.split(" ")) for c in ca.columns]
    ca = ca.reset_index(drop=True)
    ca["streamed"] = ca["Minutes_Streamed"] > 0
    return ca
