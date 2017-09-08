import helper
import pandas as pd
import os

import warnings
warnings.filterwarnings("ignore")

print '---- start feature transformation ----'
# log data transform
# read the input csv file and parse the dates into YYYY-MM-DD HH:MM:SS format
df_log = pd.read_csv('../data/logon_s.csv', parse_dates = ['date'], infer_datetime_format=True)
features_log = helper.logTrans(df_log)
print '---- finished log data transform ----'


# device data transform
df_device = pd.read_csv('../data/device_s.csv', parse_dates = ['date'], infer_datetime_format=True)
features_device = helper.deviceTrans(df_device)
print '---- finished device data transform ----'

# file data transform
df_file = pd.read_csv('../data/file_s.csv', parse_dates=['date'], infer_datetime_format=True)
decoy = pd.read_csv('../data/decoy_file.csv')
features_file = helper.fileTrans(df_file, decoy)
print '---- finished file data transform ----'

# email data transform by chunks
chunksize_email = 10 ** 6
skip_email = 0
for df in pd.read_csv('../data/email_s.csv',iterator=True,chunksize=chunksize_email,\
                      parse_dates=['date'], infer_datetime_format=True, skiprows = skip_email):
    df = df.drop(['id','content'],1)
    df['hour']=pd.DatetimeIndex(df['date']).hour
    df['date']=pd.DatetimeIndex(df['date']).date
    df = df[df['date']!=df['date'].iloc[-1]]
    
    work_domain = 'dtaa'
    
    if skip_email == 0:
        features_email = helper.emailTrans(df, work_domain)
    else:
        features_email = features_email.append(helper.emailTrans(df, work_domain))
    
    skip_email = skip_email + df['date'].count()

print '---- finished email data transform ----'    
    # if not os.path.isfile('../r6.2/feature_email.csv'):
        # feature_email.to_csv('../features/feature_email.csv', mode = 'a', header = True, index = False)
    # else:
        # feature_email.to_csv('../features/feature_email.csv', mode = 'a', header = False, index = False)
        

# http data transform
chunksize_http = 10 ** 7
skip_http = 0
for df in pd.read_csv('../data/http_s.csv',iterator=True,chunksize=chunksize_http,\
                      parse_dates=['date'], infer_datetime_format=True, skiprows = skip_http):
    df = df.drop(['id','content'], 1)
    df['hour']=pd.DatetimeIndex(df['date']).hour
    df['date']=pd.DatetimeIndex(df['date']).date
    df = df[df['date']!=df['date'].iloc[-1]]
    
    web_list = ['wikileaks.org', 'www.dropbox.com', 'linkedin.com','lockheedmartin.com','monster.com','jobhuntersbible.com','aol.com','simplyhired.com']
    
    if skip_http == 0:
        features_http = helper.httpTrans(df, web_list)
    else:
        features_http = features_http.append(helper.httpTrans(df, web_list))
        
    skip_http = skip_http + df['date'].count()
    
    # if not os.path.isfile('../r6.2/feature_http.csv'):
        # feature_http.to_csv('../r6.2/feature_http.csv', mode = 'a', header = True, index = False)
    # else:
        # feature_http.to_csv('../r6.2/feature_http.csv', mode = 'a', header = False, index = False)        

print '---- finished http data transform ----'

# extract role information
def f(path):
    df1 = pd.read_csv(path)
    df1 = df1[['user_id','role','functional_unit','department','employee_name']]
    df1.functional_unit = df1['functional_unit'].str.split('-', expand=True)[0]
    
    return df1
    
filenames = os.listdir('../data/LDAP')
df_role = f('../data/LDAP/'+filenames[0])
for i in range(len(filenames)-1):
    df_role = df_role.append(f('../data/LDAP/'+filenames[i+1]))
    
df_role = df_role.drop_duplicates('user_id')
df_role = df_role.rename(index = str,columns={'user_id':'user'})


# read true insider information
trueInsider = pd.read_csv('../data/trueInsider.csv', parse_dates = ['date'], infer_datetime_format = True)

# join all features
features = features_log.join(features_device.set_index(['date','user']), on=['date','user'])
features = features.join(features_email.set_index(['date','user']), on=['date','user'], how='outer')
features = features.join(features_http.set_index(['date','user']), on=['date','user'], how='outer')
features = features.join(features_file.set_index(['date','user']), on=['date','user'], how='outer')

features = features.fillna(0)
features = features.join(df_role.set_index('user'), on='user', how = 'left')
features = features.join(trueInsider.set_index(['date','user']), on=['date','user'],how='left')
features['insider'] = 0
features['insider'][features.count > 0] = 1

# features = features.loc[:, (features != 0).any(axis=0)]
features['dayofweek']=pd.DatetimeIndex(features['date']).dayofweek
# features = features[features.dayofweek<5]

features.to_csv('../feature/feature_all.csv', index = False)

print '---- finished feature transformation and saved features to csv file ----'