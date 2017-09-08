import pandas as pd

def logTrans(df):
# Drop the first column id from the input
    df = df.drop('id', 1)
# Store the hour and date seperately
    df['hour']=pd.DatetimeIndex(df['date']).hour
    df['date']=pd.to_datetime(pd.DatetimeIndex(df['date']).date)
#  Group the input by user,pc and add the count as third column    
    a = df.groupby(['user','pc']).size()
    a.name = 'count'
# Reset the index
    a = a.reset_index()
# group by user and find the max count for each. Join the user,max count with previous df and add the pc to it.Change the
# column name as commonPC
# a ==> 
    a = a.groupby('user').agg({"count":"max"}).reset_index().join(a.set_index(['user','count']), on = ['user','count'],how = 'inner').rename(columns = {'pc':'commonPC'})
    
    df = df.join(a.set_index('user'),on=['user'])
    df['common'] = (df['pc']==df['commonPC'])
    
    hours = [6,12,18,24]
    activity = ['Logon','Logoff']
    common = [True, False]
    listConditions = [[x,y,z] for x in hours for y in activity for z in common]
    
    def f(df,x):
        temp = df[(df.hour<x[0])&(df.hour>=x[0]-6) & (df.activity==x[1])&(df.common==x[2])].groupby(['date','user']).size()
        temp.name = str(x[1])+'_'+str(x[0]) + '_'+str(x[2])
        temp = temp.reset_index()
        return temp
    
    arr = [f(df,x) for x in listConditions]
    
    df_all = arr[0]
    for i in range(len(listConditions)-1):
        df_all = df_all.join(arr[i+1].set_index(['date','user']), on=['date','user'], how='outer')

    df_all = df_all.fillna(0)
    
    return df_all
    
    
    
def deviceTrans(df):

    df = df.drop('id', 1)
    df['hour']=pd.DatetimeIndex(df['date']).hour
    df['date']=pd.to_datetime(pd.DatetimeIndex(df['date']).date)
    
    a = df.groupby(['user','pc']).size()
    a.name = 'count'
    a = a.reset_index()
    a = a.groupby('user').agg({"count":"max"}).reset_index().join(a.set_index(['user','count']), on = ['user','count'],how = 'inner').rename(columns = {'pc':'commonPC'})
    
    df = df.join(a.set_index('user'),on=['user'])
    df['common'] = (df['pc']==df['commonPC'])
    
    hours = [6,12,18,24]
    activity = ['Connect','Disconnect']
    common = [True, False]
    listConditions = [[x,y,z] for x in hours for y in activity for z in common]
    
    def f(df,x):
        temp = df[(df.hour<x[0])&(df.hour>=x[0]-6) & (df.activity==x[1])&(df.common==x[2])].groupby(['date','user']).size()
        temp.name = 'device_'+str(x[1])+'_'+str(x[0]) + '_'+str(x[2])
        temp = temp.reset_index()
        return temp
    
    arr = [f(df,x) for x in listConditions]

    df_all = arr[0]
    for i in range(len(listConditions)-1):
        df_all = df_all.join(arr[i+1].set_index(['date','user']), on=['date','user'], how='outer')

    df_all = df_all.fillna(0)
    
    return df_all
    
    
    
def emailTrans(df, work_domain):
    
    df['attachments']=df['attachments'].fillna(0)
    df.loc[df['attachments'] != 0, 'attachments'] = 1

    to = df[['date','user','to','attachments','hour']][df['activity']=='Send']
    cc = df[['date','user','cc','attachments','hour']][df['activity']=='Send'].dropna(subset=['cc'])
    bcc = df[['date','user','bcc','attachments','hour']][df['activity']=='Send'].dropna(subset=['bcc'])
    frm = df[['date','user','from','attachments','hour']][df['activity']=='View']

    def flatten(df,col):
        s = df[col].str.split(';', expand=True).stack()
        i = s.index.get_level_values(0)
        df2 = df.loc[i]
        df2[col] = s.values
        return df2
    
    def address(df):
        df['address'] = 'other'
        indices = [work_domain in x for x in df.iloc[:,2]]
        df['address'][indices] = 'work'
        return df

    to = flatten(to,'to')
    cc = flatten(cc,'cc')
    bcc = flatten(bcc, 'bcc')
    
    to = address(to)
    cc = address(cc)
    bcc = address(bcc)
    frm = address(frm)

    hours = [6,12,18,24]
    attachments = [1,0]
    common = [True, False]
    address = ['other','work']
    listConditions = [[x,y,z,a] for x in hours for y in attachments for z in common for a in address]


    def emailPro(z,y):
        x_total = z.groupby('user').size()
        x_total.name = 'total_email'
        x_total = x_total.reset_index()

        x_indl = z.groupby(['user',y]).size()
        x_indl.name = 'indl_email'
        x_indl = x_indl.reset_index()

        z = z.join(x_total.set_index(['user']),on=['user']).join(x_indl.set_index(['user',y]),on=['user',y])
        z['common'] = z.indl_email > 0.01*z.total_email

        def f(df, x):
            temp = df[(df.hour<x[0])&(df.hour>=x[0]-6) & (df.attachments==x[1])&(df.common==x[2])&(df.address==x[3])].groupby(['date','user']).size()
            temp.name = y +'_'+ str(x[0])+'_'+str(x[1]) + '_'+str(x[2])+'_'+x[3]
            temp = temp.reset_index()
            return temp

        arr = [f(z, x) for x in listConditions]

        return arr

    toArr = emailPro(to,'to')
    ccArr = emailPro(cc,'cc')
    bccArr = emailPro(bcc,'bcc')
    frmArr = emailPro(frm,'from')


    to_all = toArr[0]
    for i in range(len(listConditions)-1):
        to_all = to_all.join(toArr[i+1].set_index(['date','user']), on=['date','user'], how ='outer')

    cc_all = ccArr[0]
    for i in range(len(listConditions)-1):
        cc_all = cc_all.join(ccArr[i+1].set_index(['date','user']), on=['date','user'], how ='outer')

    bcc_all = bccArr[0]
    for i in range(len(listConditions)-1):
        bcc_all = bcc_all.join(bccArr[i+1].set_index(['date','user']), on=['date','user'], how ='outer')

    frm_all = frmArr[0]
    for i in range(len(listConditions)-1):
        frm_all = frm_all.join(frmArr[i+1].set_index(['date','user']), on=['date','user'], how ='outer')


    df_all = to_all.join(cc_all.set_index(['date','user']), on=['date','user'],how='outer')
    df_all = df_all.join(bcc_all.set_index(['date','user']), on=['date','user'],how='outer')
    df_all = df_all.join(frm_all.set_index(['date','user']), on=['date','user'],how='outer')

    df_all = df_all.fillna(0)

    df_all = df_all.reset_index(level=[0,1], drop=True)
    
    return df_all
    
    
    
def httpTrans(df, web_list):

    # web_list = ['wikileaks.org', 'www.dropbox.com', 'linkedin.com','lockheedmartin.com','monster.com','jobhuntersbible.com','aol.com','simplyhired.com']
    
    df['url'] = df['url'].str.split('/', expand=True)[2]

    df['activity']=df['activity'].str.split(' ', expand=True)[1]

    # t1 = df.groupBy('user').count().withColumnRenamed('count', 'total_count')
    t1 = df.groupby('user').size()
    t1.name = 'total_count'
    t1 = t1.reset_index()

    t2 = df.groupby(['user','url']).size()
    t2.name = 'indl_count'
    t2 = t2.reset_index()

    df = df.join(t1.set_index(['user']),on='user')
    df = df.join(t2.set_index(['user','url']),on=['user','url'])

    df['common']=(df['indl_count']>0.002*df['total_count'])
    df['inList']='no'
    df['inList'][[x in web_list for x in df['url']]] = 'yes'

    hours = [6,12,18,24]
    activity = ['Visit', 'Download', 'Upload']
    common = [True, False]
    inList = ['yes','no']
    listConditions = [[x,y,z, a] for x in hours for y in activity for z in common for a in inList]

    def f(df,x):
        temp = df[(df.hour<x[0])&(df.hour>=x[0]-6) & (df.activity==x[1])&(df.common==x[2])&(df.inList==x[3])].groupby(['date','user']).size()
        temp.name = 'http_'+str(x[0])+'_'+str(x[1]) + '_'+str(x[2])+'_'+x[3]
        temp = temp.reset_index()
        return temp

    arr = [f(df,x) for x in listConditions]

    df_all = arr[0]
    for i in range(len(listConditions)-1):
        df_all = df_all.join(arr[i+1].set_index(['date','user']), on=['date','user'], how='outer')

    df_all = df_all.fillna(0)
    df_all = df_all.reset_index(level=[0,1], drop=True)
    
    return df_all
    
    
    
def fileTrans(df, decoy):
    
    df['hour']=pd.DatetimeIndex(df['date']).hour
    df['date']=pd.to_datetime(pd.DatetimeIndex(df['date']).date)
    
    df['location'] = 'local'
    df.loc[df['to_removable_media'],'location'] = 'to_rm'
    df.loc[df['from_removable_media'], 'location'] = 'from_rm'
    
    df = df.drop(['to_removable_media', 'from_removable_media'],1)
    
    decoy = decoy.drop('pc',1)
    decoy['decoy'] = True
    df = df.join(decoy.set_index('decoy_filename'), on='filename')
    df.decoy = df.decoy.fillna(False)
    
    hours = [6,12,18,24]
    activity = ['File Open', 'File Write', 'File Copy', 'File Delete']
    decoy = [True, False]
    location = ['to_rm','from_rm','local']
    listConditions = [[x,y,z,a] for x in hours for y in activity for z in location for a in decoy]
    
    def f(df,x):
        temp = df[(df.hour<x[0])&(df.hour>=x[0]-6) & (df.activity==x[1])&(df.decoy==x[3])&(df.location==x[2])].groupby(['date','user']).size()
        temp.name = 'file_'+str(x[0])+'_'+str(x[1]) + '_'+str(x[2]) + '_'+str(x[3])
        temp = temp.reset_index()
        return temp

    arr = [f(df,x) for x in listConditions]
    
    df_all = arr[0]
    for i in range(len(listConditions)-1):
        df_all = df_all.join(arr[i+1].set_index(['date','user']), ['date','user'], 'outer')

    df_all = df_all.fillna(0)
    
    df_all = df_all.reset_index(level=[0,1], drop=True)
    
    return df_all