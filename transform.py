from clean import run_clean
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
import os



silver_logger = logging.getLogger('silver')
file_handler=logging.FileHandler('log/silver.log',mode='w')
silver_logger.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
silver_logger.addHandler(file_handler)

class Transform:
    def __init__(self,path):
        
        self.df=pd.read_csv(path)
        silver_logger.info(f'Reading file from {path}')

    def director(self):
        dir_list=self.df['director'].apply(lambda x:x.split(',')).to_list()
        dir_df=pd.DataFrame(dir_list,index=self.df['title']).stack().reset_index().drop(columns=['level_1'])
        self.df1=self.df.merge(dir_df,how='left',on='title').drop(columns=['director']).rename({0:'director'},axis=1)
        silver_logger.info('The DB is transformed wrt director column')
    
    def cast(self):
        cast_list=self.df['cast'].apply(lambda x:x.split(',')).to_list()
        cast_df=pd.DataFrame(cast_list,index=self.df['title']).stack().reset_index().drop(columns=['level_1'])
        self.df1=self.df1.merge(cast_df,how='left',on='title').drop(columns=['cast']).rename({0:'cast'},axis=1)
        silver_logger.info('The DB is transformed wrt cast column')
    
    def country(self):
        country_list=self.df['country'].apply(lambda x:x.split(',')).to_list()
        country_df=pd.DataFrame(country_list,index=self.df['title']).stack().reset_index().drop(columns=['level_1'])
        country_df=country_df[country_df[0]!='']
        self.df1=self.df1.merge(country_df,how='left',on='title').drop(columns=['country']).rename({0:'country'},axis=1)
        silver_logger.info('The DB is transformed wrt country column')
    
    def listed_in(self):
        listed_in_list=self.df['listed_in'].apply(lambda x:x.split(',')).to_list()
        listed_in_df=pd.DataFrame(listed_in_list,index=self.df['title']).stack().reset_index().drop(columns=['level_1'])
        
        self.df1=self.df1.merge(listed_in_df,how='left',on='title').drop(columns=['listed_in']).rename({0:'listed_in'},axis=1)
        silver_logger.info('The DB is transformed wrt listed_in column')
        
    
    def to_date(self):
        self.df1['date_added']=pd.to_datetime(self.df1['date_added'].str.strip())
        self.df1['date_added'].fillna(self.df1['date_added'].max(),inplace=True)
        self.df1['year_added']=self.df1['date_added'].dt.year
        self.df1['month_added']=self.df1['date_added'].dt.month
        
        self.df1['day_of_week']=self.df1['date_added'].dt.day_name()

        silver_logger.info('year, month and day_of_week added')


    def write_to_csv(self):
        self.df1.to_csv('data/silver.csv',index=False)
        silver_logger.info('silver data created in "data/silver.csv"')

def run_transform(folder,filename):
    silver_logger.info('Running clean.py.......')

    bronze_path=run_clean(folder,filename)
        
    transform=Transform(bronze_path)

    transform.director()

    transform.cast()

    transform.country()

    transform.listed_in()

    transform.to_date()

    transform.write_to_csv()

    return os.path.join(folder,'silver.csv')


if __name__=='__main__':

    folder='data'
    filename='netflix.csv'
    run_transform(folder,filename)