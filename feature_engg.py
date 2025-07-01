from transform import run_transform
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
import os



feature_engg_logger = logging.getLogger('feature engg')
file_handler=logging.FileHandler('log/feature_engg.log',mode='w')
feature_engg_logger.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
feature_engg_logger.addHandler(file_handler)

class Feature_engg:
    def __init__(self,path):
        self.df=pd.read_csv(path)
    
    def duration(self):
        self.df['Movie_duration']=self.df[['duration','type']].apply(lambda x: x['duration'].split(' ')[0] if x['type']=='Movie' else 'Not applicable',axis=1)

        self.df['Seasons']=self.df[['duration','type']].apply(lambda x: x['duration'].split(' ')[0] if x['type']!='Movie' else 'Not applicable',axis=1)
    
        feature_engg_logger.info('Movie duration and Seasons columns added')
    
    def final_cleaning(self):
        self.df.drop(columns=['duration'],inplace=True)
        

    def write_to_csv(self):
        self.df.to_csv('data/gold.csv',index=False)
        feature_engg_logger.info('Gold data created in "data/gold.csv"')



def run_feature_engg(folder,filname):
    feature_engg_logger.info('Running transformation.....')
    silver_path=run_transform(folder,filname)
    feature_engg_logger.info(f'Reading transformed data from {silver_path}')

    feature=Feature_engg(silver_path)

    feature.duration()

    feature.final_cleaning()
    feature.write_to_csv()





if __name__=='__main__':
    folder='data'
    filname='netflix.csv'
    run_feature_engg(folder,filname)
