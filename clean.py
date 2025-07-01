import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
import os



bronze_logger = logging.getLogger('bronze')
bronze_logger.setLevel(logging.INFO)

file_handler=logging.FileHandler('log/bronze.log','w')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

bronze_logger.addHandler(file_handler)

class Cleaning_data:
    def __init__(self,path):
        self.df=pd.read_csv(path)
        bronze_logger.info(f'The data is read from {path}')
        bronze_logger.info(f'The data contains {self.df.shape[0]} rows and {self.df.shape[1]} columns')
        bronze_logger.info(f'These columns have null values')
        for col in self.df.columns:
            warning_msg="{}:{:.2f}%".format(col,self.df[col].isna().sum()*100/self.df.shape[0])
            bronze_logger.warning(warning_msg)

    def delete_columns(self):
        self.df.drop(columns=['show_id'],inplace=True)
        bronze_logger.info('show_id columns is removed')
    
    def impute_null_values(self):
        self.df[['director','cast','duration','country']]=self.df[['director','cast','duration','country']].fillna('Unknown')
        bronze_logger.info('The columns director, cast, duration and country are imputed with "unknown" ')

        self.df['rating']=self.df['rating'].fillna(self.df['rating'].mode()[0])


        bronze_logger.info('The columns rating is imputed with most frequent entr')

    def write_to_csv(self):
        self.df.to_csv('data/bronze.csv',index=False)
        bronze_logger.info('Bronze data created in "data/bronze.csv"')

def run_clean(folder,filename):
    clean=Cleaning_data(os.path.join(folder,filename))
    clean.delete_columns()
    clean.impute_null_values()
    clean.write_to_csv()
   
    return os.path.join(folder,'bronze.csv')
    
    


if __name__=='__main__':
    folder='data'
    filename='netflix.csv'
    print(run_clean(folder,filename))

  

