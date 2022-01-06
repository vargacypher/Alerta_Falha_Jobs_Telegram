# -*- coding: utf-8 -*-
__author__ = 'Guilherme Cardoso de Vargas'
__version__ = 1.0
__maintainer__ = 'Guilherme Cardoso de Vargas'
__email__ = 'vargas93626@gmail.com'
__status__ = 'Development'

########## CONEXÃO E DADOS DO BANCO #############
import pymssql
import pandas as pd
import os
from datetime import date
import time

import telegram_send

import schedule


server = '55.220.444.84'  
database = 'BASE' 


def alerta_jobs ():
    
    #Conexão
    cnxn = pymssql.connect(server=server, database = database)


    df = pd.read_sql('EXEC msdb.dbo.sp_help_jobactivity;', cnxn)

    df['last_executed_step_date2'] = pd.to_datetime(df['last_executed_step_date']).apply(lambda x: x.date())
    df['last_executed_step_date2'] = df['last_executed_step_date2'].astype(str)

    #Falhas no dia atual
    df_today = df[df['last_executed_step_date2'] == date.today().strftime("%Y-%m-%d")]

    #STATUS de falha
    df_failed = df_today[df_today['run_status'] == 0]

    if len(df_failed) > 0:

        for index, element in df_failed.iterrows():

            print('*Falha no Job: {}, Falhou em {}'.format(element['job_name'], element['last_executed_step_date'])) 
            
            #Mensagem Telegram
            telegram_send.send( messages=['*Falha no Job: {}, Falhou em {}'.format(element['job_name'], element['last_executed_step_date'])])
            print('Alerta enviado pelo TELEGRAM')    

    else :
        telegram_send.send( messages=["Nenhum Job teve falha"])  



    cnxn.commit()
    cnxn.close()

#Horarios dos alertas

schedule.every().day.at("08:05").do(alerta_jobs)
schedule.every().day.at("10:30").do(alerta_jobs)
schedule.every().day.at("12:00").do(alerta_jobs)
schedule.every().day.at("16:00").do(alerta_jobs)
schedule.every().day.at("21:00").do(alerta_jobs)

while True:
     
    schedule.run_pending()
    print('Aguardando novo alerta')
    time.sleep(30)

#nohup python2.7 sql_alert.py &