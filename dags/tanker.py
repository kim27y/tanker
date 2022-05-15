import os
import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
import cv2
import os
import glob

def makedirs(path): 
   try: 
        os.makedirs(path) 
   except OSError: 
       if not os.path.isdir(path): 
           raise

def take_data(ti): #확장성을 위해 type 별 json 전달하도록 만들기
    video_root = str(Variable.get('file_root')) # Variable로 할 지, 다른 방식으로 할지 설정
    if not video_root:
        raise Exception('No video root.')
    video_root=video_root+'/video.mp4'#환경에 맞게 변경 (테스트용 경로)
    return video_root
    
def check_data(ti): #데이터의 
    ti = str(ti.xcom_pull(task_ids=['take_data'])[0])
    task_id=''
    if os.path.splitext(ti)[1] == '.mp4':
        task_id='process_video'
    elif os.path.splitext(ti)[1] == '.csv':
        task_id='process_csv'
    return task_id

def process_video(ti): 
    ti = str(ti.xcom_pull(task_ids=['take_data'])[0]) #현재는 str, 나중에는 json으로 받아와서 데이터 종류별로 처리방식 다르게
    makedirs(ti[:-4])
    vidcap = cv2.VideoCapture(ti)

    count = 1

    while True:
        success,image = vidcap.read()
        if not success:
            break
        cv2.imwrite(f"{ti[:-4]}/{count}.jpg", image) # 이부분에 image를 데이터베이스에 넣도록 처리하면 됨 (비디오이름, 시간, 프레임번째수, 비디오)
        
    
        count += 1


def process_csv(ti):
    pass

with DAG(
    dag_id='tanker_dag',
    schedule_interval='0 17 * * *',
    start_date=datetime(year=2022, month=2, day=1),
    catchup=False
) as dag:# 1. Get data (data classification)
    task_take_video = PythonOperator(
        task_id='take_data',
        python_callable=take_data
    )
    # 2. Check which data type is
    task_check_data = BranchPythonOperator(
        task_id='check_data',
        python_callable=check_data
    )
    # 3-1. Processing and save data each data type
    task_process_video = PythonOperator(
        task_id='process_video',
        python_callable=process_video
    )
    # 3-2. Processing and save data each data type
    task_process_csv = PythonOperator(
        task_id='process_csv',
        python_callable=process_csv
    )
    # 4. Save processed datetime # 시각화 부분으로 만들면 되지 않을까
    # task_save_video = PythonOperator(
    #     task_id='save_video',
    #     python_callable=save_video
    # )
    task_take_video >> task_check_data >> [task_process_video,task_process_csv]