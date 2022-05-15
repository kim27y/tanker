import os
import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
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

def take_video(ti):
    video_root = str(Variable.get('video_root'))
    if not video_root:
        raise Exception('No video root.')
    return video_root
    

def process_video(ti):
    ti = str(ti.xcom_pull(task_ids=['take_video'])[0])
    mp4_list = [filename for filename in glob.iglob(os.path.join(ti,'**/*'), recursive=True) if filename.lower().endswith('mp4')]
    for path in mp4_list:
        makedirs(path[:-4])
        vidcap = cv2.VideoCapture(path)

        count = 1

        while True:
            success,image = vidcap.read()
            if not success:
                break
            cv2.imwrite(f"{path[:-4]}/{count}.jpg", image)
        
            count += 1

with DAG(
    dag_id='tanker_dag',
    schedule_interval='* * * * *',
    start_date=datetime(year=2022, month=2, day=1),
    catchup=False
) as dag:# 1. Get current datetime
    task_take_video = PythonOperator(
        task_id='take_video',
        python_callable=take_video
    )# 2. Process current datetime
    task_process_video = PythonOperator(
        task_id='process_video',
        python_callable=process_video
    )# 3. Save processed datetime
    # task_save_video = PythonOperator(
    #     task_id='save_video',
    #     python_callable=save_video
    # )
    # task_take_video >> task_process_video >> task_save_video
    task_take_video >> task_process_video