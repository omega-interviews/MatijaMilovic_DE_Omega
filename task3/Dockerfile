FROM python:3.10

WORKDIR /task3

COPY requirements.txt .

COPY python_task_data.csv .

RUN pip install -r requirements.txt

COPY ./task3.py ./task3.py

EXPOSE 5000

CMD ["python3", "./task3.py"]