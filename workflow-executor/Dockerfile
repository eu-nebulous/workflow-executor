FROM python:3.10.15

WORKDIR /workflow

COPY requirements.txt . 

RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY src/*.py .

EXPOSE 8080
EXPOSE 9999

CMD ["python", "WorkflowProxyHandler.py"]
