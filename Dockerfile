FROM python:3.10.15

WORKDIR /workflow

COPY requirements.txt . 

RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY src/ .

EXPOSE 8080

CMD ["python", "WorkflowProxyHandler.py"]
