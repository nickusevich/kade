FROM python:3.12

WORKDIR /source

#COPY ../Rest_service/requirements.txt /source/requirements.txt
COPY ../Rest_service /source
COPY ../Database /source/Database

RUN pip install --upgrade pip
RUN pip install --no-cache-dir --upgrade -r /source/requirements.txt

EXPOSE 80

CMD ["uvicorn", "rest_api:app", "--host", "0.0.0.0", "--port", "80", "--workers", "10"]