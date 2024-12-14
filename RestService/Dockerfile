FROM python:3.12

WORKDIR /source

COPY ../RestService /source/RestService
COPY ../Backend /source/Backend/

RUN pip install --upgrade pip
RUN pip install --no-cache-dir --upgrade -r /source/RestService/requirements.txt

EXPOSE 80

CMD ["uvicorn", "rest_api:app", "--host", "0.0.0.0", "--port", "80", "--workers", "10"]