FROM python:3.12

WORKDIR /source

# Copy only requirements.txt first to cache dependencies layer
COPY requirements.txt /source/requirements.txt

# Copy the rest of the application files
COPY . /source

RUN pip install --upgrade pip
RUN pip install --no-cache-dir --upgrade -r /source/requirements.txt

EXPOSE 80

CMD ["uvicorn", "rest_api:app", "--host", "0.0.0.0", "--port", "80", "--workers", "2", "--timeout-keep-alive", "5000"]
