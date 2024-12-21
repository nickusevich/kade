## How to build docker image:
1. CD to main folder of procjet
2. Run the following command:
```
docker build -t rest-service -f ./RestService/Dockerfile .

```

## How to build Docker contianer for rest api:
```
docker-compose -f ".\RestService\rest_service.yml" up -d --build
```
