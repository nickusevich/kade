## How to build docker image:
1. CD to main folder of procjet
2. Run the following command:
```
docker build -t ui -f ".\UI\Dockerfile".
```

## How to build Docker contianer for UI app:
```
docker-compose -f ".\UI\ui.yml" up -d --build
```