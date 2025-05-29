#!/bin/bash

echo "" >> .env
echo "MONGODB_URI=$MONGODB_URI" >> .env
echo "ENV=$ENV" >> .env
echo "MYSQL_URI=$MYSQL_URI" >> .env


echo "[arte arena security] Configurando variáveis de ambiente..."

/app/main
tail -f /dev/null
