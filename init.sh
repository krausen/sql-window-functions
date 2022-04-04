# !/bin/bash
docker stop $(docker ps -a -q --filter ancestor=postgres --format="{{.ID}}") > /dev/null
docker run --rm -d -e POSTGRES_HOST_AUTH_METHOD="trust" -p 5432:5432 postgres > /dev/null

until pg_isready -U postgres -h localhost -p 5432
do
    sleep 1
done

python generate_data.py
