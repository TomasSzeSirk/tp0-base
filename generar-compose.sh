#!/bin/bash
if [ "$#" -ne 2 ]; then
    echo "Error: Invalid number of arguments"
    exit 1
fi

output_file=$1
number_of_clients=$2

if [[ "$number_of_clients" =~ [^0-9] ]]; then
    echo "Error: The value for the number of clients should be an integer"
    exit 1
fi

if [[ $output_file != *.yaml ]]; then
    echo "Error: Output file extension must be .yaml"
    exit 1
fi

echo "name: tp0" > $output_file
echo "services:" >> $output_file

echo "  server:" >> $output_file
echo "    container_name: server" >> $output_file
echo "    image: server:latest" >> $output_file
echo "    entrypoint: python3 /main.py" >> $output_file
echo "    environment:" >> $output_file
echo "      - PYTHONUNBUFFERED=1" >> $output_file
echo "    networks:" >> $output_file
echo "      - testing_net" >> $output_file
echo "    volumes:" >> $output_file
echo "      - ./server/config.ini:/server/config.ini" >> $output_file

for ((i=1; i<$number_of_clients+1; i++))
do  
    echo "" >> $output_file
    echo "  client$i:" >> $output_file
    echo "    container_name: client$i" >> $output_file
    echo "    image: client:latest" >> $output_file
    echo "    entrypoint: /client" >> $output_file
    echo "    environment:" >> $output_file
    echo "      - CLI-ID=$i" >> $output_file
    echo "    networks:" >> $output_file
    echo "      - testing_net" >> $output_file
    echo "    volumes:" >> $output_file
    echo "      - ./client/config.yaml:/config.yaml" >> $output_file
    echo "    depends_on:" >> $output_file
    echo "      - server" >> $output_file
done

echo "" >> $output_file
echo "networks:" >> $output_file
echo "  testing_net:" >> $output_file
echo "    ipam:" >> $output_file
echo "      driver: default" >> $output_file
echo "      config:" >> $output_file
echo "        - subnet: 172.25.125.0/24" >> $output_file

echo "File created successfully"