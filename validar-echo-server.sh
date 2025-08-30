#!/bin/bash

message="Hello World!"
SERVER_PORT=$(grep SERVER_PORT server/config.ini | cut -d ' ' -f 3)
SERVER_IP=$(grep SERVER_IP server/config.ini | cut -d ' ' -f 3)

response=$(echo "$message" | docker run -i --rm --network tp0_testing_net gophernet/netcat "$SERVER_IP" "$SERVER_PORT")

echo $response
if [ "$message" = "$response" ]; then
    echo "action: test_echo_server | result: success"
else
    echo "action: test_echo_server | result: fail"
fi