#!/bin/bash


export REPLICAS=3
export LOCAL=1
export DOCKER=0
export CONSIST_TYPE=Causal
export RANDOM_REPLICA=0

if [ -d "bin" ]; then
    cd bin || exit
    rm -f server client
    cd .. || exit
fi

# Compilare il server
go build -o bin/server ./main/server

# Compilare il client
go build -o bin/client ./main/client

# Funzione per aprire una nuova finestra del terminale
open_terminal() {
    local cmd=$1
    if command -v gnome-terminal > /dev/null; then
        gnome-terminal -- bash -c "$cmd; exec bash"
    elif command -v xterm > /dev/null; then
        xterm -hold -e "$cmd"
    elif command -v konsole > /dev/null; then
        konsole --noclose -e "$cmd"
    else
        echo "No supported terminal found (gnome-terminal, xterm, konsole)"
        exit 1
    fi
}

# Lanciare le istanze del server
for ((i=0; i<REPLICAS; i++)); do
    open_terminal "./bin/server $i"
    echo "Server $i started"
done

    open_terminal "./bin/client"
    echo "Client started"
