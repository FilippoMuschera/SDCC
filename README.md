# Progetto SDCC: Storage chiave-valore con garanzie di consistenza sequenziale o causale

### Esecuzione locale
Il progetto può essere eseguito in locale lanciando lo script `./run.sh` dopo avergli fornito gli opportuni permessi per l'esecuzione (`sudo chmod +x run.sh`).
In questa modalità verrà lanciata una finestra del terminale per ogni server, e una che raccoglie l'output dei vari client.

Modificando lo script `run.sh` si possono andare a settare le diverse variabili d'ambiente per modificare l'esecuzione.
Per una spiegazione dettagliata delle variabili d'ambiente si veda la sezione [Environment](#environment).

Se si vuole eseguire il progetto tramite l'utilizzo di Docker Compose si può utilizzare il comando
`sudo docker-compose up --build`. In questo modo ogni server verrà eseguito all'interno di un container dedicato. 
In questo caso per andare a modificare le variabili d'ambiente sarà sufficiente modificare il file
`.env` secondo le proprie esigenze.

### Esecuzione su istanza EC2
Dopo aver opportunamente avviato un'istanza EC2 dalla dashboard di AWS sarà necessario collegarvisi via SSH.
Per farlo sarà sufficiente eseguire il comando `ssh -i <private-key>.pem ec2-user@<VM-Public-IPv4>`, utilizzando la coppia di chiavi prodotta
in fase di setup dell'istanza.

1. Installazione delle dipendenze:
   1. `sudo yum install docker -y` per installare docker
   2. `sudo curl -L https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose` per installare docker-compose
   3. `sudo chmod +x /usr/local/bin/docker-compose` per renderlo eseguibile
   4. `sudo yum install git -y` servirà per clonare la repository
2. Clone della repository del progetto:
   1. `git clone https://github.com/FilippoMuschera/SDCC.git`
   2. `cd SDCC`
3. Lancio del progetto con Docker Compose:
   1. `sudo service docker start` per avviare il servizio di docker
   2. `sudo docker-compose up --build`

A questo punto nel terminale verranno mostrati tutti gli output dei vari container.

Se si vogliono osservare meglio gli output di uno specifico container è consigliabile collegarsi 
all'istanza EC2 da un secondo terminale (sempre via ssh), e a questo punto eseguire il comando
`sudo docker ps` per vedere i container attivi. Una volta scelto il container che si vuole visualizzare
nello specifico si può eseguire il comando `sudo docker logs -f <container-id>`, per visualizzare tutti
gli output prodotti da un determinato container.    

Per terminare l'esecuzione sarà sufficiente premere la combinazione di tasti Ctrl+C dal terminale in cui è stato
lanciato Docker Compose.

Se si vuole pulire del tutto l'ambiente di esecuzione si può eseguire il comando `sudo docker rm $(sudo docker ps -a -q)`
per eliminare tutti i container.

Per arrestare del tutto Docker: `sudo service docker stop 2> /dev/null && sudo systemctl stop docker.socket`.

Se infine si desidera eliminare tutta la cache di Docker si può eseguire il comando `sudo docker builder prune -a`.


### Environment
- `REPLICAS`: Numero di server (e di client da lanciare). I test sono attualmente configurati per eseguire con 3 repliche, ma il sistema è pensato per lavorare con un numero di repliche generico.
- `LOCAL`: '1' per esecuzione in locale, '0' se si intende lanciare il progetto tramite Docker Compose
- `DOCKER`: '1' se si vuole utilizzare Docker, '0' altrimenti (N.B.: Se `LOCAL` è impostato a '1' avrà la priorità su questa variabile d'ambiente. Quindi se si vuole eseguire il progetto con Docker Compose è necessario settare `LOCAL=0` e `DOCKER=1`).
- `CONSIST_TYPE`: 'Sequential' o 'Causal', in base a quella che si vuole che lo storage garantisca durante l'esecuzione. 
- `DOCKER_OP`: Quale operazione si vuole eseguire se si esegue il progetto tramite Docker Compose. Quando si esegue in locale è possibile sceglierla tramite un prompt interattivo, con Docker Compose si può inserire in questa variabile il numero dell'operazione desiderata. I possibili valori sono '1' o '2' con la consistenza sequenziale, '3' o '4' con quella causale.
- `RANDOM_REPLICA`: '1' o '0'. Se settata a '0' ogni client comunicherà con il server "corrispettivo" (client-1 con server-1, client-2 con server-2, e così via). Altrimenti ogni client sceglierà casualmente il server con cui comunicare (N.B.: Il sistema è realizzato in modo che se ci sono N repliche e N client, anche se casualmente, ogni client sceglierà un server diverso, in modo da non avere server inutilizzati).
- `DELETE_CAUSAL`: Stabilisce se l'operazione di delete va considerata in relazione di causa-effetto con una write ("1") oppure no ("0").

### Operazioni
Le operazioni che si possono lanciare all'avvio del progetto sono:
1. Test sequenziale di base
2. Test sequenziale avanzato
3. Test causale di base
4. Test causale avanzato

Ogni test provvederà, dopo il lancio, a stampare a schermo le operazioni che verranno eseguite da ogni processo.<br>
Ogni server, invece, dopo il termine dell'esecuzione, stamperà il contenuto del suo storage chiave-valore.
