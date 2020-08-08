#Kafka backed inventory management thorugh streams. 

Inventory has a default total set at provider intialization but, can be set later via message to the @stock topic.

Order requests are passed into kafka streams with a materialzied view containing the current inventory available to the provider.

## Start up
* `source venv/bin/active`
* `pip install -r requirements.txt`

    See `faust --help` for more details on other command line options not listed below

## Intializing Services

Check Docker status:
* `docker-compose -f kafka-docker/docker-compose-single-broker.yml ps `

Run up and build docker environment. Includes Kafka and Zookeeper
* `docker-compose -f kafka-docker/docker-compose-single-broker.yml up -d`

To restart take the whole thing down
* `docker-compose -f kafka-docker/docker-compose-single-broker.yml down`

To Spin up the Faust provider (required to process worker requests):
* `faust -A fausted worker -l info`
    * A Trailing & will run it in the background and allow other commands but, running in screen and dettaching is recommended instead for ease of termination.

To Bulk process random orders until inventory exhaustion:
* `faust -A fausted seed-orders`

To See Individual orders or other sources:
* `faust -A fausted send ordering '{"Header": 1, "Line": [{"Product": "A", "Quantity":"1"}, {"Product":"B", "Quantity": 3"}]}'`
