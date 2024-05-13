## Steps to follow

1. Download kafka connect elasticsearch sink connector
https://www.confluent.io/hub/confluentinc/kafka-connect-elasticsearch
> Note: download 11.x.x, as latest gives error saying Invalid or missing tagline

1. Copy above to the connector folder

1. Update `config/connect-standalone.properties` with the connector folder path

1. Run the connector
`connect-standalone config/connect-standalone.properties config/wikimedia.properties`

1. Send to elasticsearch
`connect-standalone config/connect-standalone.properties config/elasticsearch.properties`

1. verify data in elasticsearch
```
GET wikimedia.recentchange.connect/_search
{
 "query": {
   "match_all": {}
 }
}
```