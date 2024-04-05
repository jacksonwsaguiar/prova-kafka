# Simulação do apache Kafka para prova

Este projeto tem como objetivo simular a um sistema de interação com o apache kafka utilizando a cloud do confluent. Sendo as mensagens recebidas enviadas para o bando de dados sqlite.

## Requisitos

- Python 3.x
- Biblioteca `confluent-kafka` e `sqlite3` (instalável via `pip install confluent_kafka sqlite3`)

## Estrutura do Projeto

- `producer.py`: Módulo contendo funções para enviar mensagens para o tópico.
- `consumer.py`: Módulo contendo funções se inscrever em no topico e receber mensagens de um tópico MQTT.
- `database_conn.py`: Módulo contendo funções de iniciar e realizar operações no banco de dados.
- `config.py`: Módulo contendo configuração do servidor kafka.
- `test_broker.py`: Módulo contendo funções teste do sistema.

## Execução

1. Execute o script do producer `producer.py` para iniciar a simulação.
    ```
    python3 producer.py
    ```
2. Execute o script `consumer.py` para receber as mensagens.
    ```
    python3 consumer.py
    ```
## Tests
Testes unitarios de casos para integração e persistência dos dados.

### Para executar

1. Execute o script de teste do publisher:
    ```
    pytest test_broker.py
    ```
