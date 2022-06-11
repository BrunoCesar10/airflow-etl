# Airflow ETL 

Projeto com pipeline ETL utilizando o Apache Airflow para a transformação e carga contínua de dados fictícios de um sistema operacional de uma universidade para um sistema dimensional

## How To Run

O arquivo 'docker-compose.yml' possui todos os detalhes da execução do projeto. Após a construção do container Docker, o banco de dados operacional será criado e populado automaticamente. O Apache Airflow estará disponível na porta '8080', com as credenciais de acesso disponíveis no docker compose.

##

Baseado nas ferramentas disponíveis no repositório https://github.com/natanascimento/data-engineering