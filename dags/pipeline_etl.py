from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
default_args = {
        "owner": "bruno",
        "depends_on_past": False,
        "retries": 0
        }
origem = PostgresHook(postgres_conn_id='abl_escola')
destino = PostgresHook(postgres_conn_id='abl_escola_dw')
dag=DAG(
        dag_id='PIPE_ETL_ESCOLA',
        default_args=default_args,
        start_date=datetime.now(),
        catchup=False,
        schedule_interval=timedelta(days=1)
        )

drop_all_tables_dw = PostgresOperator(
            task_id="drop_all_tables_dw",
            postgres_conn_id="abl_escola_dw",
            sql="sql/drop_all_tables.sql",
            dag = dag
            )

create_tables_dw = PostgresOperator(
            task_id="create_tables_dw",
            postgres_conn_id="abl_escola_dw",
            sql="sql/criar_tabelas_dw.sql",
            dag = dag
            )

## Processamento e Carregamento da tabela DM Cursos
def extract_curso():
    origem_conn = origem.get_conn()
    origem_cursor = origem_conn.cursor()  
    origem_cursor.execute("SELECT COD_CURSO, NOM_CURSO FROM CURSOS;")
    query_transfer = origem_cursor.fetchall()
    return query_transfer

def load_curso():
    query_transfer = extract_curso()
    dest_conn = destino.get_conn()
    dest_cursor = dest_conn.cursor()
    for row in query_transfer:
        dest_cursor.execute(f"INSERT INTO DM_CURSO(ID_CURSO, NOM_CURSO) VALUES({row[0]},'{row[1]}');")
        dest_conn.commit()
    

extract_load_tables_curso = PythonOperator(
                task_id="extract_load_tables_curso",
                python_callable=load_curso,
                dag = dag
            )


## Processamento e Carregamento da tabela DM Departamentos
def extract_departamento():
    origem_conn = origem.get_conn()
    origem_cursor = origem_conn.cursor()  
    origem_cursor.execute("SELECT COD_DPTO, NOME_DPTO FROM DEPARTAMENTOS;")
    query_transfer = origem_cursor.fetchall()
    return query_transfer

def load_departamento():
    query_transfer = extract_departamento()
    dest_conn = destino.get_conn()
    dest_cursor = dest_conn.cursor()
    for row in query_transfer:
        dest_cursor.execute(f"INSERT INTO DM_DEPARTAMENTO(ID_DPTO, NOM_DPTO) VALUES({row[0]},'{row[1]}');")
        dest_conn.commit()

extract_load_tables_departamento = PythonOperator(
                task_id="extract_load_tables_departamento",
                python_callable=load_departamento,
                dag = dag
            )

def extract_disciplina():
    origem_conn = origem.get_conn()
    origem_cursor = origem_conn.cursor()  
    origem_cursor.execute("SELECT COD_DISC, NOME_DISC, CARGA_HORARIA FROM DISCIPLINAS;")
    query_transfer = origem_cursor.fetchall()
    return query_transfer

def load_disciplina():
    query_transfer = extract_disciplina()
    dest_conn = destino.get_conn()
    dest_cursor = dest_conn.cursor()
    for row in query_transfer:
        dest_cursor.execute(f"INSERT INTO DM_DISCIPLINA(ID_DISC, NOME_DISC, CARGA_HORARIA) VALUES({row[0]},'{row[1]}','{row[2]}');")
        dest_conn.commit()

extract_load_tables_disciplina = PythonOperator(
                task_id="extract_load_tables_disciplina",
                python_callable=load_disciplina,
                dag = dag
            )

def extract_tempo():
    origem_conn = origem.get_conn()
    origem_cursor = origem_conn.cursor()  
    origem_cursor.execute("SELECT DISTINCT SEMESTRE FROM MATRICULAS;")
    query_transfer = origem_cursor.fetchall()
    return query_transfer

def load_tempo():
    query_transfer = extract_tempo()
    dest_conn = destino.get_conn()
    dest_cursor = dest_conn.cursor()
    for row in query_transfer:
        dest_cursor.execute(f"INSERT INTO DM_TEMPO(ID_TEMPO, SEMESTRE) VALUES('20113','{row[0]}');")
        dest_conn.commit()

extract_load_tables_tempo = PythonOperator(
                task_id="extract_load_tables_tempo",
                python_callable=load_tempo,
                dag = dag
            )

def extract_ft_reprovacao():
    origem_conn = origem.get_conn()
    origem_cursor = origem_conn.cursor()  
    origem_cursor.execute("SELECT SEMESTRE, COD_DISC, COD_CURSO, COD_DPTO, TOTAL_REP_DISC, \
                            TOTAL_MAT_ALU FROM (SELECT SEMESTRE, COD_DISC, COD_CURSO, COD_DPTO, \
                            COUNT(*) TOTAL_REP_DISC FROM MATRICULAS JOIN ALUNOS USING(MAT_ALU) \
                            JOIN CURSOS USING(COD_CURSO) WHERE STATUS = 'R' GROUP BY SEMESTRE, COD_DISC, \
                            COD_CURSO, COD_DPTO) AS a JOIN(SELECT SEMESTRE, COD_DISC, COD_CURSO, COD_DPTO, \
                            COUNT(*) TOTAL_MAT_ALU FROM MATRICULAS JOIN ALUNOS USING(MAT_ALU)\
                            JOIN CURSOS USING(COD_CURSO) GROUP BY SEMESTRE, COD_DISC, COD_CURSO, \
                            COD_DPTO) AS b USING(SEMESTRE, COD_DISC, COD_CURSO, COD_DPTO);")
    query_transfer = origem_cursor.fetchall()
    return query_transfer

def load_ft_reprovacao():
    query_transfer = extract_ft_reprovacao()
    dest_conn = destino.get_conn()
    dest_cursor = dest_conn.cursor()
    for row in query_transfer:
        dest_cursor.execute(f"INSERT INTO FT_REPROVACAO(ID_TEMPO, ID_DISC, ID_CURSO, ID_DPTO, TOTAL_REP_DISC, TOTAL_MAT_ALU) VALUES('{row[0]}','{row[1]}','{row[2]}','{row[3]}','{row[4]}','{row[5]}');")
        dest_conn.commit()

extract_load_ft_reprovacao = PythonOperator(
                task_id="extract_load_ft_reprovacao",
                python_callable=load_ft_reprovacao,
                dag = dag
            )

def extract_ft_reprovacao_gc():
    origem_conn = origem.get_conn()
    origem_cursor = origem_conn.cursor()  
    origem_cursor.execute("SELECT SEMESTRE, COD_DISC, TOTAL_REP_DISC, TOTAL_MAT_ALU, TOTAL_MAT, \
                            TOTAL_REPROVADOS FROM (SELECT SEMESTRE, COD_DISC, COUNT(*) TOTAL_REP_DISC \
                            FROM MATRICULAS JOIN ALUNOS USING(MAT_ALU) WHERE \
                            STATUS = 'R' AND COTISTA = 'S' GROUP BY SEMESTRE, COD_DISC) AS a JOIN \
                            (SELECT SEMESTRE, COD_DISC, COUNT(*) TOTAL_MAT_ALU FROM \
                            MATRICULAS JOIN ALUNOS USING(MAT_ALU) WHERE COTISTA = 'S' \
                            GROUP BY SEMESTRE, COD_DISC) AS b USING(SEMESTRE, COD_DISC) JOIN \
                            (SELECT SEMESTRE, COD_DISC, COUNT(*) TOTAL_MAT FROM MATRICULAS \
                            GROUP BY SEMESTRE, COD_DISC) AS c USING(SEMESTRE, COD_DISC) JOIN \
                            (SELECT SEMESTRE, COD_DISC, COUNT(*) TOTAL_REPROVADOS FROM MATRICULAS \
                            WHERE STATUS = 'R' GROUP BY SEMESTRE, COD_DISC) AS d USING(SEMESTRE, COD_DISC);")
    query_transfer = origem_cursor.fetchall()
    return query_transfer

def load_ft_reprovacao_gc():
    query_transfer = extract_ft_reprovacao_gc()
    dest_conn = destino.get_conn()
    dest_cursor = dest_conn.cursor()
    for row in query_transfer:
        dest_cursor.execute(f"INSERT INTO FT_REPROVACAO_GC(ID_TEMPO,ID_DISC,TOTAL_REP_COT_DISC,TOTAL_MAT_ALU_COT,TOTAL_MAT,TOTAL_REPROVADOS) VALUES('{row[0]}','{row[1]}','{row[2]}','{row[3]}','{row[4]}','{row[5]}');")
        dest_conn.commit()

extract_load_ft_reprovacao_gc = PythonOperator(
                task_id="extract_load_ft_reprovacao_gc",
                python_callable=load_ft_reprovacao_gc,
                dag = dag
            )

check_tables_creation = DummyOperator(
                task_id="tables_health_check",
                dag=dag    
                )

drop_all_tables_dw >> create_tables_dw >> [extract_load_tables_curso, extract_load_tables_departamento, extract_load_tables_disciplina, extract_load_tables_tempo] >> check_tables_creation >> [extract_load_ft_reprovacao, extract_load_ft_reprovacao_gc]