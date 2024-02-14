import mysql.connector as msql
from mysql.connector import Error

import sqlite3
from datetime import datetime


# banco sqlite
path_db_sqlite = "C:/Users/pactoti02/pactoapps/db.sqlite3"
banco = 'db'

# banco mysql
host = 'localhost'
user = 'admin'
password = 'admin'
port = '3306'
charset = 'utf8mb4'

# pasta log
path_log = "C:/Users/pactoti02/Desktop/sqlite_to_mysql"

def banco_mysql(arquivo_nome_tabela):
    try:
        conn = msql.connect(host=host, user=user, password=password, port=port, database=banco, consume_results=True, charset=charset)
        if conn.is_connected():
            cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {arquivo_nome_tabela};")
        columns_names = [i[0] for i in cursor.description]
    except Error as e:
        print("Error while connecting to MySQL", e)
        registrar_log(str(e) + ' ' + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))
    return conn,cursor, columns_names

def registrar_log(texto):
    arquivo = open(path_log + "/log.txt", "a+")
    arquivo.write(texto+'\n')
    arquivo.close()

con = sqlite3.connect(path_db_sqlite) # conecta banco local
cur = con.cursor()

tables = cur.execute('SELECT name FROM sqlite_master WHERE type="table" and name NOT IN ("sqlite_sequence")') #lista todas as tabelas do banco
# tables = cur.execute('SELECT name FROM sqlite_master WHERE type="table" and name NOT IN ("sqlite_sequence", "django_migrations", "django_content_type", "auth_group_permissions", "auth_permission", "auth_group", "apipacto_user_groups", "apipacto_user_user_permissions","django_admin_log","django_session", "authtoken_token", "apipacto_admcondominio", "apipacto_enderecouser", "apipacto_admimovel", "apipacto_categoriaparceiro", "apipacto_parceirocondominio", "apipacto_avaliacaoparceiro", "apipacto_telefoneuser", "apipacto_solicitacaofaturamentoterceiro", "apipacto_dadosparaconexaolegado", "apipacto_importacaotelefone", "apipacto_parceiro", "apipacto_enderecocondominio", "apipacto_gerente", "apipacto_categoriadocumento", "apipacto_mural", "apipacto_documento", "apipacto_avisogrupo", "apipacto_categoriadocumentoblaklist", "apipacto_categoriadocumentowhitelist", "apipacto_emailuser", "apipacto_cacheexpiration", "apipacto_termo","apipacto_aceitetermo", "background_task_completedtask","background_task","apipacto_ocorrencia","apipacto_ocorrenciaanexo", "apipacto_comunicado", "apipacto_condominioreserva", "apipacto_pautaanexo", "apipacto_solicitacaomalote", "apipacto_cartaodespesa", "apipacto_pushnameconfig", "apipacto_pushname", "apipacto_visitantehistoricowhitelist", "apipacto_pautaresposta", "apipacto_pautaopcao", "apipacto_veiculoentrada", "apipacto_veiculo", "apipacto_pessoaautorizada", "apipacto_visitante", "apipacto_sms", "apipacto_agendamentoacao", "apipacto_acaoapi", "apipacto_atualizacaofontes", "apipacto_sindico", "apipacto_funcionario", "apipacto_importacaolegado", "apipacto_ultimousuariologado", "apipacto_reservaespacohorario","apipacto_condominio", "apipacto_unidade", "apipacto_avisouser", "apipacto_avisounidade", "apipacto_avisouser", "apipacto_avisounidade", "apipacto_aviso", "apipacto_extrato", "apipacto_segundaviaboleto", "django_q_ormq", "django_q_schedule", "django_q_task", "apipacto_nadaconsta", "apipacto_sistema", "apipacto_espacoregrabloqueio", "apipacto_unidadeespaco", "apipacto_condominioespaco", "apipacto_manutencaoanexo", "apipacto_ocorrenciaesclarecimento", "apipacto_encomenda", "apipacto_emailpacto", "apipacto_manutencao", "apipacto_manutencaoesclarecimento", "apipacto_lotesmsboleto", "apipacto_conselheiro", "apipacto_pauta", "apipacto_reservaespaco", "apipacto_contabancaria", "apipacto_solicitacaotransferencia", "apipacto_requisicao", "apipacto_assembleia", "apipacto_solicitacaocadastro", "apipacto_fornecedor", "apipacto_endereco", "apipacto_locatario", "apipacto_proprietario", "apipacto_user", "apipacto_ordempagamento", "apipacto_userdevice", "apipacto_userdevicetoken")') #lista todas as tabelas do banco
lit_tables = list(tables)

texto = 'Horário de inicialização: '+ datetime.now().strftime('%d/%m/%Y %H:%M:%S') +'\n'
registrar_log(texto)

for table in lit_tables:
    table = table[0]

    res_sqlite = cur.execute('SELECT * FROM ' + table)
    columns_sqlite =  [i[0] for i in res_sqlite.description]
    con_mysql, cursor_mysql, columns_mysql = banco_mysql(table)
    
    for i, column in enumerate(columns_mysql):
        if not column == columns_sqlite[i]:
            raise Exception('Tabela com colunas diferente: '+column+' '+table)
    sql = "SET FOREIGN_KEY_CHECKS = 0;"
    cursor_mysql.execute(sql)
    con_mysql.commit()

    col = str(tuple(columns_mysql)).replace("'", "`")
    columns = str(tuple(["%s" for i in cur.description])).replace("'", "")
    if len(columns_mysql) == 1:
        col = col.replace(",","")
        columns = columns.replace(",","")
    for row in res_sqlite:
        print(table)
        row = list(row)
        for j, r in enumerate(row):
            if r == '':
                row[j] = None
        sql = f"INSERT INTO {banco}.{table} {col} VALUES " + columns+';'
        cursor_mysql.execute(sql, tuple(row))
        con_mysql.commit()
        
        print(row)
    registrar_log(table + ' ' + datetime.now().strftime('%d/%m/%Y %H:%M:%S') + '\n')

texto = 'Horário de finalização: '+ datetime.now().strftime('%d/%m/%Y %H:%M:%S') +'\n'
registrar_log(texto)

 

