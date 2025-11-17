# =====================================================================================
# BEM - VINDO À VERSÃO 3.6 DO SISTEMA DE ALMOXARIFADO
	# Alteração: tela de login melhorada, mas não finalizada.
# =====================================================================================

# --- Imports Necessários ---
import streamlit as st
import mysql.connector
from mysql.connector import Error
import pandas as pd
import hashlib
import io
from datetime import datetime

# DICIONÁRIO PARA TRADUZIR MESES
meses_pt_en = {
    'janeiro': 'january', 'fevereiro': 'february', 'março': 'march',
    'abril': 'april', 'maio': 'may', 'junho': 'june',
    'julho': 'july', 'agosto': 'august', 'setembro': 'september',
    'outubro': 'october', 'novembro': 'november', 'dezembro': 'december'
}

def traduzir_data_pt_en(data_str):
    """Recebe uma data em texto (ex: '27 Fevereiro 2025') e a traduz para inglês."""
    if not isinstance(data_str, str):
        return data_str  # Retorna o valor original se não for texto
    
    data_str_lower = data_str.lower()
    for mes_pt, mes_en in meses_pt_en.items():
        if mes_pt in data_str_lower:
            # Substitui o mês em português pelo inglês e retorna
            return data_str_lower.replace(mes_pt, mes_en)
            
    return data_str # Retorna o original se nenhum mês for encontrado

import os
import tempfile

# =====================================================================================
# SEÇÃO DE CONFIGURAÇÃO E FUNÇÕES GLOBAIS
# =====================================================================================

# --- Configuração Inicial da Página ---
st.set_page_config(layout="wide")

#region Funções de Conexão e Autenticação

@st.cache_resource(ttl=600) # Cache dura 10 minutos ou até invalidar
def _get_cached_connection():
    """Cria a conexão física e a mantém em cache."""
    return mysql.connector.connect(
        host=st.secrets["db"]["host"],
        user=st.secrets["db"]["user"],
        password=st.secrets["db"]["password"],
        database=st.secrets["db"]["database"],
        port=st.secrets["db"]["port"]
    )

def conectar_mysql_leitura():
    """
    Tenta usar a conexão cacheada. Se ela caiu, recria.
    Essa é a mágica para performance + estabilidade.
    """
    try:
        cnx = _get_cached_connection()
        # Faz um teste leve (ping) para ver se a conexão ainda está viva
        if not cnx.is_connected():
            st.cache_resource.clear() # Limpa o cache velho
            cnx = _get_cached_connection() # Cria nova
            
        # Um teste extra garantido (ping do MySQL)
        cnx.ping(reconnect=True, attempts=3, delay=1)
        return cnx
    except Error:
        # Se der erro grave, força limpeza total e nova tentativa
        st.cache_resource.clear()
        return _get_cached_connection()

# A função de escrita continua criando uma nova por segurança, 
# mas como é usada menos vezes, não impacta tanto a navegação.
def obter_conexao_para_transacao():
    """CONEXÃO PARA ESCRITA: Obtém uma conexão nova (sem cache)."""
    # Reutilizamos a lógica de criar conexão, mas sem passar pelo cache_resource direto
    return mysql.connector.connect(
        host=st.secrets["db"]["host"],
        user=st.secrets["db"]["user"],
        password=st.secrets["db"]["password"],
        database=st.secrets["db"]["database"],
        port=st.secrets["db"]["port"]
    )

def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(stored_password_hash, provided_password):
    return stored_password_hash == hash_password(provided_password)

def login_user(username, password):
    conexao = conectar_mysql_leitura()
    if conexao:
        try:
            cursor = conexao.cursor(dictionary=True)
            cursor.execute("SELECT * FROM usuarios WHERE nome_usuario = %s", (username,))
            user_data = cursor.fetchone()
            if user_data and verify_password(user_data['senha_hash'], password):
                return user_data
        except Error as e:
            st.error(f"Erro durante o login: {e}")
    return None

def registrar_usuario_db(nome, email, senha_hash):
    conexao = obter_conexao_para_transacao()
    if not conexao: return False, "Falha na conexão."
    try:
        cursor = conexao.cursor()
        comando = "INSERT INTO usuarios (nome_usuario, email, senha_hash) VALUES (%s, %s, %s)"
        valores = (nome, email, senha_hash)
        cursor.execute(comando, valores)
        conexao.commit()
        st.cache_resource.clear()
        st.cache_data.clear() ##não sei se prejudica em algo
        return True, "Usuário cadastrado com sucesso!"
    except Error as e:
        conexao.rollback()
        if e.errno == 1062: return False, "Erro: Nome de usuário ou e-mail já existe."
        return False, f"Erro ao registrar usuário: {e}"
    finally:
        if conexao.is_connected(): conexao.close()

#endregion

#region Funções de Lógica de Negócio (Banco de Dados)

@st.cache_data(ttl=3600)
def buscar_categorias_unicas():
    conexao = conectar_mysql_leitura()
    if not conexao: return []
    try:
        df = pd.read_sql("SELECT DISTINCT categoria FROM materiais WHERE categoria IS NOT NULL AND categoria != '' ORDER BY categoria", conexao)
        return df['categoria'].tolist()
    except Error: return []

@st.cache_data(ttl=3600)
def buscar_unidades_unicas():
    conexao = conectar_mysql_leitura()
    if not conexao: return []
    try:
        df = pd.read_sql("SELECT DISTINCT unidade FROM materiais WHERE unidade IS NOT NULL AND unidade != '' ORDER BY unidade", conexao)
        return df['unidade'].tolist()
    except Error: return []

@st.cache_data(ttl=300)
def buscar_materiais_paginados(obra_id, filtros, pagina=1, itens_por_pagina=20, ver_todos=False):
    """Busca materiais no banco com filtros e paginação, juntando com o estoque da obra específica para obter a quantidade correta."""
    conexao = conectar_mysql_leitura()
    if not conexao: return pd.DataFrame(), 0
    
    # A base da query agora faz a junção (LEFT JOIN) entre materiais e o estoque da obra
    query_base = """
    FROM 
        materiais AS m
    LEFT JOIN 
        estoque_obra AS eo ON m.id = eo.material_id AND eo.obra_id = %s
    WHERE 1=1
    """
    # O primeiro valor na lista de parâmetros agora é sempre o obra_id
    valores = [obra_id]

    # A lógica de filtros continua a mesma, mas aplicada à tabela 'm' (materiais)
    if filtros['nome']:
        query_base += " AND m.descricao LIKE %s"
        valores.append(f"%{filtros['nome']}%")
    if filtros['categoria'] != "Todas":
        query_base += " AND m.categoria = %s"
        valores.append(filtros['categoria'])
        
    try:
        cursor = conexao.cursor(dictionary=True)
        
        # A query para contar o total de itens continua funcionando
        cursor.execute(f"SELECT COUNT(m.id) as total {query_base}", tuple(valores))
        total_itens = cursor.fetchone()['total']
        
        # A grande mudança está aqui: selecionamos todas as colunas de materiais (m.*)
        # e a quantidade da tabela de estoque (eo.quantidade).
        # COALESCE garante que, se não houver registro de estoque, a quantidade seja 0.
        query_final = f"""
        SELECT 
            m.*, 
            COALESCE(eo.quantidade, 0) AS estoque_atual 
        {query_base} 
        ORDER BY m.descricao ASC
        """
        valores_finais = valores

        # A lógica de paginação continua a mesma
        if not ver_todos:
            offset = (pagina - 1) * itens_por_pagina
            query_final += " LIMIT %s OFFSET %s"
            valores_finais.extend([itens_por_pagina, offset])
        
        cursor.execute(query_final, tuple(valores_finais))
        return pd.DataFrame(cursor.fetchall()), total_itens
        
    except Error as e:
        st.error(f"Erro ao buscar materiais: {e}")
        return pd.DataFrame(), 0

@st.cache_data(ttl=300)
def buscar_todos_codigos_materiais():
    conexao = conectar_mysql_leitura()
    if not conexao: return []
    try:
        df = pd.read_sql("SELECT codigo FROM materiais", conexao)
        return df['codigo'].tolist()
    except Error: return []

@st.cache_data(ttl=3600)            
def buscar_material_por_id(material_id):
    conexao = conectar_mysql_leitura()
    if not conexao: return None
    try:
        cursor = conexao.cursor(dictionary=True)
        cursor.execute("SELECT * FROM materiais WHERE id = %s", (material_id,))
        return cursor.fetchone()
    except Error: return None

@st.cache_data(ttl=300)
def buscar_materiais_para_selecao():
    conexao = conectar_mysql_leitura()
    if not conexao: return pd.DataFrame()
    try:
        df = pd.read_sql("SELECT id, codigo, descricao, unidade FROM materiais ORDER BY descricao", conexao)
        df['display'] = df['codigo'] + ' - ' + df['descricao']
        return df
    except Error: return pd.DataFrame()

@st.cache_data(ttl=3600)
def buscar_outras_obras(obra_id_atual):
    conexao = conectar_mysql_leitura()
    if not conexao: return pd.DataFrame()
    try:
        query = "SELECT id, nome_obra FROM obras WHERE id != %s ORDER BY nome_obra"
        return pd.read_sql(query, conexao, params=(obra_id_atual,))
    except Error: return pd.DataFrame()

@st.cache_data(ttl=3600)
def buscar_historico_db(obra_id, limit=None, data_inicio=None, data_fim=None, tipos_transacao=None):
    """Busca o histórico de movimentações com filtros avançados de forma robusta."""
    # MUDANÇA CRÍTICA: Usa uma conexão nova para garantir a visão mais atual dos dados.
    conexao = obter_conexao_para_transacao()
    if not conexao: return pd.DataFrame()

    query = "SELECT id, DATE_FORMAT(data, '%d/%m/%Y %H:%i') as data_formatada, tipo, descricao, quantidade, fornecedor, recebedor, observacoes FROM movimentacoes WHERE obra_id = %s AND estornado = FALSE"
    params = [obra_id]

    if data_inicio:
        query += " AND DATE(data) >= %s"
        params.append(data_inicio)
    if data_fim:
        query += " AND DATE(data) <= %s"
        params.append(data_fim)
    if tipos_transacao:
        if len(tipos_transacao) > 0:
            query += f" AND tipo IN ({','.join(['%s'] * len(tipos_transacao))})"
            params.extend(tipos_transacao)
    
    query += " ORDER BY data DESC"
    
    if limit:
        query += " LIMIT %s"
        params.append(limit)
        
    try:
        # A lógica de execução permanece a mesma, agora com a conexão correta.
        cursor = conexao.cursor(dictionary=True)
        cursor.execute(query, tuple(params))
        resultados = cursor.fetchall()
        return pd.DataFrame(resultados)
    except Error as e:
        st.error(f"Erro ao buscar histórico: {e}")
        return pd.DataFrame()
    finally:
        # Garante que a conexão nova seja sempre fechada.
        if conexao.is_connected():
            conexao.close()

@st.cache_data(ttl=3600)
def cadastrar_material_db(codigo, nome, unidade, categoria, est_min, est_max, obs):
    conexao = obter_conexao_para_transacao()
    if not conexao: return False, "Falha na conexão"
    try:
        cursor = conexao.cursor()
        comando = "INSERT INTO materiais (codigo, descricao, unidade, categoria, estoque_minimo, estoque_maximo, observacoes) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        valores = (codigo, nome, unidade, categoria, est_min, est_max, obs)
        cursor.execute(comando, valores)
        conexao.commit()
        st.cache_resource.clear()
        st.cache_data.clear()
        return True, f"Material '{nome}' cadastrado com sucesso!"
    except Error as e:
        conexao.rollback(); return False, f"Erro ao cadastrar: {e}"
    finally:
        if conexao.is_connected(): conexao.close()

@st.cache_data(ttl=3600)
def atualizar_material_db(mid, codigo, descricao, unidade, categoria, est_min, est_max, obs):
    conexao = obter_conexao_para_transacao()
    if not conexao: return False, "Falha na conexão"
    try:
        cursor = conexao.cursor()
        comando = "UPDATE materiais SET codigo=%s, descricao=%s, unidade=%s, categoria=%s, estoque_minimo=%s, estoque_maximo=%s, observacoes=%s WHERE id=%s"
        valores = (codigo, descricao, unidade, categoria, est_min, est_max, obs, mid)
        cursor.execute(comando, valores)
        conexao.commit()
        st.cache_resource.clear()
        st.cache_data.clear() 
        return True, f"Material '{descricao}' atualizado!"
    except Error as e:
        conexao.rollback(); return False, f"Erro ao atualizar: {e}"
    finally:
        if conexao.is_connected(): conexao.close()

@st.cache_data(ttl=3600)
def cadastrar_materiais_em_lote_db(df_materiais):
    conexao = obter_conexao_para_transacao()
    if not conexao: return 0, 0, []
    sucessos, erros, mensagens_erro = 0, 0, []
    comando = "INSERT INTO materiais (codigo, descricao, unidade, categoria, estoque_minimo, estoque_maximo, observacoes) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    cursor = conexao.cursor()
    for _, row in df_materiais.iterrows():
        try:
            valores = (row['codigo'], row['descricao'], row['unidade'], row['categoria'],
                row['estoque_minimo'] if pd.notna(row['estoque_minimo']) else None,
                row['estoque_maximo'] if pd.notna(row['estoque_maximo']) else None,
                row['observacoes'] if pd.notna(row['observacoes']) else None)
            cursor.execute(comando, valores)
            conexao.commit(); sucessos += 1
        except Error as e:
            erros += 1; conexao.rollback()
            mensagens_erro.append(f"Material Cód: {row['codigo']} - Erro: {e}")
    st.cache_resource.clear()
    st.cache_data.clear()
    return sucessos, erros, mensagens_erro

@st.cache_data(ttl=3600)
def excluir_materiais_db(lista_de_ids):
    if not lista_de_ids: return False, "Nenhum material selecionado."
    conexao = obter_conexao_para_transacao()
    if not conexao: return False, "Falha na conexão"
    try:
        cursor = conexao.cursor()
        placeholders = ','.join(['%s'] * len(lista_de_ids))
        comando = f"DELETE FROM materiais WHERE id IN ({placeholders})"
        cursor.execute(comando, tuple(lista_de_ids))
        conexao.commit()
        st.cache_resource.clear()
        st.cache_data.clear()
        return True, f"{cursor.rowcount} material(is) excluído(s)!"
    except Error as e:
        conexao.rollback(); return False, f"Erro ao excluir: {e}"
    finally:
        if conexao.is_connected(): conexao.close()

@st.cache_data(ttl=3600)
def registrar_movimentacao_db(obra_id, usuario_id, dados):
    tipo = dados['tipo']
    conexao = obter_conexao_para_transacao()
    if not conexao: return False, "Falha na conexão."
    try:
        cursor = conexao.cursor()
        material_id = dados['material_id']
        quantidade = dados['quantidade']
        
        # Lógica para movimentações que afetam o estoque da obra atual
        if tipo in ["Entrada", "Saída"]:
            conexao.start_transaction()
            
            # Passo 1: Registrar a movimentação no histórico (lógica antiga, continua igual)
            sql_insert = "INSERT INTO movimentacoes (obra_id, data, tipo, descricao, quantidade, fornecedor, recebedor, observacoes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            valores_insert = (obra_id, dados['data'], dados['tipo'], dados['descricao'], quantidade, dados.get('fornecedor'), dados.get('recebedor'), dados['observacoes'])
            cursor.execute(sql_insert, valores_insert)

            # --- NOVA LÓGICA DE ATUALIZAÇÃO DE ESTOQUE ---
            # Passo 2: Garante que a linha de estoque existe para este material nesta obra
            cursor.execute("INSERT INTO estoque_obra (material_id, obra_id, quantidade) VALUES (%s, %s, 0) ON DUPLICATE KEY UPDATE quantidade=quantidade", (material_id, obra_id))

            # Passo 3: Define se a quantidade é positiva (Entrada) ou negativa (Saída)
            qtd_para_update = quantidade if tipo == "Entrada" else -quantidade
            
            # Passo 4: Atualiza o estoque específico da obra na nova tabela
            sql_update = "UPDATE estoque_obra SET quantidade = quantidade + %s WHERE material_id = %s AND obra_id = %s"
            cursor.execute(sql_update, (qtd_para_update, material_id, obra_id))

            conexao.commit()
            st.cache_resource.clear()
            st.cache_data.clear() # Limpa ambos os caches por segurança
            
            return True, f"Movimentação de '{tipo}' registrada com sucesso!"
            
        # Lógica para movimentações que geram uma solicitação (não afeta o estoque imediatamente)
        elif tipo in ["Transferência", "Empréstimo", "Devolução"]:
            # Esta parte não mexe no estoque, apenas cria a solicitação, então continua a mesma.
            sql_pendente = "INSERT INTO transacoes_pendentes (obra_origem_id, obra_destino_id, material_id, quantidade, tipo_transacao, observacoes, solicitado_por_usuario_id) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            valores_pendente = (dados['obra_origem_id'], dados['obra_destino_id'], material_id, quantidade, dados['tipo'], dados['observacoes'], usuario_id)
            cursor.execute(sql_pendente, valores_pendente)
            conexao.commit()

            st.cache_resource.clear()
            st.cache_data.clear()
            return True, f"Solicitação de '{tipo}' enviada. Aguardando aprovação."
            
    except Error as e:
        if conexao.is_connected():
            conexao.rollback()
        return False, f"Erro ao registrar: {e}"
    finally:
        if conexao.is_connected():
            conexao.close()

@st.cache_data(ttl=3600)
def criar_movimentacao_estorno_db(movimentacao_id, usuario_id):
    conexao = obter_conexao_para_transacao()
    if not conexao: return False, "Falha na conexão."
    try:
        cursor = conexao.cursor(dictionary=True)
        conexao.start_transaction()

        # Busca a movimentação original para obter os detalhes
        cursor.execute("SELECT * FROM movimentacoes WHERE id = %s", (movimentacao_id,))
        mov_original = cursor.fetchone()
        if not mov_original or mov_original['estornado']:
            return False, "Movimentação não encontrada ou já estornada."

        # Encontra o ID do material pela descrição
        cursor.execute("SELECT id FROM materiais WHERE descricao = %s", (mov_original['descricao'],))
        material = cursor.fetchone()
        if not material:
            return False, f"Material '{mov_original['descricao']}' não encontrado."
        material_id = material['id']
        obra_id = mov_original['obra_id']
        
        # Define o tipo de movimentação inversa
        tipo_inverso = "Saída" if mov_original['tipo'] == "Entrada" else "Entrada" if mov_original['tipo'] == "Saída" else None
        if not tipo_inverso:
            return False, f"Não é possível estornar uma movimentação do tipo '{mov_original['tipo']}'."

        # 1. Cria o novo registro de movimentação de estorno
        observacao_estorno = f"ESTORNO da Movimentação ID: {movimentacao_id}. Por usuário ID: {usuario_id}."
        sql_insert = "INSERT INTO movimentacoes (obra_id, data, tipo, descricao, quantidade, observacoes, fornecedor, recebedor) VALUES (%s, NOW(), %s, %s, %s, %s, %s, %s)"
        valores_insert = (obra_id, tipo_inverso, mov_original['descricao'], mov_original['quantidade'], observacao_estorno, mov_original['fornecedor'], mov_original['recebedor'])
        cursor.execute(sql_insert, valores_insert)

        # 2. ATUALIZA O ESTOQUE NA TABELA CORRETA (estoque_obra)
        qtd_para_update = mov_original['quantidade'] if tipo_inverso == "Entrada" else -mov_original['quantidade']
        cursor.execute("UPDATE estoque_obra SET quantidade = quantidade + %s WHERE material_id = %s AND obra_id = %s", (qtd_para_update, material_id, obra_id))
        
        # 3. Marca a movimentação original como estornada
        cursor.execute("UPDATE movimentacoes SET estornado = TRUE WHERE id = %s", (movimentacao_id,))
        
        conexao.commit()
        
        # 4. LIMPA OS CACHES CORRETAMENTE
        st.cache_resource.clear()
        st.cache_data.clear()
        
        return True, "Movimentação estornada com sucesso!"

    except Error as e:
        conexao.rollback()
        return False, f"Erro ao estornar: {e}"
    finally:
        if conexao.is_connected(): conexao.close()

@st.cache_data(ttl=3600)
def buscar_prazos_compra_db():
    """Busca todos os prazos de compra cadastrados por categoria."""
    conexao = conectar_mysql_leitura()
    if not conexao: return pd.DataFrame()
    query = "SELECT id, categoria, prazo_dias FROM prazos_compra ORDER BY categoria ASC"
    try:
        return pd.read_sql(query, conexao)
    except Error as e:
        st.error(f"Erro ao buscar prazos de compra: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def salvar_prazo_compra_db(categoria, prazo_dias):
    """Salva ou atualiza o prazo de compra para uma categoria."""
    conexao = obter_conexao_para_transacao()
    if not conexao: return False, "Falha na conexão."
    try:
        cursor = conexao.cursor()
        # Usa INSERT ... ON DUPLICATE KEY UPDATE para inserir ou atualizar
        sql = """
            INSERT INTO prazos_compra (categoria, prazo_dias) 
            VALUES (%s, %s) 
            ON DUPLICATE KEY UPDATE prazo_dias = %s
        """
        cursor.execute(sql, (categoria, prazo_dias, prazo_dias))
        conexao.commit()
        st.cache_resource.clear() # Limpa cache para garantir que a próxima leitura veja a mudança
        st.cache_data.clear()
        return True, "Prazo salvo com sucesso!"
    except Error as e:
        conexao.rollback()
        return False, f"Erro ao salvar prazo: {e}"
    finally:
        if conexao.is_connected(): conexao.close()

@st.cache_data(ttl=3600)
def remover_prazo_compra_db(prazo_id):
    """Remove um prazo de compra pelo seu ID."""
    conexao = obter_conexao_para_transacao()
    if not conexao: return False, "Falha na conexão."
    try:
        cursor = conexao.cursor()
        sql = "DELETE FROM prazos_compra WHERE id = %s"
        cursor.execute(sql, (prazo_id,))
        conexao.commit()
        st.cache_resource.clear()
        st.cache_data.clear()
        return True, "Prazo removido com sucesso."
    except Error as e:
        conexao.rollback()
        return False, f"Erro ao remover prazo: {e}"
    finally:
        if conexao.is_connected(): conexao.close()

@st.cache_data(ttl=3600)
def calcular_prazo_maximo_kit(kit_id):
    """
    Calcula o maior prazo de compra (em dias) entre todos os materiais de um kit.
    Retorna o prazo máximo ou 0 se nenhum material tiver prazo definido.
    """
    conexao = conectar_mysql_leitura()
    if not conexao: return 0 # Retorna 0 em caso de falha na conexão

    prazo_maximo = 0
    try:
        cursor = conexao.cursor(dictionary=True)
        # Query para buscar a categoria de cada material no kit e o prazo dessa categoria
        query = """
            SELECT pc.prazo_dias
            FROM kit_materiais km
            JOIN materiais m ON km.material_id = m.id
            LEFT JOIN prazos_compra pc ON m.categoria = pc.categoria
            WHERE km.kit_id = %s
        """
        cursor.execute(query, (kit_id,))
        resultados = cursor.fetchall()

        # Encontra o maior prazo entre os materiais do kit
        for linha in resultados:
            if linha['prazo_dias'] is not None and linha['prazo_dias'] > prazo_maximo:
                prazo_maximo = linha['prazo_dias']
        
        return prazo_maximo

    except Error as e:
        print(f"Erro ao calcular prazo máximo do kit {kit_id}: {e}")
        return 0 # Retorna 0 em caso de erro
    finally:
        if conexao.is_connected():
            conexao.close()

@st.cache_data(ttl=3600)
def verificar_e_gerar_notificacoes_compra(obra_id, antecedencia_seguranca_dias=60):
    """
    Verifica tarefas futuras com kits vinculados e gera notificações de compra
    com base no prazo mais longo do kit e na antecedência de segurança.
    """
    conexao = obter_conexao_para_transacao()
    if not conexao: return

    try:
        cursor = conexao.cursor(dictionary=True)
        
        # Busca todos os vínculos de kits para tarefas futuras nesta obra
        query_vinculos = """
            SELECT 
                v.id as kit_vinculado_id, 
                v.kit_id, 
                t.data_inicio as data_necessidade
            FROM tarefa_kits_vinculados v
            JOIN planejamento_tarefas t ON v.tarefa_id = t.id
            WHERE t.obra_id = %s 
              AND t.data_inicio >= CURDATE()
        """
        cursor.execute(query_vinculos, (obra_id,))
        vinculos_futuros = cursor.fetchall()

        notificacoes_para_criar = []
        hoje = datetime.now().date()

        for vinculo in vinculos_futuros:
            kit_vinculado_id = vinculo['kit_vinculado_id']
            kit_id = vinculo['kit_id']
            data_necessidade = vinculo['data_necessidade']

            # Verifica se já existe notificação para este vínculo
            cursor.execute("SELECT id FROM notificacoes_compra WHERE kit_vinculado_id = %s", (kit_vinculado_id,))
            notificacao_existente = cursor.fetchone()

            if not notificacao_existente:
                # Calcula o prazo máximo para os materiais deste kit
                prazo_compra_kit = calcular_prazo_maximo_kit(kit_id)
                
                # Calcula o prazo total (compra + segurança)
                prazo_total_dias = prazo_compra_kit + antecedencia_seguranca_dias
                
                # Calcula a data limite para a notificação
                from datetime import timedelta
                data_notificacao = data_necessidade - timedelta(days=prazo_total_dias)

                # Se a data de notificação for hoje ou já passou, adiciona à lista
                if data_notificacao <= hoje:
                    notificacoes_para_criar.append((kit_vinculado_id, data_notificacao, data_necessidade))

        # Insere todas as novas notificações no banco de dados
        if notificacoes_para_criar:
            sql_insert = """
                INSERT INTO notificacoes_compra 
                (kit_vinculado_id, data_notificacao, data_necessidade) 
                VALUES (%s, %s, %s)
            """
            cursor.executemany(sql_insert, notificacoes_para_criar)
            conexao.commit()
            print(f"INFO: {len(notificacoes_para_criar)} nova(s) notificação(ões) de compra criada(s).")

    except Error as e:
        print(f"ERRO ao gerar notificações de compra: {e}")
        if conexao.is_connected(): conexao.rollback()
    finally:
        if conexao.is_connected(): conexao.close()

@st.cache_data(ttl=60)
def buscar_notificacoes_compra_db(obra_id):
    """Busca as notificações de COMPRA pendentes para uma obra."""
    conexao = conectar_mysql_leitura()
    if not conexao: return pd.DataFrame()
    query = """
        SELECT
            n.id,
            k.nome AS nome_kit,
            t.nome_tarefa,
            DATE_FORMAT(n.data_necessidade, '%%d/%%m/%%Y') AS data_necessidade_fmt,
            DATE_FORMAT(n.data_notificacao, '%%d/%%m/%%Y') AS data_notificacao_fmt,
            n.status
        FROM notificacoes_compra n
        JOIN tarefa_kits_vinculados v ON n.kit_vinculado_id = v.id
        JOIN kits k ON v.kit_id = k.id
        JOIN planejamento_tarefas t ON v.tarefa_id = t.id
        WHERE t.obra_id = %s AND n.status = 'Pendente'
        ORDER BY n.data_notificacao ASC
    """
    try:
        # Usamos %% para escapar o % para o pandas/mysql connector
        return pd.read_sql(query.replace('%%', '%'), conexao, params=(obra_id,))
    except Error as e:
        st.error(f"Erro ao buscar notificações de compra: {e}")
        return pd.DataFrame()

def atualizar_status_notificacao_compra_db(notificacao_id, novo_status):
    """Atualiza o status de uma notificação de compra."""
    conexao = obter_conexao_para_transacao()
    if not conexao: return False, "Falha na conexão."
    try:
        cursor = conexao.cursor()
        sql = "UPDATE notificacoes_compra SET status = %s WHERE id = %s"
        cursor.execute(sql, (novo_status, notificacao_id))
        conexao.commit()
        st.cache_data.clear() # Limpa cache de dados para a lista de notificações atualizar
        return True, "Status da notificação atualizado!"
    except Error as e:
        conexao.rollback()
        return False, f"Erro ao atualizar status da notificação: {e}"
    finally:
        if conexao.is_connected():
            conexao.close()

def buscar_notificacoes_compra_solicitadas_db(obra_id):
    """Busca o histórico de notificações de COMPRA marcadas como 'Solicitado'."""
    conexao = conectar_mysql_leitura()
    if not conexao: return pd.DataFrame()
    query = """
        SELECT
            k.nome AS nome_kit,
            t.nome_tarefa,
            DATE_FORMAT(n.data_necessidade, '%%d/%%m/%%Y') AS data_necessidade_fmt,
            DATE_FORMAT(n.data_notificacao, '%%d/%%m/%%Y') AS data_notificacao_fmt
            -- Poderíamos adicionar a data em que foi marcado como solicitado se tivéssemos essa coluna
        FROM notificacoes_compra n
        JOIN tarefa_kits_vinculados v ON n.kit_vinculado_id = v.id
        JOIN kits k ON v.kit_id = k.id
        JOIN planejamento_tarefas t ON v.tarefa_id = t.id
        WHERE t.obra_id = %s AND n.status = 'Solicitado'
        ORDER BY n.data_notificacao DESC -- Ordena pelas mais recentes primeiro
    """
    try:
        # Usamos %% para escapar o % para o pandas/mysql connector
        df = pd.read_sql(query.replace('%%', '%'), conexao, params=(obra_id,))
        # Renomeia colunas para clareza na exibição
        df.rename(columns={
            'nome_kit': 'Kit Solicitado',
            'nome_tarefa': 'Para Tarefa',
            'data_necessidade_fmt': 'Data Necessidade',
            'data_notificacao_fmt': 'Data Notificação'
        }, inplace=True)
        return df
    except Error as e:
        st.error(f"Erro ao buscar histórico de notificações de compra: {e}")
        return pd.DataFrame()
    finally:
        if conexao.is_connected():
            conexao.close()



#endregion

#region Funções de Lógica de Negócio (Banco de Dados - Transferências)
@st.cache_data(ttl=3600)
def buscar_transacoes_db(obra_id, tipo_busca='recebidas'):
    """Busca transações pendentes, recebidas ou enviadas por uma obra."""
    conexao = conectar_mysql_leitura()
    if not conexao: return pd.DataFrame()

    if tipo_busca == 'recebidas':
        clausula_where = "t.obra_destino_id = %s"
    else: # enviadas
        clausula_where = "t.obra_origem_id = %s"

    query = f"""
        SELECT 
            t.id,
            t.tipo_transacao,
            o_origem.nome_obra AS obra_origem,
            o_destino.nome_obra AS obra_destino,
            m.descricao AS material_descricao,
            m.unidade AS material_unidade,
            t.quantidade,
            DATE_FORMAT(t.data_solicitacao, '%d/%m/%Y %H:%i') as data_solicitacao
        FROM transacoes_pendentes t
        JOIN obras o_origem ON t.obra_origem_id = o_origem.id
        JOIN obras o_destino ON t.obra_destino_id = o_destino.id
        JOIN materiais m ON t.material_id = m.id
        WHERE {clausula_where} AND t.status = 'Pendente'
        ORDER BY t.data_solicitacao DESC;
    """
    try:
        return pd.read_sql(query, conexao, params=(obra_id,))
    except Error as e:
        st.error(f"Erro ao buscar transações {tipo_busca}: {e}")
        return pd.DataFrame()

def processar_transacao_db(transacao_id, novo_status, usuario_id):
    """
    Processa uma transação e, se aprovada, executa as movimentações de estoque
    na nova tabela estoque_obra.
    """
    conexao = obter_conexao_para_transacao()
    if not conexao: return False, "Falha na conexão."
    
    try:
        cursor = conexao.cursor(dictionary=True)
        conexao.start_transaction()

        # Busca os detalhes da transação pendente
        cursor.execute("SELECT * FROM transacoes_pendentes WHERE id = %s AND status = 'Pendente'", (transacao_id,))
        transacao = cursor.fetchone()
        if not transacao:
            return False, "Transação não encontrada ou já processada."

        if novo_status == 'Aprovada':
            # Pega as variáveis para facilitar a leitura
            material_id = transacao['material_id']
            obra_origem_id = transacao['obra_origem_id']
            obra_destino_id = transacao['obra_destino_id']
            quantidade = transacao['quantidade']

            # Pega o nome do material para o histórico
            cursor.execute("SELECT descricao FROM materiais WHERE id = %s", (material_id,))
            material = cursor.fetchone()
            if not material:
                raise Error("Material da transação não encontrado.")
            
            # 1. Registra as movimentações (esta parte continua igual)
            sql_saida = "INSERT INTO movimentacoes (obra_id, data, tipo, descricao, quantidade, recebedor, observacoes) VALUES (%s, NOW(), %s, %s, %s, %s, %s)"
            cursor.execute(sql_saida, (obra_origem_id, transacao['tipo_transacao'], material['descricao'], quantidade, f"Obra Destino ID: {obra_destino_id}", transacao['observacoes']))
            sql_entrada = "INSERT INTO movimentacoes (obra_id, data, tipo, descricao, quantidade, fornecedor, observacoes) VALUES (%s, NOW(), %s, %s, %s, %s, %s)"
            cursor.execute(sql_entrada, (obra_destino_id, transacao['tipo_transacao'], material['descricao'], quantidade, f"Obra Origem ID: {obra_origem_id}", transacao['observacoes']))

            # 2. ATUALIZA O ESTOQUE NA NOVA TABELA (A grande mudança!)
            # Primeiro, garante que as linhas de estoque existem para ambas as obras para evitar erros
            cursor.execute("INSERT INTO estoque_obra (material_id, obra_id, quantidade) VALUES (%s, %s, 0) ON DUPLICATE KEY UPDATE quantidade=quantidade", (material_id, obra_origem_id))
            cursor.execute("INSERT INTO estoque_obra (material_id, obra_id, quantidade) VALUES (%s, %s, 0) ON DUPLICATE KEY UPDATE quantidade=quantidade", (material_id, obra_destino_id))

            # Agora, executa a transferência de saldo de fato
            # Subtrai da origem
            cursor.execute("UPDATE estoque_obra SET quantidade = quantidade - %s WHERE material_id = %s AND obra_id = %s", (quantidade, material_id, obra_origem_id))
            # Adiciona ao destino
            cursor.execute("UPDATE estoque_obra SET quantidade = quantidade + %s WHERE material_id = %s AND obra_id = %s", (quantidade, material_id, obra_destino_id))

        # 3. Atualiza o status da transação (esta parte continua igual)
        sql_update_transacao = "UPDATE transacoes_pendentes SET status = %s, data_aprovacao_recusa = NOW(), processado_por_usuario_id = %s WHERE id = %s"
        cursor.execute(sql_update_transacao, (novo_status, usuario_id, transacao_id))

        conexao.commit()
        st.cache_resource.clear()
        st.cache_data.clear()
        return True, f"Transação '{novo_status.lower()}' com sucesso!"

    except Error as e:
        conexao.rollback()
        return False, f"Erro ao processar transação: {e}"
    finally:
        if conexao.is_connected():
            conexao.close()

def buscar_historico_transacoes_db(obra_id):
    """Busca o histórico de transações concluídas (aprovadas, recusadas, canceladas)."""
    conexao = conectar_mysql_leitura()
    if not conexao: return pd.DataFrame()
    query = """
        SELECT 
            t.tipo_transacao AS "Tipo",
            o_origem.nome_obra AS "Origem",
            o_destino.nome_obra AS "Destino",
            m.descricao AS "Material",
            t.quantidade AS "Qtd",
            t.status AS "Status",
            DATE_FORMAT(t.data_solicitacao, '%d/%m/%Y %H:%i') as "Data Solicitação",
            DATE_FORMAT(t.data_aprovacao_recusa, '%d/%m/%Y %H:%i') as "Data Resposta"
        FROM transacoes_pendentes t
        JOIN obras o_origem ON t.obra_origem_id = o_origem.id
        JOIN obras o_destino ON t.obra_destino_id = o_destino.id
        JOIN materiais m ON t.material_id = m.id
        WHERE (t.obra_origem_id = %s OR t.obra_destino_id = %s) AND t.status != 'Pendente'
        ORDER BY t.data_solicitacao DESC;
    """
    try:
        return pd.read_sql(query, conexao, params=(obra_id, obra_id))
    except Error as e:
        st.error(f"Erro ao buscar histórico de transações: {e}")
        return pd.DataFrame()

def buscar_transacoes_db(obra_id, tipo_busca='recebidas'):
    """Busca transações pendentes, recebidas ou enviadas por uma obra."""
    conexao = conectar_mysql_leitura()
    if not conexao: return pd.DataFrame()

    # Define a cláusula WHERE dinamicamente com base no tipo de busca
    if tipo_busca == 'recebidas':
        clausula_where = "t.obra_destino_id = %s"
    else: # tipo_busca == 'enviadas'
        clausula_where = "t.obra_origem_id = %s"

    # Query que junta as tabelas para trazer os nomes em vez de apenas IDs
    query = f"""
        SELECT 
            t.id,
            t.tipo_transacao,
            o_origem.nome_obra AS obra_origem,
            o_destino.nome_obra AS obra_destino,
            m.descricao AS material_descricao,
            m.unidade AS material_unidade,
            t.quantidade,
            DATE_FORMAT(t.data_solicitacao, '%d/%m/%Y %H:%i') as data_solicitacao
        FROM transacoes_pendentes t
        JOIN obras o_origem ON t.obra_origem_id = o_origem.id
        JOIN obras o_destino ON t.obra_destino_id = o_destino.id
        JOIN materiais m ON t.material_id = m.id
        WHERE {clausula_where} AND t.status = 'Pendente'
        ORDER BY t.data_solicitacao DESC;
    """
    try:
        # pd.read_sql executa a query de forma segura
        return pd.read_sql(query, conexao, params=(obra_id,))
    except Error as e:
        st.error(f"Erro ao buscar transações {tipo_busca}: {e}")
        return pd.DataFrame()

def processar_transacao_db(transacao_id, novo_status, usuario_id):
    """
    Processa uma transação e, se aprovada, executa as movimentações de estoque
    na nova tabela estoque_obra. VERSÃO COM DEBUG.
    """
    conexao = obter_conexao_para_transacao()
    if not conexao: return False, "Falha na conexão."
    
    try:
        cursor = conexao.cursor(dictionary=True)
        conexao.start_transaction()

        cursor.execute("SELECT * FROM transacoes_pendentes WHERE id = %s AND status = 'Pendente'", (transacao_id,))
        transacao = cursor.fetchone()
        if not transacao:
            return False, "Transação não encontrada ou já processada."

        if novo_status == 'Aprovada':
            material_id = transacao['material_id']
            obra_origem_id = transacao['obra_origem_id']
            obra_destino_id = transacao['obra_destino_id']
            quantidade = transacao['quantidade']

            cursor.execute("SELECT descricao FROM materiais WHERE id = %s", (material_id,))
            material = cursor.fetchone()
            if not material:
                raise Error("Material da transação não encontrado.")
            
            sql_saida = "INSERT INTO movimentacoes (obra_id, data, tipo, descricao, quantidade, recebedor, observacoes) VALUES (%s, NOW(), %s, %s, %s, %s, %s)"
            cursor.execute(sql_saida, (obra_origem_id, transacao['tipo_transacao'], material['descricao'], quantidade, f"Obra Destino ID: {obra_destino_id}", transacao['observacoes']))
            sql_entrada = "INSERT INTO movimentacoes (obra_id, data, tipo, descricao, quantidade, fornecedor, observacoes) VALUES (%s, NOW(), %s, %s, %s, %s, %s)"
            cursor.execute(sql_entrada, (obra_destino_id, transacao['tipo_transacao'], material['descricao'], quantidade, f"Obra Origem ID: {obra_origem_id}", transacao['observacoes']))

            # --- DEBUGGING LOGIC ---
            print(f"\n--- DEBUG: ATUALIZANDO ESTOQUE ---")
            print(f"Material ID: {material_id}, Qtd: {quantidade}")
            print(f"Obra Origem: {obra_origem_id}, Obra Destino: {obra_destino_id}")
            
            cursor.execute("INSERT INTO estoque_obra (material_id, obra_id, quantidade) VALUES (%s, %s, 0) ON DUPLICATE KEY UPDATE quantidade=quantidade", (material_id, obra_origem_id))
            cursor.execute("INSERT INTO estoque_obra (material_id, obra_id, quantidade) VALUES (%s, %s, 0) ON DUPLICATE KEY UPDATE quantidade=quantidade", (material_id, obra_destino_id))

            cursor.execute("UPDATE estoque_obra SET quantidade = quantidade - %s WHERE material_id = %s AND obra_id = %s", (quantidade, material_id, obra_origem_id))
            print(f"Executado UPDATE de SUBTRAÇÃO. Linhas afetadas: {cursor.rowcount}")

            cursor.execute("UPDATE estoque_obra SET quantidade = quantidade + %s WHERE material_id = %s AND obra_id = %s", (quantidade, material_id, obra_destino_id))
            print(f"Executado UPDATE de ADIÇÃO. Linhas afetadas: {cursor.rowcount}")
            print(f"--- FIM DEBUG ---\n")
            # --- END DEBUGGING LOGIC ---

        sql_update_transacao = "UPDATE transacoes_pendentes SET status = %s, data_aprovacao_recusa = NOW(), processado_por_usuario_id = %s WHERE id = %s"
        cursor.execute(sql_update_transacao, (novo_status, usuario_id, transacao_id))

        conexao.commit()
        st.cache_resource.clear()
        st.cache_data.clear()
        return True, f"Transação '{novo_status.lower()}' com sucesso!"

    except Error as e:
        conexao.rollback()
        return False, f"Erro ao processar transação: {e}"
    finally:
        if conexao.is_connected():
            conexao.close()

def buscar_historico_transacoes_db(obra_id):
    """Busca o histórico de transações concluídas (aprovadas, recusadas, canceladas)."""
    conexao = conectar_mysql_leitura()
    if not conexao: return pd.DataFrame()
    query = """
        SELECT 
            t.tipo_transacao AS "Tipo",
            o_origem.nome_obra AS "Origem",
            o_destino.nome_obra AS "Destino",
            m.descricao AS "Material",
            t.quantidade AS "Qtd",
            t.status AS "Status",
            DATE_FORMAT(t.data_solicitacao, '%d/%m/%Y %H:%i') as "Data Solicitação",
            DATE_FORMAT(t.data_aprovacao_recusa, '%d/%m/%Y %H:%i') as "Data Resposta"
        FROM transacoes_pendentes t
        JOIN obras o_origem ON t.obra_origem_id = o_origem.id
        JOIN obras o_destino ON t.obra_destino_id = o_destino.id
        JOIN materiais m ON t.material_id = m.id
        WHERE (t.obra_origem_id = %s OR t.obra_destino_id = %s) AND t.status != 'Pendente'
        ORDER BY t.data_solicitacao DESC;
    """
    try:
        return pd.read_sql(query, conexao, params=(obra_id, obra_id))
    except Error as e:
        st.error(f"Erro ao buscar histórico de transações: {e}")
        return pd.DataFrame()

def calcular_balanco_emprestimos_db(obra_id):
    """
    Calcula os saldos de materiais emprestados (dívidas) e a receber (créditos)
    para uma obra específica, tratando Empréstimos e Devoluções.
    """
    conexao = conectar_mysql_leitura()
    if not conexao:
        return pd.DataFrame(), pd.DataFrame()

    # --- Query para DÍVIDAS (O que EU devo para OUTRAS obras) ---
    query_dividas = """
    SELECT
        outra_obra.nome_obra AS "Devolver para a Obra",
        m.descricao AS "Material",
        SUM(CASE
            WHEN tp.tipo_transacao = 'Empréstimo' THEN tp.quantidade
            WHEN tp.tipo_transacao = 'Devolução' THEN -tp.quantidade
            ELSE 0
        END) AS "Quantidade Pendente"
    FROM transacoes_pendentes AS tp
    JOIN materiais AS m ON tp.material_id = m.id
    JOIN obras AS outra_obra ON tp.obra_origem_id = outra_obra.id
    WHERE
        tp.obra_destino_id = %s AND tp.status = 'Aprovada'
        AND tp.tipo_transacao IN ('Empréstimo', 'Devolução')
    GROUP BY
        outra_obra.nome_obra, m.descricao
    HAVING
        SUM(CASE
            WHEN tp.tipo_transacao = 'Empréstimo' THEN tp.quantidade
            WHEN tp.tipo_transacao = 'Devolução' THEN -tp.quantidade
            ELSE 0
        END) > 0;
    """

    # --- Query para CRÉDITOS (O que OUTRAS obras me devem) ---
    query_creditos = """
    SELECT
        outra_obra.nome_obra AS "Receber da Obra",
        m.descricao AS "Material",
        SUM(CASE
            WHEN tp.tipo_transacao = 'Empréstimo' THEN tp.quantidade
            WHEN tp.tipo_transacao = 'Devolução' THEN -tp.quantidade
            ELSE 0
        END) AS "Quantidade Pendente"
    FROM transacoes_pendentes AS tp
    JOIN materiais AS m ON tp.material_id = m.id
    JOIN obras AS outra_obra ON tp.obra_destino_id = outra_obra.id
    WHERE
        tp.obra_origem_id = %s AND tp.status = 'Aprovada'
        AND tp.tipo_transacao IN ('Empréstimo', 'Devolução')
    GROUP BY
        outra_obra.nome_obra, m.descricao
    HAVING
        SUM(CASE
            WHEN tp.tipo_transacao = 'Empréstimo' THEN tp.quantidade
            WHEN tp.tipo_transacao = 'Devolução' THEN -tp.quantidade
            ELSE 0
        END) > 0;
    """
    
    try:
        df_dividas = pd.read_sql(query_dividas, conexao, params=(obra_id,))
        df_creditos = pd.read_sql(query_creditos, conexao, params=(obra_id,))
        return df_dividas, df_creditos
    except Error as e:
        st.error(f"Erro ao calcular balanço de transações: {e}")
        return pd.DataFrame(), pd.DataFrame()



#endregion

#region Funções de Lógica de Negócio (Banco de Dados - Kits)

@st.cache_data(ttl=60)
def buscar_kits_da_obra_db(obra_id):
    """Busca todos os kits de uma obra específica."""
    conexao = conectar_mysql_leitura()
    if not conexao: return pd.DataFrame()
    query = "SELECT id, nome, descricao FROM kits WHERE obra_id = %s ORDER BY nome ASC"
    try:
        return pd.read_sql(query, conexao, params=(obra_id,))
    except Error as e:
        st.error(f"Erro ao buscar kits: {e}")
        return pd.DataFrame()

def buscar_materiais_de_um_kit_db(kit_id):
    """Busca os materiais de um kit específico (não precisa de obra_id pois kit_id é único)."""
    conexao = conectar_mysql_leitura()
    if not conexao: return pd.DataFrame()
    query = """
        SELECT m.descricao, km.quantidade, m.unidade
        FROM kit_materiais km
        JOIN materiais m ON km.material_id = m.id
        WHERE km.kit_id = %s
    """
    try:
        return pd.read_sql(query, conexao, params=(kit_id,))
    except Error as e:
        st.error(f"Erro ao buscar materiais do kit: {e}")
        return pd.DataFrame()

def salvar_kit_completo_db(obra_id, nome, descricao, lista_materiais):
    """Salva um novo kit e sua lista de materiais em uma única transação."""
    conexao = obter_conexao_para_transacao()
    if not conexao: return False, "Falha na conexão."
    try:
        cursor = conexao.cursor()
        conexao.start_transaction()
        # Insere o kit e obtém o ID
        sql_kit = "INSERT INTO kits (obra_id, nome, descricao) VALUES (%s, %s, %s)"
        cursor.execute(sql_kit, (obra_id, nome, descricao))
        kit_id = cursor.lastrowid
        # Insere os materiais vinculados ao kit
        sql_materiais = "INSERT INTO kit_materiais (kit_id, material_id, quantidade) VALUES (%s, %s, %s)"
        for material in lista_materiais:
            cursor.execute(sql_materiais, (kit_id, material['id'], material['quantidade']))
        conexao.commit()
        
        st.cache_resource.clear()
        st.cache_data.clear()
        
        return True, f"Kit '{nome}' salvo com sucesso!"
    except Error as e:
        conexao.rollback()
        return False, f"Erro ao salvar o kit: {e}"
    finally:
        if conexao.is_connected(): conexao.close()

def atualizar_kit_db(kit_id, nome, descricao, lista_materiais):
    """Atualiza um kit existente e sua lista de materiais."""
    conexao = obter_conexao_para_transacao()
    if not conexao: return False, "Falha na conexão."
    try:
        cursor = conexao.cursor()
        conexao.start_transaction()
        cursor.execute("UPDATE kits SET nome = %s, descricao = %s WHERE id = %s", (nome, descricao, kit_id))
        cursor.execute("DELETE FROM kit_materiais WHERE kit_id = %s", (kit_id,))
        sql_materiais = "INSERT INTO kit_materiais (kit_id, material_id, quantidade) VALUES (%s, %s, %s)"
        if lista_materiais:
            for material in lista_materiais:
                cursor.execute(sql_materiais, (kit_id, material['id'], material['quantidade']))
        conexao.commit()

        st.cache_resource.clear()
        st.cache_data.clear()
        return True, f"Kit '{nome}' atualizado com sucesso!"
    except Error as e:
        conexao.rollback()
        return False, f"Erro ao atualizar o kit: {e}"
    finally:
        if conexao.is_connected(): conexao.close()

def excluir_kit_db(kit_id):
    """Exclui um kit. A exclusão dos materiais é em cascata (ON DELETE CASCADE)."""
    conexao = obter_conexao_para_transacao()
    if not conexao: return False, "Falha na conexão."
    try:
        cursor = conexao.cursor()
        cursor.execute("DELETE FROM kits WHERE id = %s", (kit_id,))
        conexao.commit()

        st.cache_resource.clear()
        st.cache_data.clear()

        return True, "Kit excluído com sucesso."
    except Error as e:
        conexao.rollback()
        return False, f"Erro ao excluir o kit: {e}"
    finally:
        if conexao.is_connected(): conexao.close()

#endregion

#region Funções de Lógica de Negócio (Banco de Dados - PLANEJAMENTO)

def ler_e_salvar_tarefas_excel(uploaded_file, obra_id):
    """
    Lê um arquivo .xlsx, valida a presença da coluna 'Id_exclusiva',
    e salva as tarefas no banco de dados.
    """
    try:
        # --- 1. LEITURA E VALIDAÇÃO DO ARQUIVO EXCEL ---
        df_excel = pd.read_excel(uploaded_file, engine='openpyxl')

        # Verificação inteligente que prioriza "Id_exclusiva"
        unique_id_col = None
        if 'Id_exclusiva' in df_excel.columns:
            unique_id_col = 'Id_exclusiva'
        elif 'ID Exclusivo' in df_excel.columns:
            unique_id_col = 'ID Exclusivo'
        elif 'Unique ID' in df_excel.columns:
            unique_id_col = 'Unique ID'

        if unique_id_col is None:
            mensagem_erro = """
            **ERRO: A coluna 'Id_exclusiva' não foi encontrada no arquivo Excel.**

            Para corrigir no MS Project:
            1.  Clique com o botão direito no cabeçalho de qualquer coluna.
            2.  Selecione **'Inserir Coluna'**.
            3.  Escolha **'ID Exclusivo'** (exporta como 'Id_exclusiva').
            4.  Exporte o arquivo para Excel novamente.
            """
            return False, mensagem_erro

        colunas_necessarias = [unique_id_col, 'Nome', 'Início', 'Término']
        if not all(coluna in df_excel.columns for coluna in colunas_necessarias):
            return False, f"O arquivo Excel precisa conter as colunas: {', '.join(colunas_necessarias)}"
        
        # --- 2. LIMPEZA E PREPARAÇÃO DOS DADOS DO EXCEL ---
        df_excel = df_excel[colunas_necessarias].copy()
        df_excel.dropna(subset=[unique_id_col, 'Nome', 'Início'], inplace=True)
        df_excel.rename(columns={
            unique_id_col: 'unique_id_mpp',
            'Nome': 'nome_tarefa',
            'Início': 'data_inicio',
            'Término': 'data_fim'
        }, inplace=True)

        # --- 3. TRATAMENTO DE DATAS (CORRIGIDO) ---
        # Adicionamos dayfirst=True e format='mixed' para evitar erros de inferência
        df_excel['data_inicio'] = pd.to_datetime(
            df_excel['data_inicio'].astype(str).apply(traduzir_data_pt_en), 
            dayfirst=True, 
            format='mixed', 
            errors='coerce'
        ).dt.date
        
        df_excel['data_fim'] = pd.to_datetime(
            df_excel['data_fim'].astype(str).apply(traduzir_data_pt_en), 
            dayfirst=True, 
            format='mixed', 
            errors='coerce'
        ).dt.date

        # --- 4. GARANTIR CONSISTÊNCIA DE TIPO PARA O MERGE ---
        df_db = buscar_tarefas_para_comparacao(obra_id)
        df_db['unique_id_mpp'] = df_db['unique_id_mpp'].astype(str)
        df_excel['unique_id_mpp'] = df_excel['unique_id_mpp'].astype(str)

        # --- 5. COMPARAÇÃO COM OS DADOS DO BANCO ---
        df_merged = pd.merge(df_db, df_excel, on='unique_id_mpp', how='outer', suffixes=('_db', '_excel'), indicator=True)

        novas_tarefas = df_merged[df_merged['_merge'] == 'right_only']
        tarefas_removidas = df_merged[df_merged['_merge'] == 'left_only']
        tarefas_existentes = df_merged[df_merged['_merge'] == 'both']
        
        tarefas_modificadas = tarefas_existentes[
            (tarefas_existentes['data_inicio_db'] != tarefas_existentes['data_inicio_excel']) |
            (tarefas_existentes['nome_tarefa_db'] != tarefas_existentes['nome_tarefa_excel'])
        ].copy()

        # --- 6. EXECUÇÃO DAS OPERAÇÕES NO BANCO DE DADOS ---
        conexao = obter_conexao_para_transacao()
        if not conexao: return False, "Falha na conexão."
        cursor = conexao.cursor()
        conexao.start_transaction()

        if not tarefas_removidas.empty:
            ids_para_remover = tuple(tarefas_removidas['id'].astype(int).tolist())
            if len(ids_para_remover) == 1:
                cursor.execute("DELETE FROM planejamento_tarefas WHERE id = %s", (ids_para_remover[0],))
            else:
                cursor.execute(f"DELETE FROM planejamento_tarefas WHERE id IN ({','.join(['%s']*len(ids_para_remover))})", ids_para_remover)

        if not tarefas_modificadas.empty:
            updates = []
            for _, row in tarefas_modificadas.iterrows():
                updates.append((row['nome_tarefa_excel'], row['data_inicio_excel'], row['data_fim_excel'], int(row['id'])))
            cursor.executemany("UPDATE planejamento_tarefas SET nome_tarefa = %s, data_inicio = %s, data_fim = %s WHERE id = %s", updates)
        
        if not novas_tarefas.empty:
            inserts = []
            for _, row in novas_tarefas.iterrows():
                inserts.append((obra_id, int(row['unique_id_mpp']), row['nome_tarefa_excel'], row['data_inicio_excel'], row['data_fim_excel']))
            cursor.executemany("INSERT INTO planejamento_tarefas (obra_id, unique_id_mpp, nome_tarefa, data_inicio, data_fim) VALUES (%s, %s, %s, %s, %s)", inserts)
            
        conexao.commit()
        st.cache_resource.clear()
        st.cache_data.clear()

        relatorio = {
            "adicionadas": novas_tarefas['nome_tarefa_excel'].tolist(),
            "removidas": tarefas_removidas['nome_tarefa_db'].tolist(),
            "modificadas": tarefas_modificadas['nome_tarefa_db'].tolist()
        }
        return True, relatorio

    except Exception as e:
        if 'conexao' in locals() and conexao.is_connected(): conexao.rollback()
        return False, f"Ocorreu um erro ao processar o arquivo: {e}"
    finally:
        if 'conexao' in locals() and conexao.is_connected(): conexao.close()

@st.cache_data(ttl=60)
def buscar_tarefas_db(obra_id):
    """Busca todas as tarefas de um planejamento para uma obra específica."""
    conexao = conectar_mysql_leitura()
    if not conexao: return pd.DataFrame()
    query = "SELECT id, nome_tarefa, DATE_FORMAT(data_inicio, '%d/%m/%Y') as data_inicio_fmt FROM planejamento_tarefas WHERE obra_id = %s ORDER BY data_inicio"
    try:
        return pd.read_sql(query, conexao, params=(obra_id,))
    except Error:
        return pd.DataFrame()

def buscar_tarefas_para_comparacao(obra_id):
    """Busca as tarefas de uma obra para a lógica de comparação de planejamento."""
    conexao = conectar_mysql_leitura()
    if not conexao: return pd.DataFrame()
    query = """
        SELECT id, unique_id_mpp, nome_tarefa, data_inicio, data_fim 
        FROM planejamento_tarefas 
        WHERE obra_id = %s
    """
    try:
        df = pd.read_sql(query, conexao, params=(obra_id,))
        # Garante que as colunas de data estão no formato correto para comparação
        df['data_inicio'] = pd.to_datetime(df['data_inicio']).dt.date
        df['data_fim'] = pd.to_datetime(df['data_fim'], errors='coerce').dt.date
        return df
    except Error:
        return pd.DataFrame()

@st.cache_data(ttl=60)
def buscar_kits_vinculados_db(tarefa_id):
    """Busca os kits que já foram vinculados a uma tarefa específica."""
    conexao = conectar_mysql_leitura()
    if not conexao: return pd.DataFrame()
    query = """
        SELECT v.id, k.nome, v.quantidade_kits
        FROM tarefa_kits_vinculados v
        JOIN kits k ON v.kit_id = k.id
        WHERE v.tarefa_id = %s
    """
    try:
        return pd.read_sql(query, conexao, params=(tarefa_id,))
    except Error:
        return pd.DataFrame()

def vincular_kit_a_tarefa_db(tarefa_id, kit_id, quantidade):
    """Salva um novo vínculo entre uma tarefa e um kit."""
    conexao = obter_conexao_para_transacao()
    if not conexao: return False, "Falha na conexão."
    try:
        cursor = conexao.cursor()
        # Evita duplicatas: só insere se a combinação tarefa-kit não existir
        sql = "INSERT INTO tarefa_kits_vinculados (tarefa_id, kit_id, quantidade_kits) SELECT %s, %s, %s WHERE NOT EXISTS (SELECT 1 FROM tarefa_kits_vinculados WHERE tarefa_id = %s AND kit_id = %s)"
        cursor.execute(sql, (tarefa_id, kit_id, quantidade, tarefa_id, kit_id))
        conexao.commit()
        st.cache_resource.clear()
        st.cache_data.clear()
        if cursor.rowcount > 0:
            return True, "Kit vinculado com sucesso!"
        else:
            return False, "Este kit já foi vinculado a esta tarefa."
    except Error as e:
        conexao.rollback()
        return False, f"Erro ao vincular kit: {e}"
    finally:
        if conexao.is_connected(): conexao.close()

def desvincular_kit_da_tarefa_db(vinculo_id):
    """Remove um vínculo entre tarefa e kit pelo ID do vínculo."""
    conexao = obter_conexao_para_transacao()
    if not conexao: return False, "Falha na conexão."
    try:
        cursor = conexao.cursor()
        sql = "DELETE FROM tarefa_kits_vinculados WHERE id = %s"
        cursor.execute(sql, (vinculo_id,))
        conexao.commit()
        st.cache_resource.clear()
        st.cache_data.clear()
        return True, "Vínculo removido com sucesso."
    except Error as e:
        conexao.rollback()
        return False, f"Erro ao remover vínculo: {e}"
    finally:
        if conexao.is_connected(): conexao.close()

@st.cache_data(ttl=3600)
def verificar_e_gerar_solicitacoes_db(obra_id, dias_antecedencia=2):
    """
    Verifica as tarefas futuras e cria solicitações de montagem se ainda não existirem.
    Esta função atua como nosso "agendador".
    """
    conexao = obter_conexao_para_transacao()
    if not conexao: return

    try:
        cursor = conexao.cursor(dictionary=True)
        
        # Calcula a data limite para a solicitação
        from datetime import date, timedelta
        data_limite = date.today() + timedelta(days=dias_antecedencia)

        # Query para encontrar todos os kits vinculados a tarefas que começarão em breve
        # e que AINDA NÃO têm uma solicitação de montagem criada.
        query = """
            SELECT v.id, t.data_inicio
            FROM tarefa_kits_vinculados v
            JOIN planejamento_tarefas t ON v.tarefa_id = t.id
            LEFT JOIN solicitacoes_montagem s ON v.id = s.kit_vinculado_id
            WHERE t.obra_id = %s
              AND t.data_inicio <= %s
              AND s.id IS NULL
        """
        cursor.execute(query, (obra_id, data_limite))
        vinculos_para_solicitar = cursor.fetchall()

        # Se encontrou kits que precisam de solicitação, cria-os
        if vinculos_para_solicitar:
            sql_insert = "INSERT INTO solicitacoes_montagem (kit_vinculado_id, data_execucao_prevista) VALUES (%s, %s)"
            valores_para_inserir = [(vinculo['id'], vinculo['data_inicio']) for vinculo in vinculos_para_solicitar]
            cursor.executemany(sql_insert, valores_para_inserir)
            conexao.commit()
            print(f"INFO: {len(valores_para_inserir)} nova(s) solicitação(ões) de montagem criada(s).")

    except Error as e:
        print(f"ERRO ao gerar solicitações de montagem: {e}")
        conexao.rollback()
    finally:
        if conexao.is_connected():
            conexao.close()

@st.cache_data(ttl=3600)
def buscar_solicitacoes_montagem_db(obra_id):
    """Busca as solicitações de montagem de kit que estão pendentes ou em andamento para uma obra."""
    conexao = conectar_mysql_leitura()
    if not conexao: return pd.DataFrame()
    query = """
        SELECT
            s.id,
            k.nome AS nome_kit,
            v.quantidade_kits,
            DATE_FORMAT(s.data_execucao_prevista, '%d/%m/%Y') AS data_execucao,
            s.status
        FROM solicitacoes_montagem s
        JOIN tarefa_kits_vinculados v ON s.kit_vinculado_id = v.id
        JOIN kits k ON v.kit_id = k.id
        JOIN planejamento_tarefas t ON v.tarefa_id = t.id
        WHERE t.obra_id = %s AND s.status IN ('Pendente', 'Montado')
        ORDER BY s.data_execucao_prevista ASC
    """
    try:
        return pd.read_sql(query, conexao, params=(obra_id,))
    except Error as e:
        st.error(f"Erro ao buscar solicitações de montagem: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def atualizar_status_solicitacao_db(solicitacao_id, novo_status):
    """Atualiza o status de uma solicitação de montagem."""
    conexao = obter_conexao_para_transacao()
    if not conexao: return False, "Falha na conexão."
    try:
        cursor = conexao.cursor()
        sql = "UPDATE solicitacoes_montagem SET status = %s WHERE id = %s"
        cursor.execute(sql, (novo_status, solicitacao_id))
        conexao.commit()
        st.cache_resource.clear()
        st.cache_data.clear()
        return True, "Status atualizado com sucesso!"
    except Error as e:
        conexao.rollback()
        return False, f"Erro ao atualizar status: {e}"
    finally:
        if conexao.is_connected():
            conexao.close()

def vincular_kit_a_multiplas_tarefas_db(lista_tarefa_ids, kit_id, quantidade):
    """
    Vincula um kit específico a uma lista de tarefas, evitando duplicatas.
    Retorna a contagem de sucessos e uma lista de mensagens (sucesso ou aviso).
    """
    conexao = obter_conexao_para_transacao()
    if not conexao:
        return 0, ["Falha na conexão."]
        
    sucessos = 0
    mensagens = []
    
    try:
        cursor = conexao.cursor()
        sql = """
            INSERT INTO tarefa_kits_vinculados 
                (tarefa_id, kit_id, quantidade_kits) 
            SELECT %s, %s, %s 
            WHERE NOT EXISTS (
                SELECT 1 FROM tarefa_kits_vinculados 
                WHERE tarefa_id = %s AND kit_id = %s
            )
        """
        
        # Itera sobre cada ID de tarefa fornecido
        for tarefa_id in lista_tarefa_ids:
            try:
                cursor.execute(sql, (tarefa_id, kit_id, quantidade, tarefa_id, kit_id))
                if cursor.rowcount > 0:
                    sucessos += 1
                else:
                    # Se rowcount for 0, significa que o vínculo já existia
                    mensagens.append(f"Tarefa ID {tarefa_id}: Kit já estava vinculado.")
            except Error as e:
                # Captura erros por tarefa, mas continua o processo
                mensagens.append(f"Tarefa ID {tarefa_id}: Erro - {e}")
                conexao.rollback() # Desfaz a tentativa atual, mas continua o loop
                conexao.start_transaction() # Reinicia a transação para a próxima tarefa

        conexao.commit()
        
        # Limpa os caches apenas se houve alguma alteração bem-sucedida
        if sucessos > 0:
            st.cache_resource.clear()
            st.cache_data.clear()
            
        if sucessos > 0:
             mensagens.insert(0, f"{sucessos} kit(s) vinculados com sucesso!")

        return sucessos, mensagens

    except Error as e:
        conexao.rollback()
        return 0, [f"Erro geral ao vincular kits em lote: {e}"]
    finally:
        if conexao.is_connected():
            conexao.close()

#endregion

#region Funções de Callback e Renderização de Páginas
def handle_material_selection(df_materiais):
    if 'editor_materiais' in st.session_state:
        edited_rows = st.session_state.editor_materiais.get("edited_rows", {})
        ids_ja_selecionados = set(st.session_state.get('itens_selecionados_ids', []))
        ids_desmarcados_na_pagina = {int(df_materiais.iloc[idx]['id']) for idx, row in edited_rows.items() if not row.get("Selecionar", True)}
        ids_marcados_na_pagina = {int(df_materiais.iloc[idx]['id']) for idx, row in edited_rows.items() if row.get("Selecionar", False)}
        resultado_final = (ids_ja_selecionados - ids_desmarcados_na_pagina) | ids_marcados_na_pagina
        st.session_state.itens_selecionados_ids = sorted(list(resultado_final))

def close_details_panel():
    st.session_state.itens_selecionados_ids = []
    st.session_state.confirmando_exclusao = False
    

def render_login_page():
    col1, col2 = st.columns([1, 1])
    
    with col1:
        
        st.markdown("<h1 style='text-align: left;'>System Plugin: Gestão de Almoxarifado</h1>", unsafe_allow_html=True)
            
        tab1, tab2 = st.tabs(["Entrar", "Cadastrar"])
        with tab1:
            st.subheader("Por favor, faça o login para continuar")
            with st.form("login_form"):
                username = st.text_input("Nome de Usuário")
                password = st.text_input("Senha", type="password")
                if st.form_submit_button("Entrar", use_container_width=True, type="primary"):
                    if not username or not password:
                        st.warning("Por favor, preencha todos os campos.")
                    else:
                        user_data = login_user(username, password)
                        if user_data:
                            st.session_state.usuario_logado = True
                            st.session_state.usuario_id = user_data['id']
                            st.session_state.usuario_nome = user_data['nome_usuario']
                            st.success("Login bem-sucedido!"); st.rerun()
                        else:
                            st.error("Nome de usuário ou senha incorretos.")
        with tab2:
            st.subheader("Crie uma nova conta")
            with st.form("register_form"):
                new_username = st.text_input("Nome de Usuário*", key="reg_user")
                new_email = st.text_input("Email*", key="reg_email")
                new_password = st.text_input("Senha*", type="password", key="reg_pass")
                confirm_password = st.text_input("Confirme a Senha*", type="password", key="reg_pass_confirm")
                if st.form_submit_button("Cadastrar", use_container_width=True):
                    if not all([new_username, new_email, new_password, confirm_password]):
                        st.warning("Por favor, preencha todos os campos obrigatórios (*).")
                    elif new_password != confirm_password:
                        st.error("As senhas não coincidem.")
                    elif len(new_password) < 6:
                        st.error("A senha deve ter pelo menos 6 caracteres.")
                    else:
                        sucesso, mensagem = registrar_usuario_db(new_username, new_email, hash_password(new_password))
                        if sucesso: st.success(mensagem)
                        else: st.error(mensagem)
    with col2:
        st.image("assets/login_page/image4.png", use_container_width=True)

def render_estoque_materiais_page():
    # --- Inicialização de Estado da Página ---
    if "pagina_materiais" not in st.session_state:
        st.session_state.pagina_materiais = 'listar'
    if "material_para_editar_id" not in st.session_state:
        st.session_state.material_para_editar_id = None
    if "filtros_materiais" not in st.session_state:
        st.session_state.filtros_materiais = {"nome": "", "categoria": "Todas"}
    if "pagina_atual_materiais" not in st.session_state:
        st.session_state.pagina_atual_materiais = 1
    if "ver_todos_materiais" not in st.session_state:
        st.session_state.ver_todos_materiais = False
    if 'itens_selecionados_ids' not in st.session_state:
        st.session_state.itens_selecionados_ids = []
    if 'confirmando_exclusao' not in st.session_state:
        st.session_state.confirmando_exclusao = False
    if 'upload_processado' not in st.session_state:
        st.session_state.upload_processado = False

    # --- Listas Dinâmicas ---
    UNIDADES_VALIDAS = buscar_unidades_unicas()
    CATEGORIAS_VALIDAS = buscar_categorias_unicas()

    # --- Funções de Navegação Interna ---
    def ir_para_cadastro():
        st.session_state.pagina_materiais = 'cadastrar'
        close_details_panel()

    def ir_para_edicao(material_id):
        st.session_state.material_para_editar_id = material_id
        st.session_state.pagina_materiais = 'editar'
        close_details_panel()

    def ir_para_lista():
        st.session_state.pagina_materiais = 'listar'
        st.session_state.material_para_editar_id = None
    
    def on_file_change():
        st.session_state.upload_processado = False

    # --- Roteador de Páginas ---

    if st.session_state.pagina_materiais == 'listar':
        if st.session_state.itens_selecionados_ids:
            main_col, detail_col = st.columns([3, 1])
        else:
            main_col = st.container()
        
        # Linha Nova
        df_pagina, total_itens = buscar_materiais_paginados(
            obra_id=st.session_state.obra_selecionada_id,  # <-- ADICIONADO AQUI
            filtros=st.session_state.filtros_materiais,
            pagina=st.session_state.pagina_atual_materiais,
            ver_todos=st.session_state.ver_todos_materiais
        )

        with main_col:
            st.title("Estoque de Materiais")
            st.markdown("---")
            st.write("#### Ferramentas de Busca")
            
            colf1, colf2, colf3 = st.columns([3, 1, 1])
            with colf1:
                filtro_nome = st.text_input("Pesquisar por descrição", value=st.session_state.filtros_materiais['nome'], key="filtro_nome_materiais")
            with colf2:
                filtro_categoria = st.selectbox("Filtrar por Categoria", options=["Todas"] + CATEGORIAS_VALIDAS, key="filtro_cat_materiais")
            with colf3:
                st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
                if st.button("🔎 Buscar"):
                    st.session_state.filtros_materiais['nome'] = filtro_nome; st.session_state.filtros_materiais['categoria'] = filtro_categoria
                    st.session_state.pagina_atual_materiais = 1; st.session_state.ver_todos_materiais = False
                    close_details_panel(); st.rerun()
            st.markdown("---")
            
            if total_itens > 0:
                itens_por_pagina = 20
                total_paginas = (total_itens + itens_por_pagina - 1) // itens_por_pagina
                if not st.session_state.ver_todos_materiais:
                    st.write(f"Exibindo **{len(df_pagina)}** de **{total_itens}** materiais. Página **{st.session_state.pagina_atual_materiais}** de **{total_paginas}**.")
                else:
                    st.write(f"Exibindo todos os **{total_itens}** materiais encontrados.")
                col_nav_buttons = st.columns([2, 2, 3, 3, 3])
                if col_nav_buttons[0].button("⬅️ Anterior", disabled=(st.session_state.pagina_atual_materiais == 1 or st.session_state.ver_todos_materiais), use_container_width=True):
                    st.session_state.pagina_atual_materiais -= 1; close_details_panel(); st.rerun()
                if col_nav_buttons[1].button("Próxima ➡️", disabled=(st.session_state.pagina_atual_materiais >= total_paginas or st.session_state.ver_todos_materiais), use_container_width=True):
                    st.session_state.pagina_atual_materiais += 1; close_details_panel(); st.rerun()
                if not st.session_state.ver_todos_materiais:
                    if col_nav_buttons[2].button("Ver todos os resultados", key="ver_todos_btn", use_container_width=True):
                        st.session_state.ver_todos_materiais = True; close_details_panel(); st.rerun()
                else:
                    if col_nav_buttons[2].button("Ver de forma paginada", use_container_width=True):
                        st.session_state.ver_todos_materiais = False; close_details_panel(); st.rerun()
                if col_nav_buttons[3].button("🔄 Atualizar Lista", use_container_width=True):
                    st.session_state.filtros_materiais = {"nome": "", "categoria": "Todas"}; st.session_state.ver_todos_materiais = False; st.session_state.pagina_atual_materiais = 1; close_details_panel(); st.rerun()
                col_nav_buttons[4].button("➕ Novo Material", on_click=ir_para_cadastro, use_container_width=True)
                st.markdown("---")
                df_display = df_pagina.copy()
                df_display['Selecionar'] = df_display['id'].isin(st.session_state.itens_selecionados_ids)
                df_display = df_display[['Selecionar', 'codigo', 'descricao', 'estoque_atual', 'unidade', 'categoria', 'id']]
                altura_tabela = min((len(df_display.index) + 1) * 35 + 3, 600)
                st.data_editor(df_display, height=altura_tabela, key="editor_materiais", on_change=handle_material_selection, args=(df_pagina,), column_config={ "Selecionar": st.column_config.CheckboxColumn(), "estoque_atual": st.column_config.NumberColumn("Estoque Atual", format="%.2f"), "id": None, "codigo": "Código", "descricao": "Descrição", "unidade": "Un."}, hide_index=True, disabled=['codigo', 'descricao', 'estoque_atual', 'unidade', 'categoria'])
            else:
                st.info("Nenhum material encontrado.")
                if st.button("➕ Novo Material", on_click=ir_para_cadastro): pass
        if st.session_state.itens_selecionados_ids:
            with detail_col:
                num_selecionados = len(st.session_state.itens_selecionados_ids)
                if num_selecionados == 1:
                    material_id = st.session_state.itens_selecionados_ids[0]
                    material_details = buscar_material_por_id(material_id)
                    if material_details:
                        st.subheader(material_details['descricao'])
                        st.markdown("---")
                        data_cad_str = pd.to_datetime(material_details.get('data_cadastro')).strftime('%d/%m/%Y às %H:%M') if pd.notna(material_details.get('data_cadastro')) else "N/D"
                        st.markdown(f"""- **Código:** `{material_details.get('codigo', 'N/D')}`\n- **Cadastrado em:** `{data_cad_str}`\n- **Estoque Mínimo:** `{material_details.get('estoque_minimo') or 'Não definido'}`\n- **Estoque Máximo:** `{material_details.get('estoque_maximo') or 'Não definido'}`""")
                        st.text_area("Observações", value=material_details.get('observacoes') or 'Sem observações.', height=100, disabled=True, label_visibility="collapsed")
                        st.markdown("---")
                        if st.button("✏️ Editar Material", use_container_width=True, on_click=ir_para_edicao, args=(material_id,)): pass
                        if st.button("🗑️ Excluir Material", use_container_width=True): st.session_state.confirmando_exclusao = True; st.rerun()
                elif num_selecionados > 1:
                    st.subheader(f"{num_selecionados} Materiais Selecionados")
                    st.markdown("---")
                    nomes_materiais = df_pagina[df_pagina['id'].isin(st.session_state.itens_selecionados_ids)]['descricao'].tolist()
                    for nome in nomes_materiais: st.write(f"- {nome}")
                    st.markdown("---")
                    if st.button(f"🗑️ Excluir {num_selecionados} Materiais", use_container_width=True): st.session_state.confirmando_exclusao = True; st.rerun()
                if st.session_state.get('confirmando_exclusao'):
                    st.warning("**Você tem certeza que deseja excluir o(s) item(ns) selecionado(s)?**"); st.caption("Esta ação não pode ser desfeita.")
                    c1, c2 = st.columns(2)
                    if c1.button("Sim, excluir", use_container_width=True, type="primary"):
                        sucesso, msg = excluir_materiais_db(st.session_state.itens_selecionados_ids)
                        if sucesso: st.success(msg)
                        else: st.error(msg)
                        close_details_panel(); st.rerun()
                    if c2.button("Cancelar", use_container_width=True): st.session_state.confirmando_exclusao = False; st.rerun()
                st.markdown("---")
                st.button("✖️ Fechar Detalhes", use_container_width=True, on_click=close_details_panel)

    elif st.session_state.pagina_materiais == 'cadastrar':
        st.title("Cadastrar Novo Material")
        tab_manual, tab_lote = st.tabs(["Cadastro Manual", "Cadastro em Lote via Planilha"])
        with tab_manual:
            with st.form("form_cadastro_material"):
                nome = st.text_input("Nome do Material*")
                codigo = st.text_input("Código do Material*")
                unidade = st.selectbox("Unidade de Medida*", [""] + UNIDADES_VALIDAS, index=0)
                categoria = st.selectbox("Categoria*", [""] + CATEGORIAS_VALIDAS, index=0)
                estoque_minimo = st.number_input("Estoque Mínimo", value=None, placeholder="0.00", format="%.4f")
                estoque_maximo = st.number_input("Estoque Máximo", value=None, placeholder="0.00", format="%.4f")
                observacoes = st.text_area("Observações")
                submitted = st.form_submit_button("Cadastrar Material")
                if submitted:
                    if not nome or not codigo or not unidade or not categoria:
                        st.warning("Nome, Código, Unidade e Categoria são campos obrigatórios.")
                    else:
                        sucesso, msg = cadastrar_material_db(codigo, nome, unidade, categoria, estoque_minimo, estoque_maximo, observacoes)
                        if sucesso: st.success(msg); st.balloons()
                        else: st.error(msg)
        with tab_lote:
            st.info("Faça o download do modelo, preencha e envie o arquivo para cadastro em massa.")
            modelo_df = pd.DataFrame({"codigo": ["EX001"],"descricao": ["Parafuso"],"unidade": ["Un"],"categoria": ["Estrutural"], "estoque_minimo": [100.0],"estoque_maximo": [1000.0],"observacoes": ["Uso geral"]})
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer: modelo_df.to_excel(writer, index=False, sheet_name='Materiais')
            st.download_button(label="📄 Baixar Planilha Modelo", data=output.getvalue(), file_name="modelo_cadastro_materiais.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            uploaded_file = st.file_uploader("Selecione a planilha preenchida", type=['xlsx'], on_change=on_file_change, key="upload_lote_cadastro")
            if uploaded_file is not None and not st.session_state.upload_processado:
                try:
                    upload_df = pd.read_excel(uploaded_file, dtype={'codigo': str})
                    upload_df.dropna(how='all', inplace=True)
                    upload_df.dropna(subset=['codigo', 'descricao'], inplace=True)
                    upload_df['codigo'] = upload_df['codigo'].str.strip()
                    if not upload_df.empty:
                        colunas_obrigatorias = ['codigo', 'descricao', 'unidade', 'categoria']
                        upload_df['erros_validacao'] = upload_df.apply(lambda row: ', '.join([col for col in colunas_obrigatorias if pd.isna(row[col]) or str(row[col]).strip() == '']), axis=1)
                        materiais_invalidos_campos = upload_df[upload_df['erros_validacao'] != ''].copy()
                        materiais_validos_campos = upload_df[upload_df['erros_validacao'] == ''].copy()
                        codigos_existentes = buscar_todos_codigos_materiais()
                        df_duplicados = materiais_validos_campos[materiais_validos_campos['codigo'].isin(codigos_existentes)]
                        df_para_processar = materiais_validos_campos[~materiais_validos_campos['codigo'].isin(codigos_existentes)]
                        df_para_processar['unidade_valida'] = df_para_processar['unidade'].isin(UNIDADES_VALIDAS)
                        df_para_processar['categoria_valida'] = df_para_processar['categoria'].isin(CATEGORIAS_VALIDAS)
                        df_validos = df_para_processar[df_para_processar['unidade_valida'] & df_para_processar['categoria_valida']]
                        df_para_revisao = df_para_processar[~(df_para_processar['unidade_valida'] & df_para_processar['categoria_valida'])]
                        st.markdown("---"); st.subheader("Análise da Planilha")
                        if not df_duplicados.empty: st.warning(f"**{len(df_duplicados)} materiais ignorados por já possuírem código cadastrado:**"); st.dataframe(df_duplicados[['codigo', 'descricao']])
                        if not materiais_invalidos_campos.empty: st.error(f"**{len(materiais_invalidos_campos)} materiais inválidos (campos obrigatórios faltando):**"); st.dataframe(materiais_invalidos_campos[['codigo', 'descricao', 'erros_validacao']])
                        novas_unidades = df_para_revisao[~df_para_revisao['unidade'].isin(UNIDADES_VALIDAS)]['unidade'].dropna().unique().tolist()
                        novas_categorias = df_para_revisao[~df_para_revisao['categoria'].isin(CATEGORIAS_VALIDAS)]['categoria'].dropna().unique().tolist()
                        unidades_aprovadas, categorias_aprovadas = [], []
                        if novas_unidades or novas_categorias:
                            st.info("Encontramos novos termos. Selecione quais deseja adicionar ao sistema.")
                            if novas_unidades: unidades_aprovadas = st.multiselect("Adicionar novas Unidades:", options=novas_unidades)
                            if novas_categorias: categorias_aprovadas = st.multiselect("Adicionar novas Categorias:", options=novas_categorias)
                        if not df_para_revisao.empty:
                            st.markdown("**Materiais com Inconsistências (Corrija aqui):**"); st.caption("Corrija a unidade ou categoria selecionando uma opção válida.")
                            unidades_oficiais_atualizadas = UNIDADES_VALIDAS + unidades_aprovadas; categorias_oficiais_atualizadas = CATEGORIAS_VALIDAS + categorias_aprovadas
                            edited_df_revisao = st.data_editor(df_para_revisao, column_config={"unidade": st.column_config.SelectboxColumn("Unidade*", options=unidades_oficiais_atualizadas, required=True), "categoria": st.column_config.SelectboxColumn("Categoria*", options=categorias_oficiais_atualizadas, required=True), "unidade_valida": None, "categoria_valida": None, "erros_validacao": None }, key="revisao_editor")
                        st.markdown("---"); st.success(f"**{len(df_validos)}** materiais estão 100% válidos e prontos para cadastro.")
                        if st.button("Confirmar Cadastro de Materiais Válidos e Corrigidos"):
                            df_para_cadastrar = df_validos.copy()
                            if 'edited_df_revisao' in locals() and not edited_df_revisao.empty:
                                for cat_aprovada in categorias_aprovadas: edited_df_revisao.loc[edited_df_revisao['categoria'] == cat_aprovada, 'categoria'] = cat_aprovada
                                for und_aprovada in unidades_aprovadas: edited_df_revisao.loc[edited_df_revisao['unidade'] == und_aprovada, 'unidade'] = und_aprovada
                                df_para_cadastrar = pd.concat([df_para_cadastrar, edited_df_revisao], ignore_index=True)
                            if not df_para_cadastrar.empty:
                                with st.spinner("Cadastrando..."): sucessos, erros, mensagens_erro = cadastrar_materiais_em_lote_db(df_para_cadastrar)
                                st.success(f"{sucessos} materiais cadastrados com sucesso!")
                                if erros > 0:
                                    st.error(f"{erros} materiais não puderam ser cadastrados."); 
                                    for msg in mensagens_erro: st.code(msg, language=None)
                                if sucessos > 0: st.cache_data.clear()
                                st.session_state.upload_processado = True; st.rerun()
                            else: st.warning("Nenhum material válido para cadastrar.")
                except Exception as e: st.error(f"Erro ao processar o arquivo: {e}")
        st.button("⬅️ Voltar para a Lista", on_click=ir_para_lista)

    elif st.session_state.pagina_materiais == 'editar':
        st.title("Editar Material")
        material_id = st.session_state.material_para_editar_id
        material_atual = buscar_material_por_id(material_id)
        if material_atual:
            with st.form("form_edicao"):
                st.write(f"**Editando:** {material_atual['descricao']}")
                novo_nome = st.text_input("Nome do Material*", value=material_atual['descricao'])
                novo_codigo = st.text_input("Código do Material*", value=material_atual['codigo'])
                unidade_index = UNIDADES_VALIDAS.index(material_atual['unidade']) if material_atual['unidade'] in UNIDADES_VALIDAS else 0
                nova_unidade = st.selectbox("Unidade de Medida*", UNIDADES_VALIDAS, index=unidade_index)
                categoria_index = CATEGORIAS_VALIDAS.index(material_atual['categoria']) if material_atual['categoria'] in CATEGORIAS_VALIDAS else 0
                nova_categoria = st.selectbox("Categoria*", CATEGORIAS_VALIDAS, index=categoria_index)
                novo_est_min = st.number_input("Estoque Mínimo", value=float(material_atual.get('estoque_minimo', 0) or 0), format="%.4f")
                novo_est_max = st.number_input("Estoque Máximo", value=float(material_atual.get('estoque_maximo', 0) or 0), format="%.4f")
                novas_obs = st.text_area("Observações", value=material_atual.get('observacoes', ''))
                submitted = st.form_submit_button("Salvar Alterações")
                if submitted:
                    if not novo_nome or not novo_codigo:
                        st.warning("Nome e Código são campos obrigatórios.")
                    else:
                        sucesso, msg = atualizar_material_db(material_id, novo_codigo, novo_nome, nova_unidade, nova_categoria, novo_est_min, novo_est_max, novas_obs)
                        if sucesso: st.success(msg); ir_para_lista()
                        else: st.error(msg)
        else:
            st.error("Material não encontrado. Retornando para a lista.")
            if st.button("Voltar", on_click=ir_para_lista): pass
        if st.button("⬅️ Voltar para a Lista", on_click=ir_para_lista): pass


def render_prazos_compra_page():
    st.title("⚙️ Configurações de Prazos de Compra")
    st.info("Defina aqui o prazo padrão de compra (em dias) para cada categoria de material.")

    # Busca as categorias já existentes nos materiais para sugerir no selectbox
    categorias_materiais = buscar_categorias_unicas()
    
    # Busca os prazos já cadastrados
    df_prazos = buscar_prazos_compra_db()

    # --- Formulário para Adicionar/Editar Prazos ---
    st.subheader("Adicionar ou Atualizar Prazo")
    with st.form("form_prazo"):
        cols = st.columns([2, 1])
        categoria_selecionada = cols[0].selectbox(
            "Selecione a Categoria",
            options=categorias_materiais,
            index=None,
            placeholder="Escolha uma categoria..."
        )
        prazo_dias_input = cols[1].number_input("Prazo em Dias", min_value=0, step=1, value=30)
        
        submitted = st.form_submit_button("Salvar Prazo")
        if submitted:
            if not categoria_selecionada:
                st.warning("Por favor, selecione uma categoria.")
            else:
                sucesso, msg = salvar_prazo_compra_db(categoria_selecionada, prazo_dias_input)
                if sucesso:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)
    
    st.markdown("---")

    # --- Tabela de Prazos Cadastrados ---
    st.subheader("Prazos Atuais")
    if df_prazos.empty:
        st.info("Nenhum prazo de compra cadastrado ainda.")
    else:
        # Adiciona uma coluna com botões de remover
        df_prazos['Ação'] = df_prazos['id'].apply(lambda x: x) # Apenas copia o ID para usar na key do botão
        
        st.data_editor(
            df_prazos,
            column_config={
                "id": None, # Oculta a coluna ID
                "categoria": st.column_config.TextColumn("Categoria", disabled=True),
                "prazo_dias": st.column_config.NumberColumn("Prazo (dias)", min_value=0, step=1),
                "Ação": st.column_config.SelectboxColumn( # Usamos Selectbox como "botão" dentro do editor
                    "Remover?",
                    options=["Não", "Sim"],
                    default="Não",
                    width="small"
                )
            },
            hide_index=True,
            use_container_width=True,
            key="editor_prazos"
        )
        
        # Lógica para processar as edições feitas na tabela
        if 'editor_prazos' in st.session_state:
            edited_rows = st.session_state.editor_prazos.get("edited_rows", {})
            ids_para_remover = []
            
            for idx, changes in edited_rows.items():
                prazo_id = df_prazos.iloc[idx]['id']
                
                # Se a quantidade foi editada, salva a alteração
                if 'prazo_dias' in changes:
                    novo_prazo = changes['prazo_dias']
                    categoria_original = df_prazos.iloc[idx]['categoria']
                    salvar_prazo_compra_db(categoria_original, novo_prazo) # Salva a atualização
                    st.toast(f"Prazo para '{categoria_original}' atualizado para {novo_prazo} dias.")
                
                # Se a ação "Remover?" foi marcada como "Sim"
                if changes.get("Ação") == "Sim":
                    # Converte o ID para um int padrão do Python antes de adicionar
                    ids_para_remover.append(int(prazo_id)) # <-- CORREÇÃO AQUI

            if ids_para_remover:
                for prazo_id in ids_para_remover:
                    remover_prazo_compra_db(prazo_id)
                st.success(f"{len(ids_para_remover)} prazo(s) removido(s).")
                st.rerun() # Recarrega para atualizar a tabela

def limpar_formulario_mov():
        """Função para resetar os campos do formulário após o envio."""
        st.session_state.mov_tipo = None
        st.session_state.mov_material_display = None
        st.session_state.mov_quantidade = 1.0
        st.session_state.mov_fornecedor = ""
        st.session_state.mov_recebedor = ""
        st.session_state.mov_obra_destino_nome = None
        st.session_state.mov_observacoes = ""

def render_relatar_movimentacao_page():

    # --- Inicialização de Estado da Página (sem alterações) ---
    if "pagina_mov" not in st.session_state:
        st.session_state.pagina_mov = "lancamento"
    if "mov_para_estornar_id" not in st.session_state:
        st.session_state.mov_para_estornar_id = None
    if "filtros_historico" not in st.session_state:
        st.session_state.filtros_historico = {}

    # --- NOVO: Inicialização do estado do formulário para evitar perda de dados ---
    if 'mov_tipo' not in st.session_state: st.session_state.mov_tipo = "Entrada"
    if 'mov_material_display' not in st.session_state: st.session_state.mov_material_display = None
    if 'mov_quantidade' not in st.session_state: st.session_state.mov_quantidade = 1.0
    if 'mov_fornecedor' not in st.session_state: st.session_state.mov_fornecedor = ""
    if 'mov_recebedor' not in st.session_state: st.session_state.mov_recebedor = ""
    if 'mov_obra_destino_nome' not in st.session_state: st.session_state.mov_obra_destino_nome = None
    if 'mov_observacoes' not in st.session_state: st.session_state.mov_observacoes = ""

    obra_id = st.session_state.obra_selecionada_id
    usuario_id = st.session_state.usuario_id

    # --- Funções de Navegação e Limpeza ---
    def ir_para_historico():
        st.session_state.pagina_mov = "historico"
        st.session_state.filtros_historico = {}
    
    def ir_para_lancamento():
        st.session_state.pagina_mov = "lancamento"

    # --- Roteador de Página ---
    if st.session_state.pagina_mov == "lancamento":
        st.title("Registrar Movimentação")

        col1, col2 = st.columns([1, 1])
        with col1:

            # Busca de dados
            obra_id = st.session_state.obra_selecionada_id
            usuario_id = st.session_state.usuario_id
            df_materiais = buscar_materiais_para_selecao()
            df_outras_obras = buscar_outras_obras(obra_id)

            # >>> NOVA FUNÇÃO "HANDLER" PARA O CLIQUE DO BOTÃO <<<
            def handle_submission():
                # 1. Validação (lendo direto do session_state)
                obra_destino_id = None
                if st.session_state.mov_obra_destino_nome:
                    obra_destino_id = int(df_outras_obras[df_outras_obras.nome_obra == st.session_state.mov_obra_destino_nome]['id'].iloc[0])
                
                if not all([st.session_state.mov_tipo, st.session_state.mov_material_display, st.session_state.mov_quantidade > 0]):
                    st.warning("Preencha todos os campos obrigatórios (*).")
                    return # Para a execução aqui se for inválido
                
                if st.session_state.mov_tipo in ["Transferência", "Empréstimo", "Devolução"] and not obra_destino_id:
                    st.warning("Para este tipo de movimentação, a obra de destino é obrigatória.")
                    return # Para a execução aqui se for inválido

                # 2. Preparação dos dados
                dados_mov = {
                    "tipo": st.session_state.mov_tipo,
                    "material_id": int(df_materiais[df_materiais.display == st.session_state.mov_material_display]['id'].iloc[0]),
                    "descricao": df_materiais[df_materiais.display == st.session_state.mov_material_display]['descricao'].iloc[0],
                    "quantidade": st.session_state.mov_quantidade, 
                    "data": st.session_state.data_movimentacao, 
                    "observacoes": st.session_state.mov_observacoes,
                    "fornecedor": st.session_state.mov_fornecedor, 
                    "recebedor": st.session_state.mov_recebedor,
                    "obra_origem_id": obra_id, 
                    "obra_destino_id": obra_destino_id
                }

                # 3. Execução da transação no banco
                sucesso, msg = registrar_movimentacao_db(obra_id, usuario_id, dados_mov)
                if sucesso:
                    st.success(msg)
                    limpar_formulario_mov() # Limpa o formulário SE for um sucesso
                    st.cache_resource.clear()
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(msg)

            # --- FORMULÁRIO DINÂMICO (SEM st.form) ---
            st.subheader("Detalhes da Movimentação")

            # Todos os widgets agora usam o parâmetro 'key' para salvar seu estado
            
            cols_tipo_data_hora = st.columns([1.5, 1, 1])
            
            cols_tipo_data_hora[0].selectbox(
                "Tipo da Movim.*", 
                ["Entrada", "Saída", "Transferência", "Empréstimo", "Devolução"], 
                key="mov_tipo" # Este selectbox agora controla a renderização
            )
            data_movimentacao = datetime.combine(
                cols_tipo_data_hora[1].date_input("Data*", value=datetime.now()),
                cols_tipo_data_hora[2].time_input("Hora*", value=datetime.now().time())
            )


            cols_material_qtd = st.columns([2, 1])
            cols_material_qtd[0].selectbox(
                "Material*", 
                options=df_materiais['display'].tolist(), 
                index=None, 
                placeholder="Selecione um material...",
                key="mov_material_display"
            )
            cols_material_qtd[1].number_input("Quantidade*", min_value=0.0001, format="%.4f", key="mov_quantidade")

        with col2:   
            # >>> LÓGICA DE EXIBIÇÃO DINÂMICA <<<
            # Lê o tipo de movimentação diretamente do session_state       
            
            if st.session_state.mov_tipo in ["Entrada", "Saída"]:
                st.subheader("Origem / Destino (Externo)")
                cols_Forn_Receb = st.columns(2)
                cols_Forn_Receb[0].text_input("Fornecedor / Origem", key="mov_fornecedor")
                cols_Forn_Receb[1].text_input("Recebedor / Destino", key="mov_recebedor")

            else: # Transferência, Empréstimo, Devolução
                st.subheader("Obra de Destino (Interno)")
                st.selectbox(
                    "Obra de Destino*", 
                    options=df_outras_obras['nome_obra'].tolist(), 
                    index=None, 
                    placeholder="Selecione a obra de destino...",
                    key="mov_obra_destino_nome"
                )
            
            st.text_area("Observações", key="mov_observacoes")
            
            botao_canto_direito = st.columns([1, 0.5, 2])
            with botao_canto_direito[2]:
                if st.button("✅ Registrar Movimentação",use_container_width=True, type="primary"):
                    # A validação e o envio agora acontecem aqui, lendo do st.session_state
                    obra_destino_id = None
                    if st.session_state.mov_obra_destino_nome:
                        obra_destino_id = int(df_outras_obras[df_outras_obras.nome_obra == st.session_state.mov_obra_destino_nome]['id'].iloc[0])
                    
                    if not all([st.session_state.mov_tipo, st.session_state.mov_material_display, st.session_state.mov_quantidade > 0]):
                        st.warning("Preencha todos os campos obrigatórios (*).")
                    elif st.session_state.mov_tipo in ["Transferência", "Empréstimo", "Devolução"] and not obra_destino_id:
                        st.warning("Para este tipo de movimentação, a obra de destino é obrigatória.")
                    else:
                        dados_mov = {
                            "tipo": st.session_state.mov_tipo,
                            "material_id": int(df_materiais[df_materiais.display == st.session_state.mov_material_display]['id'].iloc[0]),
                            "descricao": df_materiais[df_materiais.display == st.session_state.mov_material_display]['descricao'].iloc[0],
                            "quantidade": st.session_state.mov_quantidade, 
                            "data": data_movimentacao, 
                            "observacoes": st.session_state.mov_observacoes,
                            "fornecedor": st.session_state.mov_fornecedor, 
                            "recebedor": st.session_state.mov_recebedor,
                            "obra_origem_id": obra_id, 
                            "obra_destino_id": obra_destino_id
                        }
                        sucesso, msg = registrar_movimentacao_db(obra_id, usuario_id, dados_mov)

                        if sucesso:
                            st.success(msg)
                            st.session_state.limpar_form_agora = True
                            st.rerun()
                        else:
                            st.error(msg)
        
        st.markdown("---")
        st.subheader("Histórico Recente")
        st.button("Ver Histórico Completo 📜", on_click=ir_para_historico, use_container_width=True)
        
        df_recente = buscar_historico_db(obra_id, limit=10)
        if not df_recente.empty:
            for _, row in df_recente.iterrows():
                # ... (o seu código do histórico recente continua aqui sem alterações) ...
                cols = st.columns([2, 2, 1, 1, 1])
                cols[0].write(f"**{row['descricao']}**")
                cols[1].write(f"_{row['tipo']}_")
                cols[2].write(f"**Qtd:** {row['quantidade']:.2f}")
                cols[3].write(row['data_formatada'])
                if cols[4].button("Estornar", key=f"estornar_rec_{row['id']}", use_container_width=True):
                    st.session_state.mov_para_estornar_id = row['id']
                    st.rerun()
                
                if st.session_state.mov_para_estornar_id == row['id']:
                    st.warning(f"Tem certeza que deseja estornar a movimentação de **{row['descricao']}**?")
                    c1, c2 = st.columns(2)
                    if c1.button("Sim, tenho certeza", key=f"conf_est_{row['id']}", type="primary"):
                        sucesso, msg = criar_movimentacao_estorno_db(row['id'], usuario_id)
                        if sucesso: st.success(msg)
                        else: st.error(msg)
                        st.session_state.mov_para_estornar_id = None
                        st.rerun()
                    if c2.button("Cancelar", key=f"canc_est_{row['id']}"):
                        st.session_state.mov_para_estornar_id = None
                        st.rerun()
        else:
            st.info("Nenhuma movimentação registrada recentemente.")

    elif st.session_state.pagina_mov == "historico":
        # ... (a sua página de histórico continua aqui sem alterações) ...
        st.title("Histórico de Movimentações")
        st.write("#### Ferramentas de Busca")
        tipos_disponiveis = ["Entrada", "Saída"]
        
        cols_filtro = st.columns([1, 1, 2, 1, 1])
        with cols_filtro[0]:
            data_inicio_input = st.date_input("Data Inicial", value=None)
        with cols_filtro[1]:
            data_fim_input = st.date_input("Data Final", value=None)
        with cols_filtro[2]:
            tipos_selecionados_input = st.multiselect("Filtrar por Tipo", options=tipos_disponiveis)
        
        with cols_filtro[3]:
            st.markdown("<div><br></div>", unsafe_allow_html=True)
            if st.button("Aplicar Filtros", use_container_width=True, type="primary"):
                st.session_state.filtros_historico = {
                    'data_inicio': data_inicio_input,
                    'data_fim': data_fim_input,
                    'tipos_transacao': tipos_selecionados_input
                }
                st.rerun()

        with cols_filtro[4]:
            st.markdown("<div><br></div>", unsafe_allow_html=True)
            if st.button("Limpar Filtros", use_container_width=True):
                st.session_state.filtros_historico = {}
                st.rerun()

        st.markdown("---")
        filtros_ativos = st.session_state.get('filtros_historico', {})
        df_historico = buscar_historico_db(
            obra_id, 
            data_inicio=filtros_ativos.get('data_inicio'), 
            data_fim=filtros_ativos.get('data_fim'), 
            tipos_transacao=filtros_ativos.get('tipos_transacao')
        )

        st.markdown(f"**{len(df_historico)} registros encontrados.**")
        st.dataframe(df_historico, use_container_width=True)
        
        st.button("⬅️ Voltar para Lançamentos", on_click=ir_para_lancamento)

def render_kits_cadastrados_page():
    
    obra_id = st.session_state.obra_selecionada_id
    
    st.subheader("Solicitação de Montagem de KITs")
    df_solicitacoes = buscar_solicitacoes_montagem_db(obra_id)

    if df_solicitacoes.empty:
        st.info("Nenhuma solicitação de montagem de kit pendente.")
    else:
        for _, solicitacao in df_solicitacoes.iterrows():
            cols = st.columns([2, 1.5, 1, 1.5, 1.5, 1.5])
            with cols[0]:
                st.write(f"**{solicitacao['nome_kit']}**")
            with cols[1]:
                st.write(f"Para: **{solicitacao['data_execucao']}**")
            with cols[2]:
                st.write(f"Qtde: **{solicitacao['quantidade_kits']}**")

            # Lógica dos botões
            if solicitacao['status'] == 'Pendente':
                if cols[3].button("✅ Kit Montado", key=f"mont_{solicitacao['id']}", use_container_width=True):
                    atualizar_status_solicitacao_db(solicitacao['id'], "Montado")
                    st.rerun()
            elif solicitacao['status'] == 'Montado':
                cols[3].success("Montado", icon="✅")

            if solicitacao['status'] == 'Montado':
                if cols[4].button("🚚 Kit Entregue", key=f"entr_{solicitacao['id']}", use_container_width=True, type="primary"):
                    atualizar_status_solicitacao_db(solicitacao['id'], "Entregue")
                    st.rerun()
            
            if cols[5].button("Cancelar", key=f"canc_{solicitacao['id']}", use_container_width=True):
                atualizar_status_solicitacao_db(solicitacao['id'], "Cancelado")
                st.rerun()
    
    st.markdown("---")
    
    # --- Inicialização de Estado da Página ---
    if 'pagina_kits' not in st.session_state:
        st.session_state.pagina_kits = 'listar'
    if 'kit_para_editar_id' not in st.session_state:
        st.session_state.kit_para_editar_id = None
    if 'kit_materiais_temp' not in st.session_state:
        st.session_state.kit_materiais_temp = []
    if 'kit_info_edicao' not in st.session_state:
        st.session_state.kit_info_edicao = None
    if 'operacao_concluida' not in st.session_state:
        st.session_state.operacao_concluida = None

    obra_id = st.session_state.obra_selecionada_id
    df_materiais_disponiveis = buscar_materiais_para_selecao() 

    # --- Funções de Navegação Interna ---
    def ir_para_cadastro():
        st.session_state.kit_materiais_temp = []
        st.session_state.pagina_kits = 'cadastrar'

    def ir_para_edicao(kit_id):
        st.session_state.kit_para_editar_id = kit_id
        st.session_state.pagina_kits = 'editar'
        st.session_state.kit_info_edicao = None 

    def ir_para_lista():
        st.session_state.kit_materiais_temp = []
        st.session_state.kit_para_editar_id = None
        st.session_state.kit_info_edicao = None
        st.session_state.pagina_kits = 'listar'

    # --- LÓGICA DE NAVEGAÇÃO PÓS-AÇÃO ---
    if st.session_state.get("operacao_concluida") == "kit_salvo":
        st.session_state.operacao_concluida = None
        ir_para_lista()
        st.rerun()

    # --- Roteador de Páginas ---

    if st.session_state.pagina_kits == 'listar':
        
        cols = st.columns([3, 1])
        with cols[0]:
            st.subheader("Kits de Materiais Cadastrados")
        with cols[1]:
            st.button("➕ Novo Kit", on_click=ir_para_cadastro, use_container_width=True)
        st.markdown("---")

        df_kits = buscar_kits_da_obra_db(obra_id)
        if df_kits.empty:
            st.info("Nenhum kit cadastrado para esta obra.")
        else:
            for _, kit in df_kits.iterrows():
                with st.expander(f"**{kit['nome']}**"):
                    st.caption(kit['descricao'] or "Sem descrição.")
                    df_materiais_do_kit = buscar_materiais_de_um_kit_db(kit['id'])
                    st.dataframe(df_materiais_do_kit, use_container_width=True, hide_index=True)
                    
                    c1, c2 = st.columns(2)
                    c1.button("✏️ Editar", key=f"edit_kit_{kit['id']}", on_click=ir_para_edicao, args=(kit['id'],), use_container_width=True)
                    if c2.button("🗑️ Excluir", key=f"del_kit_{kit['id']}", use_container_width=True):
                        sucesso, msg = excluir_kit_db(kit['id'])
                        if sucesso: st.success(msg)
                        else: st.error(msg)
                        st.rerun()

    elif st.session_state.pagina_kits == 'cadastrar':
        st.title("Cadastrar Novo Kit")
        with st.form("form_cad_kit"):
            nome_kit = st.text_input("Nome do Kit*")
            desc_kit = st.text_area("Descrição do Kit")
            st.markdown("---")
            
            st.subheader("Adicionar Materiais ao Kit")
            c1, c2 = st.columns([3,1])
            material_selecionado = c1.selectbox("Material", options=df_materiais_disponiveis['display'], index=None, placeholder="Selecione um material...")
            quantidade = c2.number_input("Quantidade", min_value=0.01, format="%.4f", value=1.0)
            
            if st.form_submit_button("Adicionar Material ➕"):
                if material_selecionado:
                    material_info = df_materiais_disponiveis[df_materiais_disponiveis['display'] == material_selecionado].iloc[0]
                    novo_item = {'id': int(material_info['id']), 'descricao': material_info['descricao'], 'unidade': material_info['unidade'], 'quantidade': quantidade}
                    if not any(item['id'] == novo_item['id'] for item in st.session_state.kit_materiais_temp):
                        st.session_state.kit_materiais_temp.append(novo_item)
                    else:
                        st.warning("Este material já foi adicionado.")
                else:
                    st.warning("Selecione um material para adicionar.")
                st.rerun()

            if st.session_state.kit_materiais_temp:
                st.write("Materiais no Kit:")
                df_temp = pd.DataFrame(st.session_state.kit_materiais_temp)
                st.dataframe(df_temp[['descricao', 'quantidade', 'unidade']], hide_index=True)
            
            st.markdown("---")
            if st.form_submit_button("✅ Salvar Kit Completo", type="primary"):
                if not nome_kit:
                    st.error("O nome do kit é obrigatório.")
                elif not st.session_state.kit_materiais_temp:
                    st.error("Adicione pelo menos um material ao kit.")
                else:
                    sucesso, msg = salvar_kit_completo_db(obra_id, nome_kit, desc_kit, st.session_state.kit_materiais_temp)
                    if sucesso:
                        st.success(msg)
                        st.session_state.operacao_concluida = "kit_salvo"
                        st.rerun()
                    else:
                        st.error(msg)
        
        st.button("⬅️ Voltar para a Lista", on_click=ir_para_lista)

    elif st.session_state.pagina_kits == 'editar':
        st.title("Editar Kit")
        kit_id = st.session_state.kit_para_editar_id
        
        # Carrega os dados do kit para a memória (session_state) apenas uma vez
        if st.session_state.kit_info_edicao is None:
            df_kits = buscar_kits_da_obra_db(obra_id)
            kit_info = df_kits[df_kits['id'] == kit_id].iloc[0]
            df_materiais_kit = buscar_materiais_de_um_kit_db(kit_id)
            
            materiais_com_id = []
            for _, row in df_materiais_kit.iterrows():
                info_completa = df_materiais_disponiveis[df_materiais_disponiveis['descricao'] == row['descricao']]
                if not info_completa.empty:
                    materiais_com_id.append({
                        'id': int(info_completa.iloc[0]['id']),
                        'descricao': row['descricao'],
                        'quantidade': row['quantidade'],
                        'unidade': row['unidade']
                    })
            st.session_state.kit_info_edicao = {'id': kit_id, 'nome': kit_info['nome'], 'descricao': kit_info['descricao'], 'materiais': materiais_com_id}
        
        kit_atual = st.session_state.kit_info_edicao

        # --- SEÇÃO 1: INFORMAÇÕES DO KIT (CAMPOS DE TEXTO) ---
        st.subheader("Informações do Kit")
        novo_nome = st.text_input("Nome do Kit*", value=kit_atual['nome'], key="kit_edit_nome")
        nova_desc = st.text_area("Descrição", value=kit_atual['descricao'], key="kit_edit_desc")
        st.markdown("---")

        # --- SEÇÃO 2: MATERIAIS NO KIT (TABELA EDITÁVEL) ---
        st.subheader("Materiais no Kit")
        if kit_atual['materiais']:
            df_para_editar = pd.DataFrame(kit_atual['materiais'])
            df_para_editar['Remover'] = False
            
            edited_df = st.data_editor(
                df_para_editar[['id', 'descricao', 'quantidade', 'unidade', 'Remover']],
                column_config={"id": None, "descricao": st.column_config.TextColumn("Descrição", disabled=True), "quantidade": st.column_config.NumberColumn("Quantidade", min_value=0.01, format="%.4f"), "unidade": st.column_config.TextColumn("Un.", disabled=True), "Remover": st.column_config.CheckboxColumn("Remover?")},
                hide_index=True, key="editor_materiais_kit"
            )

        # --- SEÇÃO 3: ADICIONAR NOVOS MATERIAIS ---
        st.markdown("---")
        st.subheader("Adicionar Novos Materiais")
        c1, c2 = st.columns([3,1])
        material_selecionado = c1.selectbox("Material", options=df_materiais_disponiveis['display'], index=None, placeholder="Selecione um material...", key="kit_add_mat_select")
        quantidade = c2.number_input("Quantidade", min_value=0.01, format="%.4f", value=1.0, key="kit_add_mat_qty")

        if st.button("Adicionar Material ➕"):
            if material_selecionado:
                material_info = df_materiais_disponiveis[df_materiais_disponiveis['display'] == material_selecionado].iloc[0]
                novo_item = {'id': int(material_info['id']), 'descricao': material_info['descricao'], 'unidade': material_info['unidade'], 'quantidade': quantidade}
                if not any(item['id'] == novo_item['id'] for item in kit_atual['materiais']):
                    kit_atual['materiais'].append(novo_item)
                    st.rerun() # Apenas recarrega para mostrar o item na lista
                else:
                    st.warning("Este material já foi adicionado.")
            else:
                st.warning("Selecione um material para adicionar.")

        # --- SEÇÃO 4: AÇÕES FINAIS (SALVAR E VOLTAR) ---
        st.markdown("---")
        c1, c2 = st.columns(2)
        if c1.button("✅ Salvar Alterações", type="primary", use_container_width=True):
            # 1. Pega os valores atualizados dos campos de texto
            nome_final = st.session_state.kit_edit_nome
            desc_final = st.session_state.kit_edit_desc

            # 2. Processa a lista final de materiais a partir do data_editor
            lista_materiais_finais = []
            if 'edited_df' in locals():
                materiais_a_manter_df = pd.DataFrame(edited_df)[edited_df['Remover'] == False]
                lista_materiais_finais = materiais_a_manter_df[['id', 'quantidade']].to_dict('records')

            # 3. Chama o backend
            sucesso, msg = atualizar_kit_db(kit_id, nome_final, desc_final, lista_materiais_finais)
            
            if sucesso:
                st.success(msg)
                st.session_state.operacao_concluida = "kit_salvo"
                st.rerun()
            else:
                st.error(msg)
        
        c2.button("⬅️ Voltar para a Lista (sem salvar)", on_click=ir_para_lista, use_container_width=True)

def render_central_de_transferencias_page():
    st.title("Central de Transferências")
    obra_id = st.session_state.obra_selecionada_id
    usuario_id = st.session_state.usuario_id

    # Cria as três abas
    tab_recebidas, tab_enviadas, tab_historico, tab_balanco = st.tabs([
        "Solicitações Recebidas", 
        "Solicitações Enviadas", 
        "Histórico de Transações",
        "Balanço de Transações"
    ])

    # --- Aba 1: Solicitações Recebidas ---
    with tab_recebidas:
        st.subheader("Aguardando sua aprovação")
        df_recebidas = buscar_transacoes_db(obra_id, tipo_busca='recebidas')

        if df_recebidas.empty:
            st.info("Nenhuma solicitação recebida pendente.")
        else:
            for _, row in df_recebidas.iterrows():
                cols = st.columns([1, 2, 3, 1, 2])
                with cols[0]:
                    st.caption(row['tipo_transacao'])
                with cols[1]:
                    st.write(f"**De:** {row['obra_origem']}")
                with cols[2]:
                    st.write(f"**{row['quantidade']} {row['material_unidade']}** de *{row['material_descricao']}*")
                with cols[3]:
                    st.caption(f"Solicitado em:")
                    st.caption(row['data_solicitacao'])
                with cols[4]:
                    c_btn1, c_btn2 = st.columns(2)
                    if c_btn1.button("Aprovar ✅", key=f"apv_{row['id']}", use_container_width=True):
                        sucesso, msg = processar_transacao_db(row['id'], 'Aprovada', usuario_id)
                        if sucesso: st.success(msg)
                        else: st.error(msg)
                        st.rerun()

                    if c_btn2.button("Recusar ❌", key=f"rec_{row['id']}", use_container_width=True):
                        sucesso, msg = processar_transacao_db(row['id'], 'Recusada', usuario_id)
                        if sucesso: st.warning(msg)
                        else: st.error(msg)
                        st.rerun()
                st.markdown("---")

    # --- Aba 2: Solicitações Enviadas ---
    with tab_enviadas:
        st.subheader("Solicitações que você enviou")
        df_enviadas = buscar_transacoes_db(obra_id, tipo_busca='enviadas')

        if df_enviadas.empty:
            st.info("Nenhuma solicitação enviada pendente.")
        else:
            for _, row in df_enviadas.iterrows():
                cols = st.columns([1, 2, 3, 1, 1])
                cols[0].caption(row['tipo_transacao'])
                cols[1].write(f"**Para:** {row['obra_destino']}")
                cols[2].write(f"**{row['quantidade']} {row['material_unidade']}** de *{row['material_descricao']}*")
                cols[3].caption(f"Enviado em:")
                cols[3].caption(row['data_solicitacao'])
                if cols[4].button("Cancelar ⚠️", key=f"canc_env_{row['id']}", use_container_width=True):
                    sucesso, msg = processar_transacao_db(row['id'], 'Cancelada', usuario_id)
                    if sucesso: st.warning(msg)
                    else: st.error(msg)
                    st.rerun()
                st.markdown("---")

    # --- Aba 3: Histórico de Transações ---
    with tab_historico:
        st.subheader("Histórico de todas as transações concluídas")
        df_historico = buscar_historico_transacoes_db(obra_id)
        if df_historico.empty:
            st.info("Nenhum registro encontrado no histórico.")
        else:
            st.dataframe(df_historico, use_container_width=True)

    # --- Aba 4: Balanço de Transações ---
    with tab_balanco:
        st.title("📊 Balanço de Transações")
        st.info("Esta tela mostra os saldos de materiais de 'Empréstimo' e 'Devolução' com outras obras.")
        
        obra_id = st.session_state.obra_selecionada_id
        
        # Chama a função de backend para obter os dataframes
        df_dividas, df_creditos = calcular_balanco_emprestimos_db(obra_id)
        
        st.markdown("---")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Materiais a Devolver (Suas Dívidas)")
            if df_dividas.empty:
                st.success("🎉 Você não tem nenhuma dívida de material com outras obras.")
            else:
                # Reordena as colunas para a exibição desejada
                df_dividas = df_dividas[["Material", "Quantidade Pendente", "Devolver para a Obra"]]
                st.dataframe(df_dividas, use_container_width=True, hide_index=True)
                
        with col2:
            st.subheader("Materiais a Receber (Seus Créditos)")
            if df_creditos.empty:
                st.info("ℹ️ Você não possui materiais a receber de outras obras.")
            else:
                # Reordena as colunas para a exibição desejada
                df_creditos = df_creditos[["Material", "Quantidade Pendente", "Receber da Obra"]]
                st.dataframe(df_creditos, use_container_width=True, hide_index=True)


# ADICIONE ESTA FUNÇÃO NO ESCOPO GLOBAL
def handle_tarefa_selection(df_tarefas_display):
    """Callback para atualizar a lista de tarefas selecionadas para vinculação em lote."""
    if 'editor_tarefas_lote' in st.session_state:
        # Pega as edições feitas no data_editor (quais checkboxes mudaram)
        edited_rows = st.session_state.editor_tarefas_lote.get("edited_rows", {})
        
        # Pega o conjunto atual de IDs selecionados
        ids_ja_selecionados = st.session_state.get('tarefas_selecionadas_lote_ids', set())
        
        # Itera sobre as linhas que foram editadas pelo usuário
        for idx, row_changes in edited_rows.items():
            # Pega o ID da tarefa correspondente à linha editada no dataframe *exibido*
            tarefa_id = int(df_tarefas_display.iloc[int(idx)]['id'])
            
            # Verifica se a coluna 'Selecionar' foi alterada
            if "Selecionar" in row_changes:
                if row_changes["Selecionar"]:
                    # Se foi marcada, adiciona o ID ao conjunto
                    ids_ja_selecionados.add(tarefa_id)
                else:
                    # Se foi desmarcada, remove o ID do conjunto (se existir)
                    ids_ja_selecionados.discard(tarefa_id)
                    
        # Atualiza o session_state com o novo conjunto de IDs selecionados
        st.session_state.tarefas_selecionadas_lote_ids = ids_ja_selecionados


def render_planejamento_page():
    st.title(" Planejamento e Vinculação de Kits")
    obra_id = st.session_state.obra_selecionada_id

    # --- Inicialização de Estado Específico da Página ---
    if 'tarefas_selecionadas_lote_ids' not in st.session_state:
        st.session_state.tarefas_selecionadas_lote_ids = set()
    if 'filtro_tarefa_lote' not in st.session_state:
        st.session_state.filtro_tarefa_lote = ""

    # --- Pré-busca de dados ---
    df_tarefas_all = buscar_tarefas_db(obra_id)
    df_kits_obra = buscar_kits_da_obra_db(obra_id)
    opcoes_kits = df_kits_obra.set_index('id')['nome'].to_dict()

    # --- SEÇÃO 1: UPLOAD DO ARQUIVO (código existente) ---
    st.subheader("Importar/Atualizar Planejamento (.xlsx)")
    # ... (o seu código do file_uploader continua aqui, sem alterações) ...
    if 'plan_file' not in st.session_state:
        st.session_state.plan_file = None
    uploaded_file = st.file_uploader(
        "Selecione o arquivo Excel exportado do MS Project",
        type=["xlsx"],
        key=str(st.session_state.get('plan_file', ''))
    )
    if uploaded_file is not None:
        st.session_state.plan_file = uploaded_file
    if st.session_state.plan_file is not None:
        if st.button("Processar Arquivo"):
            with st.spinner("Comparando e atualizando planejamento..."):
                sucesso, relatorio = ler_e_salvar_tarefas_excel(st.session_state.plan_file, obra_id)
            if sucesso:
                st.success("Planejamento atualizado com sucesso!")
                if relatorio["adicionadas"]:
                    with st.expander(f"✅ {len(relatorio['adicionadas'])} Tarefas Adicionadas"):
                        for tarefa in relatorio["adicionadas"]: st.write(f"- {tarefa}")
                if relatorio["removidas"]:
                    with st.expander(f"❌ {len(relatorio['removidas'])} Tarefas Removidas"):
                        for tarefa in relatorio["removidas"]: st.write(f"- {tarefa}")
                if relatorio["modificadas"]:
                    with st.expander(f"✏️ {len(relatorio['modificadas'])} Tarefas Modificadas"):
                        for tarefa in relatorio["modificadas"]: st.write(f"- {tarefa}")
                st.session_state.plan_file = None
                st.rerun()
            else:
                st.error(relatorio, icon="🚨")

    st.info("""
        **Atenção:** Verifique se a coluna 'Id_exclusiva' existe em seu MSProject antes de exportar para Excel.
        Caso não, é simples. No MS Project:
        1. Clique com o botão direito no cabeçalho de qualquer coluna (ex: 'Nome da Tarefa').
        2. Selecione 'Inserir Coluna';
        3. Na lista de campos, escolha 'ID Exclusivo' ou 'ID Exclusiva';
        4. Exporte o arquivo para Excel novamente.
        """)
    
    st.markdown("---")

    # --- SEÇÃO 2: VINCULAR KIT EM LOTE (NOVO LAYOUT) ---
    st.subheader("Vincular Kit a Múltiplas Tarefas (em Lote)")

    if df_tarefas_all.empty:
        st.warning("Importe um planejamento primeiro para poder vincular kits.")
    elif df_kits_obra.empty:
        st.warning("Cadastre pelo menos um kit nesta obra para poder vinculá-lo.")
    else:
        # --- Filtro ---
        st.session_state.filtro_tarefa_lote = st.text_input(
            "Filtrar tarefas por nome:", 
            value=st.session_state.filtro_tarefa_lote,
            placeholder="Digite parte do nome da tarefa..."
        )

        # Aplica o filtro
        df_tarefas_filtradas = df_tarefas_all[
            df_tarefas_all['nome_tarefa'].str.contains(st.session_state.filtro_tarefa_lote, case=False, na=False)
        ]

        # --- Preparação do DataFrame para o Editor ---
        df_display = df_tarefas_filtradas[['id', 'nome_tarefa', 'data_inicio_fmt']].copy()
        # Marca como True as linhas cujos IDs estão no nosso conjunto de seleção
        df_display['Selecionar'] = df_display['id'].apply(lambda tid: tid in st.session_state.tarefas_selecionadas_lote_ids)

        # --- Exibição com st.data_editor ---
        st.write("Selecione as tarefas na tabela abaixo:")
        edited_df = st.data_editor(
            df_display,
            key="editor_tarefas_lote",
            on_change=handle_tarefa_selection, # Chama nosso callback
            args=(df_display,), # Passa o dataframe exibido para o callback
            column_config={
                "id": None, # Oculta a coluna ID
                "nome_tarefa": st.column_config.TextColumn("Nome da Tarefa", disabled=True),
                "data_inicio_fmt": st.column_config.TextColumn("Início Previsto", disabled=True),
                "Selecionar": st.column_config.CheckboxColumn(required=True)
            },
            hide_index=True,
            use_container_width=True
        )

        # Mostra quantas tarefas estão selecionadas
        num_selecionadas = len(st.session_state.tarefas_selecionadas_lote_ids)
        if num_selecionadas > 0:
            st.info(f"{num_selecionadas} tarefa(s) selecionada(s).")
        else:
            st.caption("Nenhuma tarefa selecionada.")

        st.markdown("---")

        # --- Seleção do Kit e Botão de Ação ---
        cols_lote = st.columns([3, 1, 1])
        with cols_lote[0]:
            kit_id_lote = st.selectbox(
                "Selecione o Kit a ser vinculado",
                options=list(opcoes_kits.keys()),
                format_func=lambda x: opcoes_kits.get(x, "Kit Inválido"),
                index=None, placeholder="Escolha um kit..."
            )
        with cols_lote[1]:
            quantidade_lote = st.number_input("Qtde. por Tarefa", min_value=1, step=1, value=1)
        with cols_lote[2]:
            st.markdown("<div><br></div>", unsafe_allow_html=True) # Espaçador
            if st.button("🔗 Vincular em Lote", type="primary", use_container_width=True):
                # Usa a lista de IDs guardada no session_state
                tarefas_selecionadas_ids_list = list(st.session_state.tarefas_selecionadas_lote_ids)
                
                if not tarefas_selecionadas_ids_list:
                    st.warning("Selecione pelo menos uma tarefa na tabela.")
                elif kit_id_lote is None:
                    st.warning("Selecione um kit para vincular.")
                else:
                    with st.spinner("Vinculando kits às tarefas selecionadas..."):
                        sucessos, mensagens = vincular_kit_a_multiplas_tarefas_db(tarefas_selecionadas_ids_list, kit_id_lote, quantidade_lote)
                    
                    for msg in mensagens:
                        if "sucesso" in msg.lower(): st.success(msg)
                        elif "já estava vinculado" in msg.lower(): st.info(msg)
                        else: st.error(msg)
                    
                    if sucessos > 0:
                        # Limpa a seleção após o sucesso
                        st.session_state.tarefas_selecionadas_lote_ids = set() 
                        st.rerun() 

    st.markdown("---")

    # --- SEÇÃO 3: VINCULAÇÃO INDIVIDUAL (código existente) ---
    st.subheader("Vincular Kits às Tarefas Individualmente")

    if df_tarefas_all.empty:
        st.info("Nenhuma tarefa de planejamento importada para esta obra ainda.")
    elif df_kits_obra.empty:
         st.warning("Não há kits cadastrados para esta obra.")
    else:
        # Itera sobre cada tarefa importada
        for _, tarefa in df_tarefas_all.iterrows():
            with st.expander(f"**Tarefa:** {tarefa['nome_tarefa']}  |  **Início Previsto:** {tarefa['data_inicio_fmt']}"):
                # ... (o seu código para exibir kits vinculados e o form de vinculação individual continua aqui, sem alterações) ...
                st.write("**Kits já vinculados:**")
                df_vinculados = buscar_kits_vinculados_db(tarefa['id'])
                if df_vinculados.empty:
                    st.caption("Nenhum kit vinculado a esta tarefa ainda.")
                else:
                    for _, vinculo in df_vinculados.iterrows():
                        col1, col2, col3 = st.columns([2, 1, 1])
                        col1.info(f"Kit: {vinculo['nome']}")
                        col2.info(f"Qtde: {vinculo['quantidade_kits']}")
                        if col3.button("Remover", key=f"del_{vinculo['id']}", use_container_width=True):
                            sucesso, msg = desvincular_kit_da_tarefa_db(vinculo['id'])
                            if sucesso: st.success(msg)
                            else: st.error(msg)
                            st.rerun()
                st.markdown("---")
                with st.form(key=f"form_{tarefa['id']}"):
                    st.write("**Adicionar novo kit a esta tarefa:**")
                    cols = st.columns([3, 1])
                    kit_id_selecionado = cols[0].selectbox(
                        "Selecione o Kit", options=list(opcoes_kits.keys()),
                        format_func=lambda x: opcoes_kits.get(x, "Kit Inválido"),
                        label_visibility="collapsed"
                    )
                    quantidade = cols[1].number_input("Qtde.", min_value=1, step=1, value=1, label_visibility="collapsed")
                    if st.form_submit_button("➕ Vincular Kit"):
                        if kit_id_selecionado:
                            sucesso, msg = vincular_kit_a_tarefa_db(tarefa['id'], kit_id_selecionado, quantidade)
                            if sucesso: st.success(msg)
                            else: st.warning(msg)
                            st.rerun()

#endregion

#region render_main_app

def render_main_app():
    # Verifica se já rodamos as verificações nesta sessão
    if 'verificacoes_rodaram' not in st.session_state:
        st.session_state.verificacoes_rodaram = False

    if 'obra_selecionada_id' in st.session_state:
        # Só roda SE ainda não rodou hoje/nesta sessão
        if not st.session_state.verificacoes_rodaram:
            with st.spinner("Atualizando notificações e tarefas..."): # Feedback visual
                verificar_e_gerar_solicitacoes_db(st.session_state.obra_selecionada_id)
                verificar_e_gerar_notificacoes_compra(st.session_state.obra_selecionada_id)
            st.session_state.verificacoes_rodaram = True # Marca como feito


    with st.sidebar:
        st.markdown(f"**Usuário:** `{st.session_state.usuario_nome}`")

        conexao_obras = obter_conexao_para_transacao()
        obras_df = pd.DataFrame()
        if conexao_obras and conexao_obras.is_connected():
            try:
                obras_df = pd.read_sql(
                    "SELECT o.id, o.nome_obra FROM obras o JOIN usuario_obras_acesso uoa ON o.id = uoa.obra_id WHERE uoa.usuario_id = %s ORDER BY o.nome_obra",
                    conexao_obras,
                    params=(st.session_state.usuario_id,)
                )
            except Error as e:
                st.sidebar.error(f"Não foi possível carregar as obras: {e}")
            finally:
                if conexao_obras.is_connected(): conexao_obras.close()
        else:
            st.sidebar.error("Falha na conexão para carregar obras.")

        if not obras_df.empty:
            obra_selecionada_nome = st.selectbox("Selecione a Obra:", options=obras_df['nome_obra'].tolist(), key='obra_selectbox')
            st.session_state.obra_selecionada_id = int(obras_df[obras_df.nome_obra == obra_selecionada_nome]['id'].iloc[0])
        else:
            st.sidebar.warning("Você não tem acesso a nenhuma obra.")
            st.stop()

        st.sidebar.markdown("---")
        st.sidebar.markdown("## Menu Principal")
        opcao = st.sidebar.radio(
            "Escolha uma opção:",
            [
                "INÍCIO", "RELATAR MOVIMENTAÇÃO", "ESTOQUE DE MATERIAIS",
                "KITS", "CENTRAL DE TRANSFERÊNCIAS", "PLANEJAMENTO", "PRAZOS DE COMPRA"
            ],
            key='menu_principal'
        )

    # Roteador de páginas
    if 'obra_selecionada_id' in st.session_state:
        obra_id = st.session_state.obra_selecionada_id

        if opcao == "INÍCIO":
            st.title(f"Bem-vindo à Obra: {st.session_state.get('obra_selectbox', '')}")
            st.markdown("---")

            # >>> ADICIONADO: USO DE TABS PARA NOTIFICAÇÕES <<<
            tab_pendentes, tab_solicitadas = st.tabs(["🔔 Compras Pendentes", "📜 Compras Solicitadas (Histórico)"])

            # --- Aba de Notificações Pendentes ---
            with tab_pendentes:
                st.subheader("Solicitações de Compra Pendentes")
                df_notificacoes_compra = buscar_notificacoes_compra_db(obra_id)

                if df_notificacoes_compra.empty:
                    st.info("Nenhuma notificação de compra pendente no momento.")
                else:
                    st.warning("Atenção! Os kits abaixo precisam ser solicitados para compra:")
                    for _, notificacao in df_notificacoes_compra.iterrows():
                        cols = st.columns([3, 2, 1.5])
                        cols[0].write(f"**Kit:** {notificacao['nome_kit']}")
                        cols[1].write(f"**Necessário para:** {notificacao['data_necessidade_fmt']} (Tarefa: *{notificacao['nome_tarefa']}*)")
                        
                        if cols[2].button("Marcar como Solicitado", key=f"sol_{notificacao['id']}", use_container_width=True, type="primary"):
                            sucesso, msg = atualizar_status_notificacao_compra_db(notificacao['id'], 'Solicitado')
                            if sucesso:
                                st.toast(f"Kit '{notificacao['nome_kit']}' marcado como solicitado.", icon="✅")
                                st.rerun()
                            else:
                                st.error(msg)
            
            # --- Aba de Notificações Solicitadas (Histórico) ---
            with tab_solicitadas:
                st.subheader("Histórico de Solicitações de Compra Realizadas")
                df_solicitadas = buscar_notificacoes_compra_solicitadas_db(obra_id)

                if df_solicitadas.empty:
                    st.info("Nenhuma solicitação de compra foi marcada como 'Solicitada' ainda.")
                else:
                    st.dataframe(
                        df_solicitadas,
                        use_container_width=True,
                        hide_index=True
                    )
            # >>> FIM DAS TABS <<<

        # O resto das opções do menu continua igual
        elif opcao == "ESTOQUE DE MATERIAIS":
            render_estoque_materiais_page()
        elif opcao == "RELATAR MOVIMENTAÇÃO":
            render_relatar_movimentacao_page()
        elif opcao == "KITS":
            render_kits_cadastrados_page()
        elif opcao == "CENTRAL DE TRANSFERÊNCIAS":
            render_central_de_transferencias_page()
        elif opcao == "PLANEJAMENTO":
            render_planejamento_page()
        elif opcao == "PRAZOS DE COMPRA":
            render_prazos_compra_page()

        
#endregion

if 'usuario_logado' not in st.session_state: st.session_state.usuario_logado = False
if not st.session_state.usuario_logado: 
    render_login_page()
else: 
    render_main_app()