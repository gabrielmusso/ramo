import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
import polars as pl
import pyarrow as pa
from pyarrow import csv as pa_csv
import threading
import os
import re
import numpy as np
import traceback
import datetime

# ==========================================
# --- CONFIGURAÇÕES GLOBAIS ---
# ==========================================
# Retornamos para latin-1, pois o Pandas (que fará a leitura) lida perfeitamente com ele
CODIFICACAO = 'latin-1' 
PASTA_DADOS = 'dados'
PASTA_SAIDA = 'relatorios_gerados'
ARQUIVO_MUNICIPIOS = os.path.join(PASTA_DADOS, 'cod_municipios_ibge.csv')
TAMANHO_BLOCO = 1_000_000 # Restaurado para o controle detalhado do Log

# Colunas Alvo
COLUNA_MUNICIPIO = 'Município - Código'
COLUNA_MUNICIPIO_TRAB = 'Município Trab - Código'
COLUNA_CNAE_CLASSE = 'CNAE 2.0 Classe - Código'
COLUNA_CNAE_SUBCLASSE = 'CNAE 2.0 Subclasse - Código'
COLUNA_FILTRO_VINCULO = 'Ind Vínculo Ativo 31/12 - Código'
COLUNA_REMUNERACAO = 'Vl Rem Dezembro Nom'
COLUNA_GENERO = 'Sexo - Código'
COLUNA_RACA = 'Raça Cor - Código'

# Mapas de Dados
MAPA_GENERO = {'01': 'Masculino', '1': 'Masculino', '02': 'Feminino', '2': 'Feminino'}
MAPA_RACA = {
    '01': 'INDIGENA', '1': 'INDIGENA',
    '02': 'BRANCA', '2': 'BRANCA',
    '04': 'PRETA', '4': 'PRETA',
    '06': 'AMARELA', '6': 'AMARELA',
    '08': 'PARDA', '8': 'PARDA',
    '09': 'NAO IDENT', '9': 'NAO IDENT',
    '-1': 'IGNORADO'
}

# Mapa de CNAEs
CNAE_MAP = {
    '64.10-7 - Banco Central': '64107', '64.21-2 - Bancos comerciais': '64212',
    '64.22-1 - Bancos múltiplos, com carteira comercial': '64221', '64.23-9 - Caixas econômicas': '64239',
    '64.24-7 - Crédito cooperativo': '64247', '64.31-0 - Bancos múltiplos, sem carteira comercial': '64310',
    '64.32-8 - Bancos de investimento': '64328', '64.33-6 - Bancos de desenvolvimento': '64336',
    '64.34-4 - Agências de fomento': '64344', '64.35-2 - Crédito imobiliário': '64352',
    '64.36-1 - Sociedades de crédito, financiamento e investimento - financeiras': '64361',
    '64.37-9 - Sociedades de crédito ao microempreendedor': '64379',
    '64.38-7 - Bancos de câmbio e outras instituições de intermediação não-monetária': '64387',
    '64.40-9 - Arrendamento mercantil': '64409', '64.50-6 - Sociedades de capitalização': '64506',
    '64.61-1 - Holdings de instituições financeiras': '64611', '64.62-0 - Holdings de instituições não-financeiras': '64620',
    '64.63-8 - Outras sociedades de participação, exceto holdings': '64638', '64.70-1 - Fundos de investimento': '64701',
    '64.91-3 - Sociedades de fomento mercantil - factoring': '64913', '64.92-1 - Securitização de créditos': '64921',
    '64.93-0 - Administração de consórcios para aquisição de bens e direitos': '64930',
    '64.99-9 - Outras atividades de serviços financeiros não especificadas anteriormente': '64999',
    '65.11-1 - Seguros de vida': '65111', '65.12-0 - Seguros não-vida': '65120', '65.20-1 - Seguros-saúde': '65201',
    '65.30-8 - Resseguros': '65308', '65.41-3 - Previdência complementar fechada': '65413',
    '65.42-1 - Previdência complementar aberta': '65421', '65.50-2 - Planos de saúde': '65502',
    '66.11-8 - Administração de bolsas e mercados de balcão organizados': '66118',
    '66.12-6 - Atividades de intermediários em transações de títulos, valores mobiliários e mercadorias': '66126',
    '66.13-4 - Administração de cartões de crédito': '66134',
    '66.19-3 - Atividades auxiliares dos serviços financeiros não especificadas anteriormente': '66193',
    '66.21-5 - Avaliação de riscos e perdas': '66215',
    '66.22-3 - Corretores e agentes de seguros, de planos de previdência complementar e de saúde': '66223',
    '66.29-1 - Atividades auxiliares dos seguros, da previdência complementar e dos planos de saúde não especificadas anteriormente': '66291',
    '66.30-4 - Atividades de administração de fundos por contrato ou comissão': '66304'
}

# Estrutura Hierárquica CNAE
CNAE_ESTRUTURA = [
    {'level': 0, 'code': 'K', 'titulo': 'ATIVIDADES FINANCEIRAS, DE SEGUROS E SERVIÇOS RELACIONADOS', 'clean_code': 'K'},
    {'level': 1, 'code': '64', 'titulo': 'ATIVIDADES DE SERVIÇOS FINANCEIROS', 'clean_code': '64'},
    {'level': 2, 'code': '64.1', 'titulo': 'Banco Central', 'clean_code': '641'},
    {'level': 3, 'code': '64.10-7', 'titulo': 'Banco Central', 'clean_code': '64107'},
    {'level': 2, 'code': '64.2', 'titulo': 'Intermediação monetária - depósitos à vista', 'clean_code': '642'},
    {'level': 3, 'code': '64.21-2', 'titulo': 'Bancos comerciais', 'clean_code': '64212'},
    {'level': 3, 'code': '64.22-1', 'titulo': 'Bancos múltiplos, com carteira comercial', 'clean_code': '64221'},
    {'level': 3, 'code': '64.23-9', 'titulo': 'Caixas econômicas', 'clean_code': '64239'},
    {'level': 3, 'code': '64.24-7', 'titulo': 'Crédito cooperativo', 'clean_code': '64247'},
    {'level': 2, 'code': '64.3', 'titulo': 'Intermediação não-monetária - outros instrumentos de captação', 'clean_code': '643'},
    {'level': 3, 'code': '64.31-0', 'titulo': 'Bancos múltiplos, sem carteira comercial', 'clean_code': '64310'},
    {'level': 3, 'code': '64.32-8', 'titulo': 'Bancos de investimento', 'clean_code': '64328'},
    {'level': 3, 'code': '64.33-6', 'titulo': 'Bancos de desenvolvimento', 'clean_code': '64336'},
    {'level': 3, 'code': '64.34-4', 'titulo': 'Agências de fomento', 'clean_code': '64344'},
    {'level': 3, 'code': '64.35-2', 'titulo': 'Crédito imobiliário', 'clean_code': '64352'},
    {'level': 3, 'code': '64.36-1', 'titulo': 'Sociedades de crédito, financiamento e investimento - financeiras', 'clean_code': '64361'},
    {'level': 3, 'code': '64.37-9', 'titulo': 'Sociedades de crédito ao microempreendedor', 'clean_code': '64379'},
    {'level': 3, 'code': '64.38-7', 'titulo': 'Bancos de câmbio e outras instituições de intermediação não-monetária', 'clean_code': '64387'},
    {'level': 2, 'code': '64.4', 'titulo': 'Arrendamento mercantil', 'clean_code': '644'},
    {'level': 3, 'code': '64.40-9', 'titulo': 'Arrendamento mercantil', 'clean_code': '64409'},
    {'level': 2, 'code': '64.5', 'titulo': 'Sociedades de capitalização', 'clean_code': '645'},
    {'level': 3, 'code': '64.50-6', 'titulo': 'Sociedades de capitalização', 'clean_code': '64506'},
    {'level': 2, 'code': '64.6', 'titulo': 'Atividades de sociedades de participação', 'clean_code': '646'},
    {'level': 3, 'code': '64.61-1', 'titulo': 'Holdings de instituições financeiras', 'clean_code': '64611'},
    {'level': 3, 'code': '64.62-0', 'titulo': 'Holdings de instituições não-financeiras', 'clean_code': '64620'},
    {'level': 3, 'code': '64.63-8', 'titulo': 'Outras sociedades de participação, exceto holdings', 'clean_code': '64638'},
    {'level': 2, 'code': '64.7', 'titulo': 'Fundos de investimento', 'clean_code': '647'},
    {'level': 3, 'code': '64.70-1', 'titulo': 'Fundos de investimento', 'clean_code': '64701'},
    {'level': 2, 'code': '64.9', 'titulo': 'Atividades de serviços financeiros não especificadas anteriormente', 'clean_code': '649'},
    {'level': 3, 'code': '64.91-3', 'titulo': 'Sociedades de fomento mercantil - factoring', 'clean_code': '64913'},
    {'level': 3, 'code': '64.92-1', 'titulo': 'Securitização de créditos', 'clean_code': '64921'},
    {'level': 3, 'code': '64.93-0', 'titulo': 'Administração de consórcios para aquisição de bens e direitos', 'clean_code': '64930'},
    {'level': 3, 'code': '64.99-9', 'titulo': 'Outras atividades de serviços financeiros não especificadas anteriormente', 'clean_code': '64999'},
    {'level': 1, 'code': '65', 'titulo': 'SEGUROS, RESSEGUROS, PREVIDÊNCIA COMPLEMENTAR E PLANOS DE SAÚDE', 'clean_code': '65'},
    {'level': 2, 'code': '65.1', 'titulo': 'Seguros de vida e não-vida', 'clean_code': '651'},
    {'level': 3, 'code': '65.11-1', 'titulo': 'Seguros de vida', 'clean_code': '65111'},
    {'level': 3, 'code': '65.12-0', 'titulo': 'Seguros não-vida', 'clean_code': '65120'},
    {'level': 2, 'code': '65.2', 'titulo': 'Seguros-saúde', 'clean_code': '652'},
    {'level': 3, 'code': '65.20-1', 'titulo': 'Seguros-saúde', 'clean_code': '65201'},
    {'level': 2, 'code': '65.3', 'titulo': 'Resseguros', 'clean_code': '653'},
    {'level': 3, 'code': '65.30-8', 'titulo': 'Resseguros', 'clean_code': '65308'},
    {'level': 2, 'code': '65.4', 'titulo': 'Previdência complementar', 'clean_code': '654'},
    {'level': 3, 'code': '65.41-3', 'titulo': 'Previdência complementar fechada', 'clean_code': '65413'},
    {'level': 3, 'code': '65.42-1', 'titulo': 'Previdência complementar aberta', 'clean_code': '65421'},
    {'level': 2, 'code': '65.5', 'titulo': 'Planos de saúde', 'clean_code': '655'},
    {'level': 3, 'code': '65.50-2', 'titulo': 'Planos de saúde', 'clean_code': '65502'},
    {'level': 1, 'code': '66', 'titulo': 'ATIVIDADES AUXILIARES DOS SERVIÇOS FINANCEIROS, SEGUROS, PREVIDÊNCIA COMPLEMENTAR E PLANOS DE SAÚDE', 'clean_code': '66'},
    {'level': 2, 'code': '66.1', 'titulo': 'Atividades auxiliares dos serviços financeiros', 'clean_code': '661'},
    {'level': 3, 'code': '66.11-8', 'titulo': 'Administração de bolsas e mercados de balcão organizados', 'clean_code': '66118'},
    {'level': 3, 'code': '66.12-6', 'titulo': 'Atividades de intermediários em transações de títulos, valores mobiliários e mercadorias', 'clean_code': '66126'},
    {'level': 3, 'code': '66.13-4', 'titulo': 'Administração de cartões de crédito', 'clean_code': '66134'},
    {'level': 3, 'code': '66.19-3', 'titulo': 'Atividades auxiliares dos serviços financeiros não especificadas anteriormente', 'clean_code': '66193'},
    {'level': 2, 'code': '66.2', 'titulo': 'Atividades auxiliares dos seguros, da previdência complementar e dos planos de saúde', 'clean_code': '662'},
    {'level': 3, 'code': '66.21-5', 'titulo': 'Avaliação de riscos e perdas', 'clean_code': '66215'},
    {'level': 3, 'code': '66.22-3', 'titulo': 'Corretores e agentes de seguros, de planos de previdência complementar e de saúde', 'clean_code': '66223'},
    {'level': 3, 'code': '66.29-1', 'titulo': 'Atividades auxiliares dos seguros, da previdência complementar e dos planos de saúde não especificadas anteriormente', 'clean_code': '66291'},
    {'level': 2, 'code': '66.3', 'titulo': 'Atividades de administração de fundos por contrato ou comissão', 'clean_code': '663'},
    {'level': 3, 'code': '66.30-4', 'titulo': 'Atividades de administração de fundos por contrato ou comissão', 'clean_code': '66304'}
]

REGIOES_ARQUIVOS = {
    'RAIS_VINC_PUB_NORTE.COMT': ['AC', 'Acre', 'AP', 'Amapá', 'AM', 'Amazonas', 'PA', 'Pará', 'RO', 'Rondônia', 'RR', 'Roraima', 'TO', 'Tocantins'],
    'RAIS_VINC_PUB_NORDESTE.COMT': ['AL', 'Alagoas', 'BA', 'Bahia', 'CE', 'Ceará', 'MA', 'Maranhão', 'PB', 'Paraíba', 'PE', 'Pernambuco', 'PI', 'Piauí', 'RN', 'Rio Grande do Norte', 'SE', 'Sergipe'],
    'RAIS_VINC_PUB_CENTRO_OESTE.COMT': ['DF', 'Distrito Federal', 'GO', 'Goiás', 'MT', 'Mato Grosso', 'MS', 'Mato Grosso do Sul'],
    'RAIS_VINC_PUB_MG_ES_RJ.COMT': ['ES', 'Espírito Santo', 'MG', 'Minas Gerais', 'RJ', 'Rio de Janeiro'],
    'RAIS_VINC_PUB_SP.COMT': ['SP', 'São Paulo'],
    'RAIS_VINC_PUB_SUL.COMT': ['PR', 'Paraná', 'RS', 'Rio Grande do Sul', 'SC', 'Santa Catarina']
}
MAPA_ARQUIVOS_UF = {uf: arquivo for arquivo, ufs in REGIOES_ARQUIVOS.items() for uf in ufs}

os.makedirs(PASTA_SAIDA, exist_ok=True)


# ==========================================
# --- FUNÇÕES DE PROCESSAMENTO (EXCEL) ---
# ==========================================

def limpar_e_converter_remuneracao(coluna: pd.Series) -> pd.Series:
    coluna_str = coluna.astype(str)
    coluna_limpa = coluna_str.str.strip().str.replace(',', '.')
    return pd.to_numeric(coluna_limpa, errors='coerce')

def calcular_metricas(group: pd.DataFrame) -> pd.Series:
    total_vinculos = len(group)
    if total_vinculos == 0:
        return pd.Series({
            'Remuneracao Media': '-', 'Pct Homens': '-', 'Pct Mulheres': '-',
            'Total Vinculos': 0, 'Pct Branca': '-', 'Pct Preta': '-',
            'Pct Parda': '-', 'Pct Amarela': '-', 'Pct Indigena': '-', 'Pct Ignorado/NI': '-'
        })

    media_remuneracao = group['Remuneracao_Limpa'].mean(skipna=True)
    media_remuneracao = round(media_remuneracao, 2) if pd.notna(media_remuneracao) else np.nan
    
    contagem_genero = group['Genero_Limpo'].value_counts(normalize=True)
    pct_homem = round(contagem_genero.get('Masculino', 0.0), 3)
    pct_mulher = round(contagem_genero.get('Feminino', 0.0), 3)
    
    contagem_raca = group['Raca_Limpa'].value_counts(normalize=True)
    pct_branca = round(contagem_raca.get('BRANCA', 0.0), 3)
    pct_preta = round(contagem_raca.get('PRETA', 0.0), 3)
    pct_parda = round(contagem_raca.get('PARDA', 0.0), 3)
    pct_amarela = round(contagem_raca.get('AMARELA', 0.0), 3)
    pct_indigena = round(contagem_raca.get('INDIGENA', 0.0), 3)
    pct_ignorado_ni = round(contagem_raca.get('NAO IDENT', 0.0) + contagem_raca.get('IGNORADO', 0.0), 3)

    return pd.Series({
        'Remunacao Media': media_remuneracao,
        'Pct Homens': pct_homem,
        'Pct Mulheres': pct_mulher,
        'Total Vinculos': total_vinculos,
        'Pct Branca': pct_branca,
        'Pct Preta': pct_preta,
        'Pct Parda': pct_parda,
        'Pct Amarela': pct_amarela,
        'Pct Indigena': pct_indigena,
        'Pct Ignorado/NI': pct_ignorado_ni
    })

def salvar_relatorio_excel(df_final: pd.DataFrame, caminho_arquivo: str):
    caminho_completo = os.path.join(PASTA_SAIDA, caminho_arquivo)
    try:
        excel_kwargs = {'options': {'nan_inf_to_errors': True}}
        with pd.ExcelWriter(caminho_completo, engine='xlsxwriter', engine_kwargs=excel_kwargs) as writer:
            workbook = writer.book
            worksheet = workbook.add_worksheet('Relatorio_CNAE')
            worksheet.set_margins(left=0.5, right=0.5, top=0.75, bottom=0.75)

            fmt_money = {'num_format': '#,##0.00', 'align': 'right'}
            fmt_percent = {'num_format': '0.0%', 'align': 'right'}
            fmt_number = {'num_format': '#,##0', 'align': 'right'}
            fmt_string_r = {'align': 'right'}
            font_base = {'font_name': 'Montserrat'}
            align_c = {'align': 'center'}
            align_l = {'align': 'left'}
            
            style_l0 = {'font_size': 12, 'bold': True, 'bottom': 6}
            style_l1 = {'font_size': 10, 'bold': True, 'bottom': 5}
            style_l2 = {'font_size': 9,  'bold': True, 'bottom': 1}
            style_l3 = {'font_size': 9,  'bold': False,'bottom': 4}

            fmt_l0_text = workbook.add_format({**font_base, **style_l0, **align_l})
            fmt_l1_code = workbook.add_format({**font_base, **style_l1, **align_c})
            fmt_l1_desc = workbook.add_format({**font_base, **style_l1, **align_l})
            fmt_l2_code = workbook.add_format({**font_base, **style_l2, **align_c})
            fmt_l2_desc = workbook.add_format({**font_base, **style_l2, **align_l})
            fmt_l3_code = workbook.add_format({**font_base, **style_l3, **align_c})
            fmt_l3_desc = workbook.add_format({**font_base, **style_l3, **align_l})

            num_formats = {}
            for i, style in enumerate([style_l0, style_l1, style_l2, style_l3]):
                num_formats[f'L{i}_Money'] = workbook.add_format({**font_base, **style, **fmt_money})
                num_formats[f'L{i}_Percent'] = workbook.add_format({**font_base, **style, **fmt_percent})
                num_formats[f'L{i}_Number'] = workbook.add_format({**font_base, **style, **fmt_number})
                num_formats[f'L{i}_StringR'] = workbook.add_format({**font_base, **style, **fmt_string_r})

            wrap_c = workbook.add_format({**font_base, **align_c})
            wrap_l = workbook.add_format({**font_base, **align_l})
            money_default = workbook.add_format({**font_base, **fmt_money})
            percent_default = workbook.add_format({**font_base, **fmt_percent})
            number_default = workbook.add_format({**font_base, **fmt_number})

            worksheet.set_column_pixels('A:A', 10, wrap_c)
            worksheet.set_column_pixels('B:B', 50, wrap_c)
            worksheet.set_column_pixels('C:C', 50, wrap_c)
            worksheet.set_column_pixels('D:D', 50, wrap_l)
            worksheet.set_column_pixels('E:E', 650, wrap_l)
            worksheet.set_column_pixels('F:F', 75, number_default)
            worksheet.set_column_pixels('G:G', 110, money_default)
            worksheet.set_column_pixels('H:O', 75, percent_default)

            header_format = workbook.add_format({'font_name': 'Montserrat', 'bold': True, 'align': 'center', 'bottom': 1, 'font_size': 10, 'text_wrap': True})
            headers = list(df_final.columns)
            headers[0:5] = [''] * 5 
            
            worksheet.write_row('A1', headers, header_format)
            worksheet.set_row_pixels(0, 20)
            
            row_num = 1
            for _, row_data in df_final.iterrows():
                level = CNAE_ESTRUTURA[row_num - 1]['level']
                
                if level == 0:
                    worksheet.write_string(row_num, 0, str(row_data['Nível 1']), fmt_l0_text)
                    for col in range(1, 5): worksheet.write_blank(row_num, col, '', fmt_l0_text)
                elif level == 1:
                    worksheet.write_string(row_num, 1, str(row_data['Nível 2']), fmt_l1_code)
                    worksheet.write_string(row_num, 2, str(row_data['Nível 3']), fmt_l1_desc)
                    for col in [0, 3, 4]: worksheet.write_blank(row_num, col, '', fmt_l1_desc if col > 0 else fmt_l1_code)
                elif level == 2:
                    worksheet.write_string(row_num, 2, str(row_data['Nível 3']), fmt_l2_code)
                    worksheet.write_string(row_num, 3, str(row_data['Nível 4']), fmt_l2_desc)
                    for col in [0, 1, 4]: worksheet.write_blank(row_num, col, '', fmt_l2_desc if col == 4 else fmt_l2_code)
                elif level == 3:
                    worksheet.write_string(row_num, 3, str(row_data['Nível 4']), fmt_l3_code)
                    worksheet.write_string(row_num, 4, str(row_data['Nível 5']), fmt_l3_desc)
                    for col in [0, 1, 2]: worksheet.write_blank(row_num, col, '', fmt_l3_code)

                fmt_dinheiro = num_formats[f'L{level}_Money']
                fmt_porcento = num_formats[f'L{level}_Percent']
                fmt_inteiro = num_formats[f'L{level}_Number']
                fmt_str_direita = num_formats[f'L{level}_StringR']

                data_cols_formats = [
                    ('Total Vínculos', fmt_inteiro), ('Remuneração Média', fmt_dinheiro),
                    ('Pct Homens', fmt_porcento), ('Pct Mulheres', fmt_porcento),
                    ('Pct Branca', fmt_porcento), ('Pct Preta', fmt_porcento),
                    ('Pct Parda', fmt_porcento), ('Pct Amarela', fmt_porcento),
                    ('Pct Indigena', fmt_porcento), ('Pct Ignorado/NI', fmt_porcento)
                ]

                col_idx = 5
                for col_name, data_format in data_cols_formats:
                    value = row_data[col_name]
                    if value == '-': worksheet.write_string(row_num, col_idx, '-', fmt_str_direita) 
                    elif pd.isna(value): worksheet.write_blank(row_num, col_idx, '', data_format)
                    else: worksheet.write_number(row_num, col_idx, value, data_format)
                    col_idx += 1
                row_num += 1

        print(f"Relatório salvo em: {caminho_completo}")
        
    except Exception as e:
        print(f"Erro ao salvar ficheiro Excel {caminho_completo}: {e}")

def processar_dataframe_para_relatorio(df: pd.DataFrame, nome_base_arquivo: str):
    if df.empty:
        print(f"DataFrame para '{nome_base_arquivo}' está vazio. Pulando.")
        return

    required_cols = [COLUNA_REMUNERACAO, COLUNA_GENERO, COLUNA_CNAE_CLASSE, COLUNA_RACA]
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        raise KeyError(f"Colunas não encontradas no ficheiro: {', '.join(missing_cols)}")

    df_proc = df.copy().assign(
        Remuneracao_Limpa = lambda x: limpar_e_converter_remuneracao(x[COLUNA_REMUNERACAO]),
        Genero_Limpo = lambda x: x[COLUNA_GENERO].astype(str).str.strip().map(MAPA_GENERO),
        CNAE_Classe_Limpo = lambda x: x[COLUNA_CNAE_CLASSE].astype(str).str.replace(r'[.-]', '', regex=True).str.strip(),
        Raca_Limpa = lambda x: x[COLUNA_RACA].astype(str).str.strip().map(MAPA_RACA),
        CNAE_Grupo_Limpo = lambda x: x['CNAE_Classe_Limpo'].str.slice(0, 3), 
        CNAE_Divisao_Limpo = lambda x: x['CNAE_Classe_Limpo'].str.slice(0, 2)
    )

    colunas_para_metricas = ['Remuneracao_Limpa', 'Genero_Limpo', 'Raca_Limpa']

    agg_classe = df_proc.groupby('CNAE_Classe_Limpo')[colunas_para_metricas].apply(calcular_metricas, include_groups=False).to_dict('index')
    agg_grupo = df_proc.groupby('CNAE_Grupo_Limpo')[colunas_para_metricas].apply(calcular_metricas, include_groups=False).to_dict('index')
    agg_divisao = df_proc.groupby('CNAE_Divisao_Limpo')[colunas_para_metricas].apply(calcular_metricas, include_groups=False).to_dict('index')
    agg_total = calcular_metricas(df_proc).to_dict()

    dados_relatorio_final = []
    
    for item in CNAE_ESTRUTURA:
        level = item['level']
        clean_code = item['clean_code']
        
        metrics = {}
        if level == 0: metrics = agg_total
        elif level == 1: metrics = agg_divisao.get(clean_code, {})
        elif level == 2: metrics = agg_grupo.get(clean_code, {})
        elif level == 3: metrics = agg_classe.get(clean_code, {})
        
        linha = {
            'Nível 1': item['titulo'] if level == 0 else '',
            'Nível 2': item['code'] if level == 1 else '',
            'Nível 3': item['titulo'] if level == 1 else (item['code'] if level == 2 else ''),
            'Nível 4': item['titulo'] if level == 2 else (item['code'] if level == 3 else ''),
            'Nível 5': item['titulo'] if level == 3 else '',
            'Total Vínculos': metrics.get('Total Vinculos'),
            'Remuneração Média': metrics.get('Remunacao Media'),
            'Pct Homens': metrics.get('Pct Homens'),
            'Pct Mulheres': metrics.get('Pct Mulheres'),
            'Pct Branca': metrics.get('Pct Branca'),
            'Pct Preta': metrics.get('Pct Preta'),
            'Pct Parda': metrics.get('Pct Parda'),
            'Pct Amarela': metrics.get('Pct Amarela'),
            'Pct Indigena': metrics.get('Pct Indigena'),
            'Pct Ignorado/NI': metrics.get('Pct Ignorado/NI')
        }
        dados_relatorio_final.append(linha)

    df_final = pd.DataFrame(dados_relatorio_final)
    caminho_arquivo_excel = f"Relatorio_Tabela ({nome_base_arquivo}).xlsx"
    salvar_relatorio_excel(df_final, caminho_arquivo_excel)


# ==========================================
# --- INTERFACE GRÁFICA E MOTOR HÍBRIDO ---
# ==========================================

class App:
    def __init__(self, root, ufs, df_municipios, municipios_map):
        self.root = root
        self.ufs = ufs
        self.df_municipios = df_municipios
        self.municipios_map = municipios_map

        self.root.title("Extrator e Gerador RAIS (Híbrido: Pandas+Polars)")
        self.root.geometry("800x750")

        main_frame = ttk.Frame(root, padding="20")
        main_frame.pack(expand=True, fill=tk.BOTH)
        main_frame.columnconfigure(1, weight=1)

        self.setup_selecao_localidade(main_frame)

        self.start_button = ttk.Button(main_frame, text="Extrair Dados e Gerar Relatórios", command=self.start_filter_thread)
        self.start_button.grid(row=6, column=0, columnspan=2, padx=5, pady=20)

        self.status_label = ttk.Label(main_frame, text="Pronto para iniciar. Arquitetura de extração otimizada.")
        self.status_label.grid(row=7, column=0, columnspan=2, padx=5, pady=5)

    def atualizar_interface(self, func, *args, **kwargs):
        self.root.after(0, lambda: func(*args, **kwargs))

    def atualizar_status(self, texto):
        self.atualizar_interface(self.status_label.config, text=texto)

    def setup_selecao_localidade(self, parent):
        ttk.Label(parent, text="1. Selecione a UF:").grid(row=0, column=0, padx=5, pady=10, sticky="w")
        self.uf_var = tk.StringVar()
        self.uf_combo = ttk.Combobox(parent, textvariable=self.uf_var, state="readonly", width=40)
        self.uf_combo['values'] = self.ufs
        self.uf_combo.grid(row=0, column=1, padx=5, pady=10, sticky="ew")
        self.uf_combo.bind("<<ComboboxSelected>>", self.update_municipios)

        ttk.Label(parent, text="2. Selecione o Município:").grid(row=1, column=0, padx=5, pady=10, sticky="w")
        self.municipio_var = tk.StringVar()
        self.municipio_combo = ttk.Combobox(parent, textvariable=self.municipio_var, state="disabled", width=40)
        self.municipio_combo.grid(row=1, column=1, padx=5, pady=10, sticky="ew")

        self.incluir_button = ttk.Button(parent, text="Incluir Município ⬇", command=self.incluir_municipio)
        self.incluir_button.grid(row=2, column=1, padx=5, pady=5, sticky="e") 

        ttk.Label(parent, text="3. Municípios a filtrar:").grid(row=3, column=0, padx=5, pady=10, sticky="nw") 
        
        list_frame_mun = ttk.Frame(parent)
        list_frame_mun.grid(row=4, column=0, columnspan=2, padx=5, pady=5, sticky="nsew")
        list_frame_mun.columnconfigure(0, weight=1)
        list_frame_mun.rowconfigure(0, weight=1)
        
        self.scroll_mun = ttk.Scrollbar(list_frame_mun, orient=tk.VERTICAL)
        self.lista_municipios = tk.Listbox(list_frame_mun, yscrollcommand=self.scroll_mun.set, height=6)
        self.scroll_mun.config(command=self.lista_municipios.yview)
        self.scroll_mun.grid(row=0, column=1, sticky="ns")
        self.lista_municipios.grid(row=0, column=0, sticky="nsew")

        parent.rowconfigure(4, weight=1)

        button_frame = ttk.Frame(parent)
        button_frame.grid(row=5, column=0, columnspan=2, padx=5, pady=5, sticky="w")
        
        self.excluir_button = ttk.Button(button_frame, text="Excluir Último", command=self.excluir_ultimo_municipio)
        self.excluir_button.pack(side=tk.LEFT, padx=5)
        self.limpar_button = ttk.Button(button_frame, text="Limpar Lista", command=self.limpar_lista_municipios)
        self.limpar_button.pack(side=tk.LEFT, padx=5)

    def update_municipios(self, event=None):
        selected_uf = self.uf_var.get()
        municipios_filtrados = self.df_municipios[self.df_municipios['UF'] == selected_uf]
        lista_formatada = [row['Nome Municipio'] for _, row in municipios_filtrados.iterrows()]
        
        self.municipio_combo['values'] = sorted(lista_formatada)
        self.municipio_combo.set("") 
        self.municipio_combo.config(state="readonly")

    def incluir_municipio(self):
        municipio_selecionado = self.municipio_var.get()
        if not municipio_selecionado:
            messagebox.showwarning("Atenção", "Selecione um município no campo '2' primeiro.")
            return
        if municipio_selecionado in self.lista_municipios.get(0, tk.END):
            messagebox.showwarning("Atenção", f"O município '{municipio_selecionado}' já está na lista.")
            return
        self.lista_municipios.insert(tk.END, municipio_selecionado)
        self.municipio_combo.set("")

    def excluir_ultimo_municipio(self):
        if self.lista_municipios.size() > 0:
            self.lista_municipios.delete(tk.END)
        else:
            messagebox.showinfo("Informação", "A lista já está vazia.")

    def limpar_lista_municipios(self):
        self.lista_municipios.delete(0, tk.END)

    def start_filter_thread(self):
        itens_selecionados = self.lista_municipios.get(0, tk.END)
        if not itens_selecionados:
            messagebox.showerror("Erro", "Por favor, inclua pelo menos um município na lista '3' para filtrar.")
            return
            
        uf_selecionada = self.uf_var.get()
        if not uf_selecionada:
            messagebox.showerror("Erro", "Por favor, selecione a UF no passo '1'.")
            return
        
        municipios_alvo = {}
        for nome in itens_selecionados:
            codigo_completo = self.municipios_map.get((uf_selecionada, nome))
            if codigo_completo:
                codigo_limpo = re.sub(r'\D', '', str(codigo_completo))
                municipios_alvo[codigo_limpo] = nome.strip()
        
        cnae_codigos_limpos = list(CNAE_MAP.values())

        self.start_button.config(state="disabled")
        self.root.config(cursor="watch")
        
        # Inicia a thread híbrida
        filter_thread = threading.Thread(target=self.run_process_hibrido, args=(uf_selecionada, municipios_alvo, cnae_codigos_limpos))
        filter_thread.daemon = True
        filter_thread.start()

    def run_process_hibrido(self, uf_selecionada, municipios_alvo, cnae_codigos_limpos):
        try:
            municipios_dfs = {codigo_alvo: [] for codigo_alvo in municipios_alvo.keys()}

            nome_arquivo = MAPA_ARQUIVOS_UF.get(uf_selecionada)
            if not nome_arquivo:
                raise ValueError(f"UF '{uf_selecionada}' não possui um ficheiro mapeado.")

            arquivo_entrada = os.path.join(PASTA_DADOS, nome_arquivo)
            if not os.path.exists(arquivo_entrada):
                raise FileNotFoundError(f"Ficheiro não encontrado: {arquivo_entrada}")

            # Identifica o separador na primeira linha
            with open(arquivo_entrada, 'r', encoding=CODIFICACAO) as f:
                primeira_linha = f.readline()
                separador_real = ';' if ';' in primeira_linha else ','
                # Extrai os nomes das colunas exatamente como estão no cabeçalho
                colunas_brutas = primeira_linha.strip().split(separador_real)
                colunas_nomes = [c.replace('"', '').strip() for c in colunas_brutas]

            self.atualizar_status(f"A ler {arquivo_entrada} via PyArrow/C++...")

            # 1. LEITURA ULTRA-RÁPIDA (PyArrow Engine) COM ZERO-COPY PARA POLARS
            tipos_pyarrow = {col: pa.string() for col in colunas_nomes}
            
            read_options = pa_csv.ReadOptions(
                encoding=CODIFICACAO, # PyArrow suporta latin-1 nativamente e em paralelo
                column_names=colunas_nomes,
                skip_rows=1, # Pula o cabeçalho original
                block_size=25_000_000 # Blocos de 25MB (aprox. 150k linhas) = barra de progresso mais suave
            )
            parse_options = pa_csv.ParseOptions(delimiter=separador_real, quote_char='"')
            convert_options = pa_csv.ConvertOptions(column_types=tipos_pyarrow)

            try:
                reader = pa_csv.open_csv(
                    arquivo_entrada,
                    read_options=read_options,
                    parse_options=parse_options,
                    convert_options=convert_options
                )
            except Exception as e:
                raise ValueError(f"Erro ao inicializar o motor PyArrow: {e}")

            # --- INICIALIZAÇÃO DO LOG ---
            log_filename = os.path.join(PASTA_SAIDA, f"log_filtros_{uf_selecionada}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
            
            with open(log_filename, 'w', encoding='utf-8') as log_f:
                log_f.write("=== REGISTO DE DEPURAÇÃO (ALTA PERFORMANCE: PyArrow + Polars Zero-Copy) ===\n")
                log_f.write(f"Data/Hora: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                log_f.write(f"Ficheiro Lido: {arquivo_entrada}\n")
                log_f.write(f"\n--- O QUE ESTAMOS A PROCURAR ---\n")
                log_f.write(f"Municípios Alvo: {list(municipios_alvo.keys())}\n")
                log_f.write(f"CNAEs Alvo ({len(cnae_codigos_limpos)} itens): {cnae_codigos_limpos}\n")
                log_f.write("=========================================\n\n")

                for i, batch in enumerate(reader):
                    self.atualizar_status(f"A analisar Bloco {i+1} (Extração Zero-Copy)...")
                    
                    # Transformação Instantânea: Memória PyArrow C++ -> Polars Rust sem cópia de dados (O(1))
                    df_pl = pl.from_arrow(batch)
                    linhas_neste_bloco = len(df_pl)
                    
                    if i == 0:
                        colunas_necessarias = [COLUNA_MUNICIPIO, COLUNA_MUNICIPIO_TRAB, COLUNA_CNAE_SUBCLASSE, COLUNA_FILTRO_VINCULO, COLUNA_REMUNERACAO, COLUNA_GENERO, COLUNA_RACA]
                        colunas_ausentes = [c for c in colunas_necessarias if c not in df_pl.columns]
                        if colunas_ausentes:
                            raise ValueError(f"Faltam colunas obrigatórias no ficheiro:\n{colunas_ausentes}")

                    # 2. ACELERAÇÃO DA LIMPEZA E DOS FILTROS USANDO POLARS
                    # Expressões seguras do Polars (atualizado para Polars 1.0+ e limpeza robusta)
                    expr_mun_1 = pl.col(COLUNA_MUNICIPIO).cast(pl.String).str.replace_all(r"\D", "")
                    expr_mun_2 = pl.col(COLUNA_MUNICIPIO_TRAB).cast(pl.String).str.replace_all(r"\D", "")
                    expr_vinc = pl.col(COLUNA_FILTRO_VINCULO).cast(pl.String).str.replace_all('"', '').str.strip_chars().str.to_uppercase()
                    expr_cnae = pl.col(COLUNA_CNAE_SUBCLASSE).cast(pl.String).str.replace_all(r"\D", "")

                    # Limpa as colunas base
                    df_pl = df_pl.with_columns(
                        expr_mun_1.alias("Mun_Limpo_1"),
                        expr_mun_2.alias("Mun_Limpo_2"),
                        expr_vinc.alias("Vinculo_Filtro"),
                        expr_cnae.str.slice(0, 5).alias("CNAE_Filtro")
                    )

                    # Lógica de Fallback de Município
                    df_pl = df_pl.with_columns(
                        pl.when(pl.col("Mun_Limpo_1").is_in(list(municipios_alvo.keys())))
                        .then(pl.col("Mun_Limpo_1"))
                        .otherwise(pl.col("Mun_Limpo_2"))
                        .alias("Mun_Filtro")
                    )

                    # Criação e Soma das Máscaras para o Arquivo de Log Exato
                    df_pl = df_pl.with_columns(
                        pl.col("Mun_Limpo_1").is_in(list(municipios_alvo.keys())).alias("pass_mun_1"),
                        pl.col("Mun_Limpo_2").is_in(list(municipios_alvo.keys())).alias("pass_mun_2"),
                        pl.col("CNAE_Filtro").is_in(cnae_codigos_limpos).alias("pass_cnae"),
                        pl.col("Vinculo_Filtro").is_in(['1', '01', 'SIM', 'S']).alias("pass_vinc")
                    ).with_columns(
                        (pl.col("pass_mun_1") | pl.col("pass_mun_2")).alias("pass_mun_total")
                    ).with_columns(
                        (pl.col("pass_mun_total") & pl.col("pass_cnae") & pl.col("pass_vinc")).alias("pass_final")
                    )

                    # Extrai os totais para o Log
                    total_mun_1 = df_pl["pass_mun_1"].sum()
                    total_mun_2 = df_pl["pass_mun_2"].sum()
                    total_mun_total = df_pl["pass_mun_total"].sum()
                    total_cnae = df_pl["pass_cnae"].sum()
                    total_vinc = df_pl["pass_vinc"].sum()
                    total_all = df_pl["pass_final"].sum()

                    log_f.write(f"--- BLOCO {i+1} ---\n")
                    log_f.write(f"Total de linhas processadas: {linhas_neste_bloco:,}\n")
                    log_f.write(f"  > Passaram no filtro Município (Sede):     {total_mun_1:,}\n")
                    log_f.write(f"  > Passaram no filtro Município (Trabalho): {total_mun_2:,}\n")
                    log_f.write(f"  > Total Único passando no filtro Mun:      {total_mun_total:,}\n")
                    log_f.write(f"  > Passaram no filtro CNAE:                 {total_cnae:,}\n")
                    log_f.write(f"  > Passaram no filtro Vínculo:              {total_vinc:,}\n")
                    log_f.write(f"  > VÍNCULOS GUARDADOS (Cruzamento):         {total_all:,}\n")

                    if i == 0:
                        log_f.write("\n[AMOSTRA DE COMO O POLARS LIMPOU OS DADOS NO PRIMEIRO BLOCO]\n")
                        amostra = df_pl.head(10).to_pandas()
                        log_f.write("Amostra Municípios (Lido Sede | Lido Trab -> Como o filtro converte):\n")
                        for _, r in amostra.iterrows():
                            log_f.write(f"  {r.get(COLUNA_MUNICIPIO, '')} | {r.get(COLUNA_MUNICIPIO_TRAB, '')} ---> Filtro Final: {r.get('Mun_Filtro', '')}\n")
                        log_f.write("-" * 50 + "\n\n")

                    # Aplica o filtro final e limpa colunas auxiliares
                    df_filtrado_pl = df_pl.filter(pl.col("pass_final"))
                    cols_to_drop = ['pass_mun_1', 'pass_mun_2', 'pass_cnae', 'pass_vinc', 'pass_mun_total', 'pass_final']
                    df_filtrado_pl = df_filtrado_pl.drop(cols_to_drop)

                    if len(df_filtrado_pl) > 0:
                        # 3. Retorna para o Pandas para agrupar e salvar (apenas com as linhas que passaram no filtro)
                        resultado_bloco = df_filtrado_pl.to_pandas()
                        resultado_bloco[COLUNA_CNAE_CLASSE] = resultado_bloco['CNAE_Filtro']
                        
                        grouped = resultado_bloco.groupby('Mun_Filtro')
                        for codigo_encontrado, grupo_df in grouped:
                            if codigo_encontrado in municipios_dfs:
                                municipios_dfs[codigo_encontrado].append(grupo_df)

            self.atualizar_status("Extração concluída. A preparar relatórios em Excel...")
            msg_final = "Processamento Concluído!\n\nResultados:\n"
            dfs_agregados = []

            for codigo_mun, lista_dfs in municipios_dfs.items():
                nome_mun = municipios_alvo[codigo_mun]
                nome_arquivo_base = re.sub(r'[\\/*?:"<>|]', '', nome_mun)
                
                if lista_dfs:
                    df_final_mun = pd.concat(lista_dfs, ignore_index=True)
                    
                    colunas_temp = ['Mun_Filtro', 'Vinculo_Filtro', 'CNAE_Filtro', 'CNAE_Subclasse_Limpo', 'Mun_Limpo_1', 'Mun_Limpo_2']
                    df_final_mun = df_final_mun.drop(columns=[col for col in colunas_temp if col in df_final_mun.columns])
                    
                    linhas_salvas = len(df_final_mun)
                    df_final_mun = df_final_mun.sort_values(by=COLUNA_CNAE_CLASSE)
                    caminho_csv = os.path.join(PASTA_SAIDA, f"{nome_arquivo_base}.csv")
                    df_final_mun.to_csv(caminho_csv, index=False, sep=';', encoding=CODIFICACAO)
                    
                    self.atualizar_status(f"A criar relatório Excel para: {nome_mun}...")
                    processar_dataframe_para_relatorio(df_final_mun, nome_arquivo_base)
                    
                    dfs_agregados.append(df_final_mun)
                    msg_final += f"✔ {nome_mun}: {linhas_salvas:,} vínculos.\n"
                else:
                    msg_final += f"✖ {nome_mun}: Nenhum vínculo encontrado.\n"

            if len(dfs_agregados) > 1:
                self.atualizar_status("A agregar dados para Relatório Regional...")
                df_regional = pd.concat(dfs_agregados, ignore_index=True)
                df_regional = df_regional.sort_values(by=COLUNA_CNAE_CLASSE)
                caminho_csv_regional = os.path.join(PASTA_SAIDA, "Regional.csv")
                df_regional.to_csv(caminho_csv_regional, index=False, sep=';', encoding=CODIFICACAO)
                processar_dataframe_para_relatorio(df_regional, "Regional")
                msg_final += f"\n✔ Relatório Regional gerado com {len(df_regional):,} vínculos.\n"

            msg_final += f"\nℹ️ O Arquivo de Log detalhado foi guardado na pasta '{PASTA_SAIDA}'."
            self.atualizar_status("Processo totalmente finalizado!")
            self.atualizar_interface(messagebox.showinfo, "Sucesso", msg_final)

        except Exception as e:
            erro_traceback = traceback.format_exc()
            self.atualizar_status("Ocorreu um erro crítico. Verifique o registo.")
            self.atualizar_interface(messagebox.showerror, "Erro Fatal", f"Ocorreu um erro inesperado:\n{str(e)}\n\nDetalhes:\n{erro_traceback}")
        finally:
            self.atualizar_interface(self.start_button.config, state="normal")
            self.atualizar_interface(self.root.config, cursor="")

# ==========================================
# --- INICIALIZAÇÃO DA APLICAÇÃO ---
# ==========================================
if __name__ == "__main__":
    try:
        df_municipios = pd.read_csv(
            ARQUIVO_MUNICIPIOS, delimiter=';', dtype=str, encoding='utf-8'
        )
        df_municipios.columns = [col.strip().strip(',') for col in df_municipios.columns] 

        ufs = sorted(df_municipios['UF'].unique())
        municipios_map = df_municipios.set_index(['UF', 'Nome Municipio'])['Codigo Municipio'].to_dict()

        root = tk.Tk()
        app = App(root, ufs, df_municipios, municipios_map)
        root.mainloop()

    except Exception as e:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Erro Fatal", f"Ocorreu um erro crítico ao iniciar:\n{e}")
        root.destroy()