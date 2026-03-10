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
import json
import sys

# ==========================================
# --- CARREGAMENTO DE CONFIGURAÇÕES ---
# ==========================================
try:
    with open('config.json', 'r', encoding='utf-8') as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    print("ERRO FATAL: O ficheiro 'config.json' não foi encontrado na pasta do script.")
    sys.exit(1)
except json.JSONDecodeError as e:
    print(f"ERRO FATAL: O ficheiro 'config.json' está mal formatado ou corrompido.\nDetalhes: {e}")
    sys.exit(1)

# Configurações Globais
CODIFICACAO = CONFIG['configuracoes_globais']['codificacao']
PASTA_DADOS = CONFIG['configuracoes_globais']['pasta_dados']
PASTA_SAIDA = CONFIG['configuracoes_globais']['pasta_saida']
ARQUIVO_MUNICIPIOS = os.path.join(PASTA_DADOS, CONFIG['configuracoes_globais']['arquivo_municipios'])
TAMANHO_BLOCO = CONFIG['configuracoes_globais']['tamanho_bloco']

# Configuração Visual do Excel
EXCEL_CONFIG = CONFIG.get('excel_config')
if not EXCEL_CONFIG:
    print("ERRO FATAL: O bloco 'excel_config' não foi encontrado no 'config.json'.")
    sys.exit(1)

# Colunas Alvo
COLUNA_MUNICIPIO = CONFIG['colunas_alvo']['municipio']
COLUNA_MUNICIPIO_TRAB = CONFIG['colunas_alvo']['municipio_trab']
COLUNA_CNAE_CLASSE = CONFIG['colunas_alvo']['cnae_classe']
COLUNA_CNAE_SUBCLASSE = CONFIG['colunas_alvo']['cnae_subclasse']
COLUNA_FILTRO_VINCULO = CONFIG['colunas_alvo']['filtro_vinculo']
COLUNA_REMUNERACAO = CONFIG['colunas_alvo']['remuneracao']
COLUNA_GENERO = CONFIG['colunas_alvo']['genero']
COLUNA_RACA = CONFIG['colunas_alvo']['raca']

# Mapas de Dados
MAPA_GENERO = CONFIG['mapas']['genero']
MAPA_RACA = CONFIG['mapas']['raca']
CNAE_MAP = CONFIG['cnae_map']
CNAE_ESTRUTURA = CONFIG['cnae_estrutura']
REGIOES_ARQUIVOS = CONFIG['regioes_arquivos']

# Gera o mapeamento reverso de UFs
MAPA_ARQUIVOS_UF = {uf: arquivo for arquivo, ufs in REGIOES_ARQUIVOS.items() for uf in ufs}

# Cria diretório de saída se não existir
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

def salvar_relatorio_multiplo_excel(relatorios: dict, caminho_arquivo: str):
    caminho_completo = os.path.join(PASTA_SAIDA, caminho_arquivo)
    try:
        excel_kwargs = {'options': {'nan_inf_to_errors': True}}
        with pd.ExcelWriter(caminho_completo, engine='xlsxwriter', engine_kwargs=excel_kwargs) as writer:
            workbook = writer.book
            
            # --- CARREGA CONFIGURAÇÕES DO JSON ---
            cfg = EXCEL_CONFIG
            
            font_base = {'font_name': cfg['fonte_padrao']}
            
            # Formatos Globais Numéricos
            fmt_money = {'num_format': cfg['formatos_numericos']['dinheiro'], 'align': 'right'}
            fmt_percent = {'num_format': cfg['formatos_numericos']['percentual'], 'align': 'right'}
            fmt_number = {'num_format': cfg['formatos_numericos']['inteiro'], 'align': 'right'}
            fmt_string_r = {'align': 'right'}
            
            align_c = {'align': 'center'}
            align_l = {'align': 'left'}
            
            # Estilos de Hierarquia (L0 a L3)
            style_l0 = cfg['estilos_niveis']['l0']
            style_l1 = cfg['estilos_niveis']['l1']
            style_l2 = cfg['estilos_niveis']['l2']
            style_l3 = cfg['estilos_niveis']['l3']

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
            
            # Formato do Cabeçalho puxado do JSON
            header_format = workbook.add_format({**font_base, **cfg['estilo_cabecalho']})
            larguras = cfg['largura_colunas_pixels']

            # Varre o dicionário de relatórios gerando uma aba para cada um
            for nome_aba, df_final in relatorios.items():
                # Higieniza o nome da aba e limita a 31 caracteres (regra do Excel)
                nome_aba_seguro = re.sub(r'[\\/*?:"<>|]', '', nome_aba)[:31]
                
                worksheet = workbook.add_worksheet(nome_aba_seguro)
                
                # Aplica as margens configuradas no JSON
                worksheet.set_margins(**cfg['margens'])

                # Aplica larguras configuradas no JSON
                worksheet.set_column_pixels('A:A', larguras.get('A', 10), wrap_c)
                worksheet.set_column_pixels('B:B', larguras.get('B', 50), wrap_c)
                worksheet.set_column_pixels('C:C', larguras.get('C', 50), wrap_c)
                worksheet.set_column_pixels('D:D', larguras.get('D', 50), wrap_l)
                worksheet.set_column_pixels('E:E', larguras.get('E', 650), wrap_l)
                worksheet.set_column_pixels('F:F', larguras.get('F', 75), number_default)
                worksheet.set_column_pixels('G:G', larguras.get('G', 110), money_default)
                worksheet.set_column_pixels('H:O', larguras.get('H_O', 75), percent_default)

                headers = list(df_final.columns)
                headers[0:5] = [''] * 5 
                
                worksheet.write_row('A1', headers, header_format)
                worksheet.set_row_pixels(0, cfg.get('altura_cabecalho', 20))
                
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

        print(f"Relatório consolidado salvo em: {caminho_completo}")
        
    except Exception as e:
        print(f"Erro ao salvar ficheiro Excel {caminho_completo}: {e}")

def gerar_dataframe_relatorio(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

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

    return pd.DataFrame(dados_relatorio_final)


# ==========================================
# --- INTERFACE GRÁFICA E MOTOR HÍBRIDO ---
# ==========================================

class App:
    def __init__(self, root, ufs, df_municipios, municipios_map):
        self.root = root
        self.ufs = ufs
        self.df_municipios = df_municipios
        self.municipios_map = municipios_map

        self.root.title("Extrator e Gerador RAIS (PyArrow + Polars)")
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
        if not selected_uf:
            return
            
        municipios_filtrados = self.df_municipios[self.df_municipios['UF'] == selected_uf]
        lista_formatada = [row['Nome Municipio'] for _, row in municipios_filtrados.iterrows()]
        
        self.municipio_combo['values'] = sorted(lista_formatada)
        self.municipio_combo.set("") 
        self.municipio_combo.config(state="readonly")

    def incluir_municipio(self):
        uf_selecionada = self.uf_var.get()
        municipio_selecionado = self.municipio_var.get()
        
        if not uf_selecionada or not municipio_selecionado:
            messagebox.showwarning("Atenção", "Selecione uma UF e um município primeiro.")
            return
            
        # Formata a entrada para identificar de qual UF vem o município
        item_formatado = f"{uf_selecionada} - {municipio_selecionado}"
        
        if item_formatado in self.lista_municipios.get(0, tk.END):
            messagebox.showwarning("Atenção", f"O município '{municipio_selecionado}' ({uf_selecionada}) já está na lista.")
            return
            
        self.lista_municipios.insert(tk.END, item_formatado)
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
            
        ufs_envolvidas = set()
        municipios_alvo = {}
        
        # Desempacota a UF e o Nome da cidade selecionados
        for item in itens_selecionados:
            try:
                uf, nome = item.split(" - ", 1)
                ufs_envolvidas.add(uf)
                codigo_completo = self.municipios_map.get((uf, nome))
                if codigo_completo:
                    codigo_limpo = re.sub(r'\D', '', str(codigo_completo))
                    municipios_alvo[codigo_limpo] = nome.strip()
            except ValueError:
                continue
        
        if not ufs_envolvidas or not municipios_alvo:
            messagebox.showerror("Erro", "Não foi possível mapear os códigos IBGE dos municípios selecionados.")
            return
            
        cnae_codigos_limpos = list(CNAE_MAP.values())

        self.start_button.config(state="disabled")
        self.root.config(cursor="watch")
        
        # Inicia a thread híbrida passando o conjunto de UFs e os alvos
        filter_thread = threading.Thread(target=self.run_process_hibrido, args=(ufs_envolvidas, municipios_alvo, cnae_codigos_limpos))
        filter_thread.daemon = True
        filter_thread.start()

    def run_process_hibrido(self, ufs_envolvidas, municipios_alvo, cnae_codigos_limpos):
        try:
            municipios_dfs = {codigo_alvo: [] for codigo_alvo in municipios_alvo.keys()}

            # Identifica quais arquivos base precisam ser lidos com base nas UFs
            arquivos_alvo_nomes = set()
            for uf in ufs_envolvidas:
                nome_arquivo = MAPA_ARQUIVOS_UF.get(uf)
                if nome_arquivo:
                    arquivos_alvo_nomes.add(nome_arquivo)
                else:
                    raise ValueError(f"UF '{uf}' não possui um ficheiro mapeado nas configurações.")

            # Gera uma string representativa das UFs para nomear os arquivos de saída
            lista_ufs_ordenada = sorted(list(ufs_envolvidas))
            ufs_str = "_".join(lista_ufs_ordenada)
            if len(ufs_str) > 15: # Evita nomes gigantescos se o usuário selecionar todo o Brasil
                ufs_str = "Multiplas_UFs"

            # --- INICIALIZAÇÃO DO LOG UNIFICADO ---
            log_filename = os.path.join(PASTA_SAIDA, f"log_filtros_{ufs_str}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
            
            with open(log_filename, 'w', encoding='utf-8') as log_f:
                log_f.write("=== REGISTO DE DEPURAÇÃO (ALTA PERFORMANCE: PyArrow + Polars Zero-Copy) ===\n")
                log_f.write(f"Data/Hora: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                log_f.write(f"UFs Envolvidas: {', '.join(lista_ufs_ordenada)}\n")
                log_f.write(f"Total de Arquivos a processar: {len(arquivos_alvo_nomes)}\n")
                log_f.write(f"\n--- O QUE ESTAMOS A PROCURAR ---\n")
                log_f.write(f"Municípios Alvo ({len(municipios_alvo)} itens): {list(municipios_alvo.keys())}\n")
                log_f.write(f"CNAEs Alvo ({len(cnae_codigos_limpos)} itens): {cnae_codigos_limpos}\n")
                log_f.write("=========================================\n\n")

                total_geral_linhas_lidas = 0
                total_geral_linhas_salvas = 0

                # Itera sobre todos os arquivos necessários para cobrir as UFs solicitadas
                for index_arq, nome_arquivo in enumerate(arquivos_alvo_nomes, 1):
                    arquivo_entrada = os.path.join(PASTA_DADOS, nome_arquivo)
                    if not os.path.exists(arquivo_entrada):
                        raise FileNotFoundError(f"Ficheiro não encontrado: {arquivo_entrada}")

                    # Identifica o separador na primeira linha
                    with open(arquivo_entrada, 'r', encoding=CODIFICACAO) as f:
                        primeira_linha = f.readline()
                        separador_real = ';' if ';' in primeira_linha else ','
                        colunas_brutas = primeira_linha.strip().split(separador_real)
                        colunas_nomes = [c.replace('"', '').strip() for c in colunas_brutas]

                    self.atualizar_status(f"[{index_arq}/{len(arquivos_alvo_nomes)}] A preparar motor PyArrow para: {nome_arquivo}")
                    log_f.write(f"\n>>>>> INICIANDO PROCESSAMENTO DO ARQUIVO: {nome_arquivo} <<<<<\n\n")

                    tipos_pyarrow = {col: pa.string() for col in colunas_nomes}
                    
                    read_options = pa_csv.ReadOptions(encoding=CODIFICACAO, column_names=colunas_nomes, skip_rows=1, block_size=TAMANHO_BLOCO)
                    parse_options = pa_csv.ParseOptions(delimiter=separador_real, quote_char='"')
                    convert_options = pa_csv.ConvertOptions(column_types=tipos_pyarrow)

                    try:
                        reader = pa_csv.open_csv(arquivo_entrada, read_options=read_options, parse_options=parse_options, convert_options=convert_options)
                    except Exception as e:
                        raise ValueError(f"Erro ao inicializar o motor PyArrow para {nome_arquivo}: {e}")

                    for i, batch in enumerate(reader):
                        self.atualizar_status(f"[{index_arq}/{len(arquivos_alvo_nomes)}] {nome_arquivo} -> A analisar Bloco {i+1}...")
                        
                        df_pl = pl.from_arrow(batch)
                        linhas_neste_bloco = len(df_pl)
                        total_geral_linhas_lidas += linhas_neste_bloco
                        
                        if i == 0:
                            colunas_necessarias = [COLUNA_MUNICIPIO, COLUNA_MUNICIPIO_TRAB, COLUNA_CNAE_SUBCLASSE, COLUNA_FILTRO_VINCULO, COLUNA_REMUNERACAO, COLUNA_GENERO, COLUNA_RACA]
                            colunas_ausentes = [c for c in colunas_necessarias if c not in df_pl.columns]
                            if colunas_ausentes:
                                raise ValueError(f"Faltam colunas obrigatórias no ficheiro {nome_arquivo}:\n{colunas_ausentes}")

                        expr_mun_1 = pl.col(COLUNA_MUNICIPIO).cast(pl.String).str.replace_all(r"\D", "")
                        expr_mun_2 = pl.col(COLUNA_MUNICIPIO_TRAB).cast(pl.String).str.replace_all(r"\D", "")
                        expr_vinc = pl.col(COLUNA_FILTRO_VINCULO).cast(pl.String).str.replace_all('"', '').str.strip_chars().str.to_uppercase()
                        expr_cnae = pl.col(COLUNA_CNAE_SUBCLASSE).cast(pl.String).str.replace_all(r"\D", "")

                        df_pl = df_pl.with_columns(
                            expr_mun_1.alias("Mun_Limpo_1"),
                            expr_mun_2.alias("Mun_Limpo_2"),
                            expr_vinc.alias("Vinculo_Filtro"),
                            expr_cnae.str.slice(0, 5).alias("CNAE_Filtro")
                        )

                        df_pl = df_pl.with_columns(
                            pl.when(pl.col("Mun_Limpo_1").is_in(list(municipios_alvo.keys())))
                            .then(pl.col("Mun_Limpo_1"))
                            .otherwise(pl.col("Mun_Limpo_2"))
                            .alias("Mun_Filtro")
                        )

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

                        total_mun_1 = df_pl["pass_mun_1"].sum()
                        total_mun_2 = df_pl["pass_mun_2"].sum()
                        total_mun_total = df_pl["pass_mun_total"].sum()
                        total_cnae = df_pl["pass_cnae"].sum()
                        total_vinc = df_pl["pass_vinc"].sum()
                        total_all = df_pl["pass_final"].sum()
                        
                        total_geral_linhas_salvas += total_all

                        log_f.write(f"--- BLOCO {i+1} ---\n")
                        log_f.write(f"Linhas processadas: {linhas_neste_bloco:,}\n")
                        log_f.write(f"  > Passaram no filtro Mun (Total Único): {total_mun_total:,}\n")
                        log_f.write(f"  > Passaram no filtro CNAE:              {total_cnae:,}\n")
                        log_f.write(f"  > Passaram no filtro Vínculo:           {total_vinc:,}\n")
                        log_f.write(f"  > VÍNCULOS GUARDADOS (Cruzamento):      {total_all:,}\n")

                        df_filtrado_pl = df_pl.filter(pl.col("pass_final"))
                        cols_to_drop = ['pass_mun_1', 'pass_mun_2', 'pass_cnae', 'pass_vinc', 'pass_mun_total', 'pass_final']
                        df_filtrado_pl = df_filtrado_pl.drop(cols_to_drop)

                        if len(df_filtrado_pl) > 0:
                            resultado_bloco = df_filtrado_pl.to_pandas()
                            resultado_bloco[COLUNA_CNAE_CLASSE] = resultado_bloco['CNAE_Filtro']
                            
                            grouped = resultado_bloco.groupby('Mun_Filtro')
                            for codigo_encontrado, grupo_df in grouped:
                                if codigo_encontrado in municipios_dfs:
                                    municipios_dfs[codigo_encontrado].append(grupo_df)

                log_f.write("\n=========================================\n")
                log_f.write(f"RESUMO GERAL DO PROCESSAMENTO:\n")
                log_f.write(f"Total Global de Linhas Varridas: {total_geral_linhas_lidas:,}\n")
                log_f.write(f"Total Global de Vínculos Extratos: {total_geral_linhas_salvas:,}\n")
                log_f.write("=========================================\n")

            self.atualizar_status("Extração de todos os arquivos concluída. A preparar relatórios...")
            msg_final = "Processamento Concluído!\n\nResultados:\n"
            dfs_brutos_agregados = []
            relatorios_para_excel = {}

            # 1. Salvar os CSVs individuais e guardar na memória para o Excel
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
                    
                    dfs_brutos_agregados.append((nome_mun, df_final_mun))
                    msg_final += f"✔ {nome_mun}: {linhas_salvas:,} vínculos.\n"
                else:
                    msg_final += f"✖ {nome_mun}: Nenhum vínculo encontrado.\n"

            if dfs_brutos_agregados:
                self.atualizar_status("A compilar dados para as abas do Excel...")

                # 2. Processar a Aba Regional PRIMEIRO (Para ser a primeira aba do arquivo)
                if len(dfs_brutos_agregados) > 1:
                    df_regional = pd.concat([df for _, df in dfs_brutos_agregados], ignore_index=True)
                    df_regional = df_regional.sort_values(by=COLUNA_CNAE_CLASSE)
                    caminho_csv_regional = os.path.join(PASTA_SAIDA, f"Regional_{ufs_str}.csv")
                    df_regional.to_csv(caminho_csv_regional, index=False, sep=';', encoding=CODIFICACAO)
                    
                    df_regional_formatado = gerar_dataframe_relatorio(df_regional)
                    if not df_regional_formatado.empty:
                        relatorios_para_excel["Regional"] = df_regional_formatado
                    msg_final += f"\n✔ Relatório Regional gerado com {len(df_regional):,} vínculos.\n"

                # 3. Processar as Abas das Cidades Individuais
                for nome_mun, df_mun in dfs_brutos_agregados:
                    df_mun_formatado = gerar_dataframe_relatorio(df_mun)
                    if not df_mun_formatado.empty:
                        relatorios_para_excel[nome_mun] = df_mun_formatado

                # 4. Salvar tudo num único arquivo Excel consolidado
                if relatorios_para_excel:
                    self.atualizar_status("A guardar arquivo Excel consolidado...")
                    nome_excel_final = f"Relatorio_Consolidado_{ufs_str}.xlsx"
                    salvar_relatorio_multiplo_excel(relatorios_para_excel, nome_excel_final)

            msg_final += f"\nℹ️ O Arquivo Excel Consolidado e o Log foram guardados na pasta '{PASTA_SAIDA}'."
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