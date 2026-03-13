import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
import threading
import os
import re
import numpy as np
import traceback
import datetime
import json
import duckdb
import sys
from pathlib import Path

# ==============================================================================
# --- CARREGAMENTO DE CONFIGURAÇÕES (JSON) ---
# ==============================================================================

ARQUIVO_CONFIG = Path("config.json")

def carregar_configuracoes():
    """Lê as configurações parametrizadas no arquivo JSON externo."""
    if not ARQUIVO_CONFIG.exists():
        raise FileNotFoundError(
            f"Arquivo de configuração '{ARQUIVO_CONFIG}' não encontrado! "
            "Certifique-se de que ele está na mesma pasta que o script."
        )
    
    with open(ARQUIVO_CONFIG, 'r', encoding='utf-8') as f:
        return json.load(f)

# Inicializa as variáveis globais a partir do JSON para facilitar o uso no código
try:
    CONFIG = carregar_configuracoes()
except Exception as e:
    print(f"Erro ao carregar o arquivo JSON: {e}")
    sys.exit(1)

C_GLOB = CONFIG['configuracoes_globais']
C_COLS = CONFIG['colunas_alvo']
C_EXCEL = CONFIG['excel_config']

PASTA_DADOS = Path(C_GLOB['pasta_dados']).expanduser()
PASTA_SAIDA = Path(C_GLOB['pasta_saida'])
ARQUIVO_MUNICIPIOS = PASTA_DADOS / C_GLOB['arquivo_municipios']

# Mapeamento dinâmico de UFs para arquivos Parquet
MAPA_ARQUIVOS_UF = {}
for arquivo, ufs in CONFIG['regioes_arquivos'].items():
    arquivo_parquet = Path(arquivo).with_suffix('.parquet').name
    for uf in ufs:
        MAPA_ARQUIVOS_UF[uf] = arquivo_parquet

PASTA_SAIDA.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# --- MAPA DE COLUNAS E FORMATAÇÕES ---
# ==============================================================================

# O dicionário 'cabecalho_duplo' foi movido integralmente para o config.json
# para facilitar futuras manutenções e personalizações de layout.

DATA_COLS_MAP = [
    ('Total Vínculos', 'int'), ('Remuneração Média', 'money'), ('Homens', 'percent'), ('Mulheres', 'percent'),
    ('Branca', 'percent'), ('Preta', 'percent'), ('Parda', 'percent'), ('Amarela', 'percent'), ('Indígena', 'percent'), ('NI', 'percent'),
    ('Até 18', 'percent'), ('18-29', 'percent'), ('30-39', 'percent'), ('40-49', 'percent'), ('50-65', 'percent'), ('Acima de 65', 'percent'),
    ('Até 6 meses', 'percent'), ('De 6 Meses a 1 Ano', 'percent'), ('De 1 a 2 Anos', 'percent'), ('De 2 a 5 Anos', 'percent'), ('De 5 a 10 Anos', 'percent'), ('Acima de 10 Anos', 'percent'),
    ('Fund', 'percent'), ('MedInc', 'percent'), ('MedComp', 'percent'), ('SupInc', 'percent'), ('SupComp', 'percent'), ('Pos', 'percent'),
    ('Nenhum Afastamento', 'percent'), ('Um', 'percent'), ('Aci_1', 'percent'), ('Dias_1', 'float'),
    ('Dois', 'percent'), ('Aci_2', 'percent'), ('Dias_2', 'float'),
    ('Três ou Mais', 'percent'), ('Aci_3', 'percent'), ('Dias_3', 'float'),
    ('Até 30H', 'percent'), ('De 30H a 40H', 'percent'), ('Acima de 40H', 'percent'),
    ('Intermitente', 'percent'), ('Parcial', 'percent')
]

# ==============================================================================
# --- FUNÇÕES DE PROCESSAMENTO E RELATÓRIO ---
# ==============================================================================

def calcular_metricas(group: pd.DataFrame) -> pd.Series:
    """Calcula estatísticas avançadas baseadas nas novas métricas RAIS."""
    total_vinculos = len(group)
    
    if total_vinculos == 0:
        vazio = {col: ('-' if fmt in ['money', 'float', 'percent'] else 0) for col, fmt in DATA_COLS_MAP}
        vazio['Total Vínculos'] = 0
        return pd.Series(vazio)

    # 1. Remuneração e Demografia Básica
    media_remun = group['Remuneracao_Num'].mean(skipna=True)
    media_remun = round(media_remun, 2) if pd.notna(media_remun) else np.nan
    
    pct_homens = group['Genero_Limpo'].eq('Masculino').sum() / total_vinculos
    pct_mulheres = group['Genero_Limpo'].eq('Feminino').sum() / total_vinculos
    
    c_raca = group['Raca_Limpa'].value_counts(dropna=False)
    pct_ni = (c_raca.get('NAO IDENT', 0) + c_raca.get('IGNORADO', 0)) / total_vinculos

    # 2. Faixa Etária (Anos)
    c_idade = pd.cut(group['Idade_Num'], bins=[-1, 18, 29, 39, 49, 65, 999], 
                     labels=['Até 18', '18-29', '30-39', '40-49', '50-65', 'Acima de 65']).value_counts(dropna=False)
    
    # 3. Tempo no Emprego (Meses)
    c_tempo = pd.cut(group['Tempo_Emprego_Num'], bins=[-1, 6, 12, 24, 60, 120, 99999], 
                     labels=['Até 6 meses', 'De 6 Meses a 1 Ano', 'De 1 a 2 Anos', 'De 2 a 5 Anos', 'De 5 a 10 Anos', 'Acima de 10 Anos']).value_counts(dropna=False)

    # 4. Escolaridade
    map_esc = {1: 'Fund', 2: 'Fund', 3: 'Fund', 4: 'Fund', 5: 'Fund', 
               6: 'MedInc', 7: 'MedComp', 8: 'SupInc', 9: 'SupComp', 10: 'Pos', 11: 'Pos'}
    c_esc = group['Escolaridade_Num'].map(map_esc).value_counts(dropna=False)

    # 5. Afastamentos (Lógica simples com strings, usando 999 para nenhum afastamento)
    c1 = group['Causa_Afast_1'].astype(str).str.strip()
    c2 = group['Causa_Afast_2'].astype(str).str.strip()
    c3 = group['Causa_Afast_3'].astype(str).str.strip()
    
    aci_codes = ['10', '10.0', '20', '20.0', '30', '30.0', '90', '90.0']
    is_aci = c1.isin(aci_codes) | c2.isin(aci_codes) | c3.isin(aci_codes)
    
    # 999 = Nenhum afastamento. Adicionamos nulos e 0 por segurança contra dados sujos.
    nao_afastado = ['999', '999.0', '999.00', 'nan', 'None', '', '-1', '-1.0', '0', '0.0']
    
    count_afast = (~c1.isin(nao_afastado)).astype(int) + \
                  (~c2.isin(nao_afastado)).astype(int) + \
                  (~c3.isin(nao_afastado)).astype(int)
    
    afast_nenhum = (count_afast == 0).sum() / total_vinculos
    
    # Cálculos por incidência reajustados à nova lógica
    def estatisticas_afast(mask):
        # Percentual de pessoas nesta faixa / total de vínculos
        pct = mask.sum() / total_vinculos
        
        # Percentual de pessoas nesta faixa que tiveram acidente / total de vínculos
        aci = (is_aci & mask).sum() / total_vinculos
        
        # Média de dias ausentes SOMENTE para os trabalhadores que estão nesta faixa
        dias = pd.to_numeric(group.loc[mask, 'Dias_Afast_Num'], errors='coerce').mean() if mask.any() else 0
        
        return pct, aci, dias

    afast_1_pct, afast_1_aci, afast_1_dias = estatisticas_afast(count_afast == 1)
    afast_2_pct, afast_2_aci, afast_2_dias = estatisticas_afast(count_afast == 2)
    afast_3_pct, afast_3_aci, afast_3_dias = estatisticas_afast(count_afast >= 3)

    # 6. Jornada Contratada (Horas)
    c_jor = pd.cut(group['Jornada_Num'], bins=[-1, 30, 40, 999], 
                   labels=['Até 30H', 'De 30H a 40H', 'Acima de 40H']).value_counts(dropna=False)

    # 7. Marcadores (Intermitente e Parcial)
    val_positivos = ['1', '01', 'SIM', 'S', 'Sim']
    ind_int = group['Ind_Intermitente'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    ind_par = group['Ind_Parcial'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    
    inter_pct = ind_int.isin(val_positivos).sum() / total_vinculos
    parcial_pct = ind_par.isin(val_positivos).sum() / total_vinculos

    return pd.Series({
        'Total Vínculos': total_vinculos, 
        'Remuneração Média': media_remun, 
        'Homens': pct_homens, 
        'Mulheres': pct_mulheres,
        'Branca': c_raca.get('BRANCA', 0) / total_vinculos, 
        'Preta': c_raca.get('PRETA', 0) / total_vinculos, 
        'Parda': c_raca.get('PARDA', 0) / total_vinculos, 
        'Amarela': c_raca.get('AMARELA', 0) / total_vinculos, 
        'Indígena': c_raca.get('INDIGENA', 0) / total_vinculos, 
        'NI': pct_ni,
        
        'Até 18': c_idade.get('Até 18', 0) / total_vinculos, 
        '18-29': c_idade.get('18-29', 0) / total_vinculos, 
        '30-39': c_idade.get('30-39', 0) / total_vinculos, 
        '40-49': c_idade.get('40-49', 0) / total_vinculos, 
        '50-65': c_idade.get('50-65', 0) / total_vinculos, 
        'Acima de 65': c_idade.get('Acima de 65', 0) / total_vinculos,
        
        'Até 6 meses': c_tempo.get('Até 6 meses', 0) / total_vinculos, 
        'De 6 Meses a 1 Ano': c_tempo.get('De 6 Meses a 1 Ano', 0) / total_vinculos, 
        'De 1 a 2 Anos': c_tempo.get('De 1 a 2 Anos', 0) / total_vinculos, 
        'De 2 a 5 Anos': c_tempo.get('De 2 a 5 Anos', 0) / total_vinculos, 
        'De 5 a 10 Anos': c_tempo.get('De 5 a 10 Anos', 0) / total_vinculos, 
        'Acima de 10 Anos': c_tempo.get('Acima de 10 Anos', 0) / total_vinculos,
        
        'Fund': c_esc.get('Fund', 0) / total_vinculos, 
        'MedInc': c_esc.get('MedInc', 0) / total_vinculos, 
        'MedComp': c_esc.get('MedComp', 0) / total_vinculos, 
        'SupInc': c_esc.get('SupInc', 0) / total_vinculos, 
        'SupComp': c_esc.get('SupComp', 0) / total_vinculos, 
        'Pos': c_esc.get('Pos', 0) / total_vinculos,
        
        'Nenhum Afastamento': afast_nenhum,
        'Um': afast_1_pct, 'Aci_1': afast_1_aci, 'Dias_1': afast_1_dias,
        'Dois': afast_2_pct, 'Aci_2': afast_2_aci, 'Dias_2': afast_2_dias,
        'Três ou Mais': afast_3_pct, 'Aci_3': afast_3_aci, 'Dias_3': afast_3_dias,
        
        'Até 30H': c_jor.get('Até 30H', 0) / total_vinculos, 
        'De 30H a 40H': c_jor.get('De 30H a 40H', 0) / total_vinculos, 
        'Acima de 40H': c_jor.get('Acima de 40H', 0) / total_vinculos,
        
        'Intermitente': inter_pct, 
        'Parcial': parcial_pct
    })

def salvar_relatorio_consolidado_excel(dict_relatorios: dict, caminho_arquivo: str):
    """Gera um único arquivo Excel contendo múltiplas abas com cabeçalho de 2 linhas."""
    caminho_completo = PASTA_SAIDA / caminho_arquivo
    try:
        excel_kwargs = {'options': {'nan_inf_to_errors': True}}
        with pd.ExcelWriter(caminho_completo, engine='xlsxwriter', engine_kwargs=excel_kwargs) as writer:
            workbook = writer.book
            
            # Configuração Visual via JSON
            f_num = C_EXCEL['formatos_numericos']
            f_base = {'font_name': C_EXCEL['fonte_padrao']}
            
            fmt_money = {**f_base, 'num_format': f_num['dinheiro'], 'align': 'right'}
            fmt_percent = {**f_base, 'num_format': f_num['percentual'], 'align': 'right'}
            fmt_number = {**f_base, 'num_format': f_num['inteiro'], 'align': 'right'}
            fmt_float = {**f_base, 'num_format': '#,##0.0', 'align': 'right'} # Novo formato para médias de dias
            fmt_string_r = {**f_base, 'align': 'right'}
            
            st_niveis = C_EXCEL['estilos_niveis']
            fmt_hierarquia = {}
            for lvl in ['l0', 'l1', 'l2', 'l3']:
                b_lvl = {**f_base, **st_niveis[lvl]}
                fmt_hierarquia[f"{lvl}_text"] = workbook.add_format({**b_lvl, 'align': 'left'})
                fmt_hierarquia[f"{lvl}_money"] = workbook.add_format({**b_lvl, **fmt_money})
                fmt_hierarquia[f"{lvl}_percent"] = workbook.add_format({**b_lvl, **fmt_percent})
                fmt_hierarquia[f"{lvl}_int"] = workbook.add_format({**b_lvl, **fmt_number})
                fmt_hierarquia[f"{lvl}_float"] = workbook.add_format({**b_lvl, **fmt_float})
                fmt_hierarquia[f"{lvl}_str_r"] = workbook.add_format({**b_lvl, **fmt_string_r})

            # Formatação do Cabeçalho (Negrito, Bordas, Fundo opcional pelo JSON)
            h_fmt = workbook.add_format({**f_base, **C_EXCEL['estilo_cabecalho'], 'valign': 'vcenter'})
            
            # Lê o cabeçalho diretamente do config.json
            if 'cabecalho_duplo' not in CONFIG:
                raise ValueError("A configuração 'cabecalho_duplo' não foi encontrada no arquivo config.json.")
            cabecalho_cfg = CONFIG['cabecalho_duplo']

            for sheet_name, df_final in dict_relatorios.items():
                worksheet = workbook.add_worksheet(sheet_name)
                
                m = C_EXCEL['margens']
                worksheet.set_margins(left=m['left'], right=m['right'], top=m['top'], bottom=m['bottom'])

                # Ajuste Fixo de Larguras com base num padrão extendido
                l_cols = C_EXCEL['largura_colunas_pixels']
                worksheet.set_column_pixels(0, 0, l_cols.get('A', 10))
                worksheet.set_column_pixels(1, 1, l_cols.get('B', 50))
                worksheet.set_column_pixels(2, 2, l_cols.get('C', 50))
                worksheet.set_column_pixels(3, 3, l_cols.get('D', 50))
                worksheet.set_column_pixels(4, 4, l_cols.get('E', 650))
                worksheet.set_column_pixels(5, 5, l_cols.get('F', 75))
                worksheet.set_column_pixels(6, 6, l_cols.get('G', 110))
                worksheet.set_column_pixels(7, 50, l_cols.get('H_O', 80)) # Aplica largura a todas as métricas

                # CONGELAR PAINÉIS (Célula F3 -> Fixa as duas primeiras linhas e as 5 primeiras colunas)
                worksheet.freeze_panes(2, 5)

                # ESCRITA DO CABEÇALHO DUPLO
                worksheet.set_row_pixels(0, C_EXCEL['altura_cabecalho'])
                worksheet.set_row_pixels(1, C_EXCEL['altura_cabecalho'])
                
                # Linha 1 (Merged Cells) - COM SUPORTE A CORES E BORDAS DINÂMICAS DO JSON
                col_idx = 0
                for cell in cabecalho_cfg['linha1']:
                    span = cell.get('span', 1)
                    texto = cell.get('texto', '')
                    
                    # Puxa o estilo base padrão do cabeçalho
                    propriedades_celula = {**f_base, **C_EXCEL['estilo_cabecalho'], 'valign': 'vcenter'}
                    
                    # Injeta propriedades extras vindas do JSON (ex: bg_color, font_color, border)
                    for chave, valor in cell.items():
                        if chave not in ['texto', 'span']:
                            propriedades_celula[chave] = valor
                            
                    fmt_dinamico = workbook.add_format(propriedades_celula)

                    if span > 1:
                        worksheet.merge_range(0, col_idx, 0, col_idx + span - 1, texto, fmt_dinamico)
                    else:
                        worksheet.write(0, col_idx, texto, fmt_dinamico)
                    col_idx += span

                # Linha 2
                h_fmt_linha2 = workbook.add_format({**f_base, **C_EXCEL['estilo_cabecalho'], 'valign': 'vcenter'})
                for c_idx, texto in enumerate(cabecalho_cfg['linha2']):
                    worksheet.write(1, c_idx, texto, h_fmt_linha2)
                
                # ESCRITA DOS DADOS
                for i, row_data in df_final.iterrows():
                    row_idx = i + 2 # Começa na 3ª linha do Excel (índice 2)
                    level_key = f"l{CONFIG['cnae_estrutura'][i].get('level', 0)}"
                    
                    for col in range(5):
                        worksheet.write_string(row_idx, col, str(row_data[f'Nível {col+1}']), fmt_hierarquia[f"{level_key}_text"])

                    for j, (col_name, type_fmt) in enumerate(DATA_COLS_MAP):
                        val = row_data[col_name]
                        target_fmt = fmt_hierarquia[f"{level_key}_{type_fmt}"]
                        
                        if val == '-': 
                            worksheet.write_string(row_idx, 5 + j, '-', fmt_hierarquia[f"{level_key}_str_r"])
                        elif pd.isna(val): 
                            worksheet.write_blank(row_idx, 5 + j, '', target_fmt)
                        else: 
                            worksheet.write_number(row_idx, 5 + j, val, target_fmt)

        print(f"Planilha consolidada exportada com sucesso: {caminho_completo.name}")
    except Exception as e:
        print(f"Erro ao salvar Excel consolidado: {e}")

def gerar_tabela_hierarquica(df: pd.DataFrame) -> pd.DataFrame:
    """Consolida os dados brutos numa tabela hierárquica completa em memória."""
    if df.empty: 
        return pd.DataFrame()
    
    df_proc = df.copy().assign(
        Genero_Limpo = lambda x: x['Genero_Raw'].astype(str).str.strip().map(CONFIG['mapas']['genero']),
        Raca_Limpa = lambda x: x['Raca_Raw'].astype(str).str.strip().map(CONFIG['mapas']['raca']),
        CNAE_Classe_Limpo = lambda x: x['CNAE_F'].astype(str).str.slice(0, 5),
        CNAE_Grupo_Limpo = lambda x: x['CNAE_Classe_Limpo'].str.slice(0, 3), 
        CNAE_Divisao_Limpo = lambda x: x['CNAE_Classe_Limpo'].str.slice(0, 2)
    )
    
    # As colunas numéricas de apoio não precisam estar aqui, pois são lidas diretamente
    # dentro da função calcular_metricas
    stats_cols = df_proc.columns.tolist()
    
    agg_classe = df_proc.groupby('CNAE_Classe_Limpo')[stats_cols].apply(calcular_metricas, include_groups=False).to_dict('index')
    agg_grupo = df_proc.groupby('CNAE_Grupo_Limpo')[stats_cols].apply(calcular_metricas, include_groups=False).to_dict('index')
    agg_divisao = df_proc.groupby('CNAE_Divisao_Limpo')[stats_cols].apply(calcular_metricas, include_groups=False).to_dict('index')
    agg_total = calcular_metricas(df_proc).to_dict()

    tabela_final = []
    
    for item in CONFIG['cnae_estrutura']:
        lvl = item.get('level', 0)
        code = item.get('clean_code', '')
        
        if lvl == 0: m = agg_total
        elif lvl == 1: m = agg_divisao.get(code, {})
        elif lvl == 2: m = agg_grupo.get(code, {})
        elif lvl == 3: m = agg_classe.get(code, {})
            
        linha_dict = {
            'Nível 1': item.get('titulo', '') if lvl == 0 else '', 
            'Nível 2': item.get('code', '') if lvl == 1 else '',
            'Nível 3': item.get('titulo', '') if lvl == 1 else (item.get('code', '') if lvl == 2 else ''),
            'Nível 4': item.get('titulo', '') if lvl == 2 else (item.get('code', '') if lvl == 3 else ''),
            'Nível 5': item.get('titulo', '') if lvl == 3 else ''
        }
        
        # Mapeia todas as métricas geradas para a linha
        for col_nome, _ in DATA_COLS_MAP:
            linha_dict[col_nome] = m.get(col_nome, '-')
            
        tabela_final.append(linha_dict)
        
    return pd.DataFrame(tabela_final)

# ==============================================================================
# --- INTERFACE GRÁFICA E THREAD DE PROCESSAMENTO ---
# ==============================================================================

class AppRAIS:
    def __init__(self, root, ufs, df_municipios, municipios_map):
        self.root = root
        self.ufs = ufs
        self.df_municipios = df_municipios
        self.municipios_map = municipios_map
        
        self.root.title("Extrator RAIS - Dashboards Avançados (DuckDB)")
        self.root.geometry("850x780")
        
        container = ttk.Frame(root, padding="25")
        container.pack(expand=True, fill=tk.BOTH)
        container.columnconfigure(1, weight=1)
        
        self.criar_interface(container)

    def criar_interface(self, parent):
        ttk.Label(parent, text="1. UF:").grid(row=0, column=0, padx=5, pady=10, sticky="w")
        self.uf_var = tk.StringVar()
        self.uf_combo = ttk.Combobox(parent, textvariable=self.uf_var, state="readonly", values=self.ufs, width=45)
        self.uf_combo.grid(row=0, column=1, padx=5, pady=10, sticky="ew")
        self.uf_combo.bind("<<ComboboxSelected>>", self.on_uf_selected)

        ttk.Label(parent, text="2. Município:").grid(row=1, column=0, padx=5, pady=10, sticky="w")
        self.mun_var = tk.StringVar()
        self.mun_combo = ttk.Combobox(parent, textvariable=self.mun_var, state="disabled", width=45)
        self.mun_combo.grid(row=1, column=1, padx=5, pady=10, sticky="ew")
        
        ttk.Button(parent, text="Adicionar à Lista ⬇", command=self.add_item).grid(row=2, column=1, padx=5, pady=5, sticky="e")

        ttk.Label(parent, text="3. Filtros Ativos:").grid(row=3, column=0, padx=5, pady=10, sticky="nw")
        list_frame = ttk.Frame(parent)
        list_frame.grid(row=4, column=0, columnspan=2, padx=5, pady=5, sticky="nsew")
        list_frame.columnconfigure(0, weight=1)
        
        self.listbox = tk.Listbox(list_frame, height=8, font=("Arial", 10))
        self.listbox.grid(row=0, column=0, sticky="nsew")
        
        btn_box = ttk.Frame(parent)
        btn_box.grid(row=5, column=0, columnspan=2, padx=5, pady=5, sticky="w")
        ttk.Button(btn_box, text="Remover", command=self.remove_item).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_box, text="Limpar", command=self.clear_all).pack(side=tk.LEFT, padx=5)

        self.btn_run = ttk.Button(parent, text="EXTRAIR E GERAR RELATÓRIOS", command=self.process_start)
        self.btn_run.grid(row=6, column=0, columnspan=2, pady=25)
        
        self.lbl_status = ttk.Label(parent, text="Pronto para uso.", foreground="blue")
        self.lbl_status.grid(row=7, column=0, columnspan=2)

    def on_uf_selected(self, event):
        muns = sorted(self.df_municipios[self.df_municipios['UF'] == self.uf_var.get()]['Nome Municipio'].tolist())
        self.mun_combo.config(values=muns, state="readonly")
        self.mun_combo.set("")

    def add_item(self):
        m = self.mun_var.get()
        uf = self.uf_var.get()
        if m and uf:
            item_formatado = f"{uf} - {m}"
            if item_formatado not in self.listbox.get(0, tk.END):
                self.listbox.insert(tk.END, item_formatado)

    def remove_item(self):
        idx = self.listbox.curselection()
        if idx:
            self.listbox.delete(idx)
        elif self.listbox.size() > 0:
            self.listbox.delete(tk.END)

    def clear_all(self):
        self.listbox.delete(0, tk.END)

    def set_status(self, msg):
        self.root.after(0, lambda: self.lbl_status.config(text=msg))

    def process_start(self):
        itens_selecionados = self.listbox.get(0, tk.END)
        if not itens_selecionados:
            messagebox.showwarning("Atenção", "Inclua pelo menos um município na lista para filtrar.")
            return
            
        self.btn_run.config(state="disabled")
        self.root.config(cursor="watch")
        
        cidades_por_uf = {}
        for item in itens_selecionados:
            uf, mun = item.split(" - ", 1)
            if uf not in cidades_por_uf:
                cidades_por_uf[uf] = []
            cidades_por_uf[uf].append(mun)
            
        threading.Thread(target=self.engine_duckdb, args=(cidades_por_uf,), daemon=True).start()

    def engine_duckdb(self, cidades_por_uf):
        """Motor DuckDB adaptado para extrair novas colunas RAIS com nomes exatos da base."""
        try:
            self.set_status("Iniciando extração profunda no DuckDB...")
            con = duckdb.connect()
            cnae_list = list(CONFIG['cnae_map'].values())
            cnae_formatados = ', '.join([f"'{c}'" for c in cnae_list])
            
            # Resgate dos nomes EXATOS das colunas com base no esquema da RAIS passado
            c_idade = C_COLS.get('idade', 'Idade')
            c_tempo = C_COLS.get('tempo_emprego', 'Tempo Emprego')
            c_esc = C_COLS.get('escolaridade', 'Escolaridade Após 2005 - Código')
            c_afast1 = C_COLS.get('causa_afastamento_1', 'Causa Afastamento 1 - Código')
            c_afast2 = C_COLS.get('causa_afastamento_2', 'Causa Afastamento 2 - Código')
            c_afast3 = C_COLS.get('causa_afastamento_3', 'Causa Afastamento 3 - Código')
            c_dias = C_COLS.get('qtd_dias_afastamento', 'Qtd Dias Afastamento')
            c_jor = C_COLS.get('jornada', 'Qtd Hora Contr')
            c_int = C_COLS.get('ind_intermitente', 'Ind Trabalho Intermitente - Código')
            c_par = C_COLS.get('ind_parcial', 'Ind Trabalho Parcial - Código')
            
            arquivos_alvo = {}
            for uf, muns in cidades_por_uf.items():
                arquivo_p = MAPA_ARQUIVOS_UF.get(uf)
                if not arquivo_p:
                    raise ValueError(f"Não existe um arquivo Parquet configurado para a UF: {uf}")
                if arquivo_p not in arquivos_alvo:
                    arquivos_alvo[arquivo_p] = {}
                arquivos_alvo[arquivo_p][uf] = muns

            summary = "Resultados da Extração:\n\n"
            stack_regional = []
            relatorios_para_excel = {}  

            for arquivo_p, ufs_dict in arquivos_alvo.items():
                path_f = PASTA_DADOS / arquivo_p

                if not path_f.exists() or path_f.stat().st_size < 1048576:
                    summary += f"⚠️ Erro: Arquivo {path_f.name} ausente ou corrompido.\n"
                    continue

                map_ibge = {}
                for uf, muns in ufs_dict.items():
                    for nome in muns:
                        cod_completo = str(self.municipios_map.get((uf, nome)))
                        cod_6dig = re.sub(r'\D', '', cod_completo)[:6]
                        map_ibge[cod_6dig] = f"{uf} - {nome}"

                cods_formatados = ', '.join([f"'{c}'" for c in map_ibge.keys()])
                caminho_parquet = path_f.as_posix()

                sql = f"""
                SELECT 
                    REGEXP_REPLACE(CAST("{C_COLS['municipio']}" AS VARCHAR), '[^0-9]', '', 'g') as M1,
                    REGEXP_REPLACE(CAST("{C_COLS['municipio_trab']}" AS VARCHAR), '[^0-9]', '', 'g') as M2,
                    SUBSTR(REGEXP_REPLACE(CAST("{C_COLS['cnae_subclasse']}" AS VARCHAR), '[^0-9]', '', 'g'), 1, 5) as CNAE_F,
                    CAST(REPLACE(CAST("{C_COLS['remuneracao']}" AS VARCHAR), ',', '.') AS DOUBLE) as Remuneracao_Num,
                    CAST("{C_COLS['genero']}" AS VARCHAR) as Genero_Raw,
                    CAST("{C_COLS['raca']}" AS VARCHAR) as Raca_Raw,
                    CAST("{c_idade}" AS DOUBLE) as Idade_Num,
                    CAST("{c_tempo}" AS DOUBLE) as Tempo_Emprego_Num,
                    CAST("{c_esc}" AS DOUBLE) as Escolaridade_Num,
                    CAST("{c_afast1}" AS VARCHAR) as Causa_Afast_1,
                    CAST("{c_afast2}" AS VARCHAR) as Causa_Afast_2,
                    CAST("{c_afast3}" AS VARCHAR) as Causa_Afast_3,
                    CAST("{c_dias}" AS DOUBLE) as Dias_Afast_Num,
                    CAST(REPLACE(CAST("{c_jor}" AS VARCHAR), ',', '.') AS DOUBLE) as Jornada_Num,
                    CAST("{c_int}" AS VARCHAR) as Ind_Intermitente,
                    CAST("{c_par}" AS VARCHAR) as Ind_Parcial
                FROM '{caminho_parquet}'
                WHERE 
                    (REGEXP_REPLACE(CAST("{C_COLS['municipio']}" AS VARCHAR), '[^0-9]', '', 'g') IN ({cods_formatados})
                     OR REGEXP_REPLACE(CAST("{C_COLS['municipio_trab']}" AS VARCHAR), '[^0-9]', '', 'g') IN ({cods_formatados}))
                    AND CAST("{C_COLS['filtro_vinculo']}" AS VARCHAR) IN ('1', '01', 'SIM', 'S', 's', 'Sim')
                    AND SUBSTR(REGEXP_REPLACE(CAST("{C_COLS['cnae_subclasse']}" AS VARCHAR), '[^0-9]', '', 'g'), 1, 5) IN ({cnae_formatados})
                """

                self.set_status(f"Extraindo variáveis da base {arquivo_p}...")
                df_res = con.execute(sql).df()
                
                if df_res.empty:
                    for cod, nome_exibicao in map_ibge.items():
                        summary += f"✖ {nome_exibicao}: Nenhum vínculo encontrado.\n"
                    continue

                self.set_status(f"Calculando painéis para {arquivo_p}...")
                
                for cod, nome_exibicao in map_ibge.items():
                    sub = df_res[(df_res['M1'] == cod) | (df_res['M2'] == cod)].copy()
                    
                    if not sub.empty:
                        sub.loc[:, C_COLS['cnae_classe']] = sub['CNAE_F']
                        aba_nome = re.sub(r'[\\/*?:\[\]]', '', nome_exibicao)[:31]
                        
                        relatorios_para_excel[aba_nome] = gerar_tabela_hierarquica(sub)
                        stack_regional.append(sub)
                        summary += f"✔ {nome_exibicao}: {len(sub):,} registros analisados.\n"
                    else:
                        summary += f"✖ {nome_exibicao}: Nenhum registro retornado\n"

            if len(stack_regional) > 1:
                self.set_status("Consolidando Região Inteira...")
                df_regional = pd.concat(stack_regional, ignore_index=True)
                
                aba_regional = {"Regional": gerar_tabela_hierarquica(df_regional)}
                relatorios_para_excel = {**aba_regional, **relatorios_para_excel}
                summary += f"\n✔ Aba Regional criada com {len(df_regional):,} vínculos globais."

            if relatorios_para_excel:
                self.set_status("Escrevendo Excel Profissional...")
                timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                # ALTERAÇÃO: Novo nome de ficheiro conforme solicitado
                nome_arquivo_final = f"Relatório Ramo Financeiro_{timestamp}.xlsx"
                salvar_relatorio_consolidado_excel(relatorios_para_excel, nome_arquivo_final)

            self.set_status("Finalizado com Excel gerado.")
            self.root.after(0, lambda s=summary: messagebox.showinfo("Extração Concluída", s))

        except Exception as err:
            self.set_status("Erro durante análise de dados.")
            mensagem_erro = f"Falha na filtragem:\n{err}\n\nTraceback:\n{traceback.format_exc()}"
            self.root.after(0, lambda msg=mensagem_erro: messagebox.showerror("Erro Crítico", msg))
        finally:
            self.root.after(0, lambda: self.btn_run.config(state="normal"))
            self.root.after(0, lambda: self.root.config(cursor=""))

# ==============================================================================
# --- STARTUP ---
# ==============================================================================

if __name__ == "__main__":
    try:
        df_ibge = pd.read_csv(ARQUIVO_MUNICIPIOS, sep=';', dtype=str, encoding='utf-8')
        df_ibge.columns = [c.strip() for c in df_ibge.columns]
        
        main_root = tk.Tk()
        ufs_disponiveis = sorted(df_ibge['UF'].unique())
        mapa_mun = df_ibge.set_index(['UF', 'Nome Municipio'])['Codigo Municipio'].to_dict()
        
        AppRAIS(main_root, ufs_disponiveis, df_ibge, mapa_mun)
        main_root.mainloop()
        
    except Exception as e:
        print(f"Erro Fatal na inicialização do extrator: {e}")