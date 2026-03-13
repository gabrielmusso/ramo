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
    # TRAVA DE SEGURANÇA: Força a extensão para .parquet independentemente do JSON
    arquivo_parquet = Path(arquivo).with_suffix('.parquet').name
    for uf in ufs:
        MAPA_ARQUIVOS_UF[uf] = arquivo_parquet

# Cria a pasta de saída para os relatórios, se não existir
PASTA_SAIDA.mkdir(parents=True, exist_ok=True)

# ==============================================================================
# --- FUNÇÕES DE PROCESSAMENTO E RELATÓRIO ---
# ==============================================================================

def calcular_metricas(group: pd.DataFrame) -> pd.Series:
    """Calcula estatísticas de remuneração e composição demográfica (Gênero/Raça)."""
    total_vinculos = len(group)
    
    # Se o grupo estiver vazio, retorna placeholders para evitar erros
    if total_vinculos == 0:
        return pd.Series({
            'Remunacao Media': '-', 'Pct Homens': '-', 'Pct Mulheres': '-',
            'Total Vinculos': 0, 'Pct Branca': '-', 'Pct Preta': '-',
            'Pct Parda': '-', 'Pct Amarela': '-', 'Pct Indigena': '-', 'Pct Ignorado/NI': '-'
        })

    # A remuneração já vem como número DOUBLE do DuckDB
    media_remuneracao = group['Remuneracao_Num'].mean(skipna=True)
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

def salvar_relatorio_consolidado_excel(dict_relatorios: dict, caminho_arquivo: str):
    """Gera um único arquivo Excel contendo múltiplas abas (uma para cada dataframe)."""
    caminho_completo = PASTA_SAIDA / caminho_arquivo
    try:
        excel_kwargs = {'options': {'nan_inf_to_errors': True}}
        with pd.ExcelWriter(caminho_completo, engine='xlsxwriter', engine_kwargs=excel_kwargs) as writer:
            workbook = writer.book
            
            # Formatos numéricos e base
            f_num = C_EXCEL['formatos_numericos']
            f_base = {'font_name': C_EXCEL['fonte_padrao']}
            
            fmt_money = {**f_base, 'num_format': f_num['dinheiro'], 'align': 'right'}
            fmt_percent = {**f_base, 'num_format': f_num['percentual'], 'align': 'right'}
            fmt_number = {**f_base, 'num_format': f_num['inteiro'], 'align': 'right'}
            fmt_string_r = {**f_base, 'align': 'right'}
            
            # Estilos Hierárquicos dinâmicos (L0 a L3) criados apenas uma vez por arquivo
            st_niveis = C_EXCEL['estilos_niveis']
            fmt_hierarquia = {}
            for lvl in ['l0', 'l1', 'l2', 'l3']:
                base_lvl = {**f_base, **st_niveis[lvl]}
                fmt_hierarquia[f"{lvl}_text"] = workbook.add_format({**base_lvl, 'align': 'left'})
                fmt_hierarquia[f"{lvl}_money"] = workbook.add_format({**base_lvl, **fmt_money})
                fmt_hierarquia[f"{lvl}_percent"] = workbook.add_format({**base_lvl, **fmt_percent})
                fmt_hierarquia[f"{lvl}_int"] = workbook.add_format({**base_lvl, **fmt_number})
                fmt_hierarquia[f"{lvl}_str_r"] = workbook.add_format({**base_lvl, **fmt_string_r})

            h_fmt = workbook.add_format({**f_base, **C_EXCEL['estilo_cabecalho']})

            # Itera sobre o dicionário construindo uma aba para cada cidade/região
            for sheet_name, df_final in dict_relatorios.items():
                worksheet = workbook.add_worksheet(sheet_name)
                
                # Margens
                m = C_EXCEL['margens']
                worksheet.set_margins(left=m['left'], right=m['right'], top=m['top'], bottom=m['bottom'])

                # Larguras de Colunas
                l_cols = C_EXCEL['largura_colunas_pixels']
                col_map = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
                for i, col_letter in enumerate(col_map):
                    worksheet.set_column_pixels(i, i, l_cols[col_letter])
                worksheet.set_column_pixels(7, 14, l_cols['H_O'])

                # Cabeçalho - Limpando as células A1 a E1
                headers = list(df_final.columns)
                headers[0:5] = [''] * 5  # Substitui os Níveis 1 a 5 por strings vazias
                worksheet.write_row('A1', headers, h_fmt)
                worksheet.set_row_pixels(0, C_EXCEL['altura_cabecalho'])
                
                # Escrita de Linhas
                for i, row_data in df_final.iterrows():
                    row_idx = i + 1
                    level_key = f"l{CONFIG['cnae_estrutura'][i].get('level', 0)}"
                    
                    # Colunas de hierarquia textual
                    for col in range(5):
                        worksheet.write_string(row_idx, col, str(row_data[f'Nível {col+1}']), fmt_hierarquia[f"{level_key}_text"])

                    # Mapeamento de métricas e formatos
                    data_cols_map = [
                        ('Total Vínculos', 'int'), ('Remuneração Média', 'money'),
                        ('Pct Homens', 'percent'), ('Pct Mulheres', 'percent'),
                        ('Pct Branca', 'percent'), ('Pct Preta', 'percent'),
                        ('Pct Parda', 'percent'), ('Pct Amarela', 'percent'),
                        ('Pct Indigena', 'percent'), ('Pct Ignorado/NI', 'percent')
                    ]

                    # Colunas de métricas numéricas
                    for j, (col_name, type_fmt) in enumerate(data_cols_map):
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
    """Consolida os dados brutos em uma tabela hierárquica e a retorna (não salva em disco)."""
    if df.empty: 
        return pd.DataFrame()
    
    # Preparação de colunas limpas para agregações
    df_proc = df.copy().assign(
        # DuckDB já tratou 'Remuneracao_Num', 'Genero_Raw', 'Raca_Raw', 'CNAE_F'
        Genero_Limpo = lambda x: x['Genero_Raw'].astype(str).str.strip().map(CONFIG['mapas']['genero']),
        Raca_Limpa = lambda x: x['Raca_Raw'].astype(str).str.strip().map(CONFIG['mapas']['raca']),
        CNAE_Classe_Limpo = lambda x: x['CNAE_F'].astype(str).str.slice(0, 5),
        CNAE_Grupo_Limpo = lambda x: x['CNAE_Classe_Limpo'].str.slice(0, 3), 
        CNAE_Divisao_Limpo = lambda x: x['CNAE_Classe_Limpo'].str.slice(0, 2)
    )
    
    stats_cols = ['Remuneracao_Num', 'Genero_Limpo', 'Raca_Limpa']
    
    # Agrupamentos Estatísticos
    agg_classe = df_proc.groupby('CNAE_Classe_Limpo')[stats_cols].apply(calcular_metricas, include_groups=False).to_dict('index')
    agg_grupo = df_proc.groupby('CNAE_Grupo_Limpo')[stats_cols].apply(calcular_metricas, include_groups=False).to_dict('index')
    agg_divisao = df_proc.groupby('CNAE_Divisao_Limpo')[stats_cols].apply(calcular_metricas, include_groups=False).to_dict('index')
    agg_total = calcular_metricas(df_proc).to_dict()

    tabela_final = []
    
    # Montagem linha a linha conforme o esqueleto JSON
    for item in CONFIG['cnae_estrutura']:
        # Uso de .get() para evitar KeyError se faltar 'level' ou 'clean_code'
        lvl = item.get('level', 0)
        code = item.get('clean_code', '')
        
        # Recupera as métricas adequadas para o nível atual
        m = {}
        if lvl == 0:
            m = agg_total
        elif lvl == 1:
            m = agg_divisao.get(code, {})
        elif lvl == 2:
            m = agg_grupo.get(code, {})
        elif lvl == 3:
            m = agg_classe.get(code, {})
            
        # Uso do método .get('titulo', '') e .get('code', '') em todas as validações
        tabela_final.append({
            'Nível 1': item.get('titulo', '') if lvl == 0 else '', 
            'Nível 2': item.get('code', '') if lvl == 1 else '',
            'Nível 3': item.get('titulo', '') if lvl == 1 else (item.get('code', '') if lvl == 2 else ''),
            'Nível 4': item.get('titulo', '') if lvl == 2 else (item.get('code', '') if lvl == 3 else ''),
            'Nível 5': item.get('titulo', '') if lvl == 3 else '',
            'Total Vínculos': m.get('Total Vinculos'), 
            'Remuneração Média': m.get('Remunacao Media'),
            'Pct Homens': m.get('Pct Homens'), 
            'Pct Mulheres': m.get('Pct Mulheres'),
            'Pct Branca': m.get('Pct Branca'), 
            'Pct Preta': m.get('Pct Preta'), 
            'Pct Parda': m.get('Pct Parda'),
            'Pct Amarela': m.get('Pct Amarela'), 
            'Pct Indigena': m.get('Pct Indigena'), 
            'Pct Ignorado/NI': m.get('Pct Ignorado/NI')
        })
        
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
        
        self.root.title("Extrator RAIS - Motor DuckDB Ultra-Rápido")
        self.root.geometry("850x780")
        
        container = ttk.Frame(root, padding="25")
        container.pack(expand=True, fill=tk.BOTH)
        container.columnconfigure(1, weight=1)
        
        self.criar_interface(container)

    def criar_interface(self, parent):
        # 1. UF
        ttk.Label(parent, text="1. UF:").grid(row=0, column=0, padx=5, pady=10, sticky="w")
        self.uf_var = tk.StringVar()
        self.uf_combo = ttk.Combobox(parent, textvariable=self.uf_var, state="readonly", values=self.ufs, width=45)
        self.uf_combo.grid(row=0, column=1, padx=5, pady=10, sticky="ew")
        self.uf_combo.bind("<<ComboboxSelected>>", self.on_uf_selected)

        # 2. Município
        ttk.Label(parent, text="2. Município:").grid(row=1, column=0, padx=5, pady=10, sticky="w")
        self.mun_var = tk.StringVar()
        self.mun_combo = ttk.Combobox(parent, textvariable=self.mun_var, state="disabled", width=45)
        self.mun_combo.grid(row=1, column=1, padx=5, pady=10, sticky="ew")
        
        ttk.Button(parent, text="Adicionar à Lista ⬇", command=self.add_item).grid(row=2, column=1, padx=5, pady=5, sticky="e")

        # 3. Lista de Filtros
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

        # Execução
        self.btn_run = ttk.Button(parent, text="EXTRAIR E GERAR RELATÓRIOS (DUCKDB)", command=self.process_start)
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
        # CORREÇÃO: Guardar a UF junto com o município na lista, garantindo a associação correta
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
        """Thread-safe update da interface gráfica."""
        self.root.after(0, lambda: self.lbl_status.config(text=msg))

    def process_start(self):
        itens_selecionados = self.listbox.get(0, tk.END)
        
        if not itens_selecionados:
            messagebox.showwarning("Atenção", "Inclua pelo menos um município na lista para filtrar.")
            return
            
        self.btn_run.config(state="disabled")
        self.root.config(cursor="watch")
        
        # CORREÇÃO: Transformamos a lista de strings "UF - MUN" em um dicionário agrupado
        cidades_por_uf = {}
        for item in itens_selecionados:
            uf, mun = item.split(" - ", 1)
            if uf not in cidades_por_uf:
                cidades_por_uf[uf] = []
            cidades_por_uf[uf].append(mun)
            
        # Desvia o trabalho pesado para uma thread secundária
        threading.Thread(target=self.engine_duckdb, args=(cidades_por_uf,), daemon=True).start()

    def engine_duckdb(self, cidades_por_uf):
        """Motor DuckDB que processa múltiplos arquivos Parquet dinamicamente."""
        try:
            self.set_status("Iniciando motor DuckDB...")
            con = duckdb.connect()
            cnae_list = list(CONFIG['cnae_map'].values())
            cnae_formatados = ', '.join([f"'{c}'" for c in cnae_list])
            
            # CORREÇÃO: Agrupa UFs pelo arquivo Parquet correspondente
            # Isso evita ler o mesmo arquivo de 20GB duas vezes se pedirem cidades de BA e PE
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
            relatorios_para_excel = {}  # Dicionário para armazenar as abas da planilha

            # Itera sobre cada arquivo Parquet necessário
            for arquivo_p, ufs_dict in arquivos_alvo.items():
                path_f = PASTA_DADOS / arquivo_p

                if not path_f.exists():
                    summary += f"⚠️ Erro: Arquivo {path_f.name} não encontrado.\n"
                    continue

                if path_f.stat().st_size < 1048576:
                    summary += f"⚠️ Erro: Arquivo {path_f.name} parece corrompido (tamanho muito pequeno).\n"
                    continue

                # Preparar os códigos IBGE desta rodada
                map_ibge = {}
                for uf, muns in ufs_dict.items():
                    for nome in muns:
                        cod_completo = str(self.municipios_map.get((uf, nome)))
                        # CORREÇÃO CRUCIAL: A RAIS usa 6 dígitos. O IBGE padrão tem 7. 
                        # Aqui cortamos para 6 dígitos para o cruzamento bater com sucesso!
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
                    *
                FROM '{caminho_parquet}'
                WHERE 
                    (REGEXP_REPLACE(CAST("{C_COLS['municipio']}" AS VARCHAR), '[^0-9]', '', 'g') IN ({cods_formatados})
                     OR REGEXP_REPLACE(CAST("{C_COLS['municipio_trab']}" AS VARCHAR), '[^0-9]', '', 'g') IN ({cods_formatados}))
                    AND CAST("{C_COLS['filtro_vinculo']}" AS VARCHAR) IN ('1', '01', 'SIM', 'S', 's', 'Sim')
                    AND SUBSTR(REGEXP_REPLACE(CAST("{C_COLS['cnae_subclasse']}" AS VARCHAR), '[^0-9]', '', 'g'), 1, 5) IN ({cnae_formatados})
                """

                self.set_status(f"Consultando DuckDB na base {arquivo_p}...")
                df_res = con.execute(sql).df()
                
                if df_res.empty:
                    for cod, nome_exibicao in map_ibge.items():
                        summary += f"✖ {nome_exibicao}: Nenhum vínculo encontrado.\n"
                    continue

                self.set_status(f"Processando resultados de {arquivo_p}...")
                
                # Separa o DataFrame consolidado município por município
                for cod, nome_exibicao in map_ibge.items():
                    sub = df_res[(df_res['M1'] == cod) | (df_res['M2'] == cod)].copy()
                    
                    if not sub.empty:
                        sub.loc[:, C_COLS['cnae_classe']] = sub['CNAE_F']
                        
                        # Gera o nome de arquivo seguro: ex 'SP_Limeira.csv'
                        fname = f"{nome_exibicao.replace(' - ', '_')}"
                        fname = re.sub(r'[^\w\s]', '', fname).replace(' ', '_')
                        
                        # Gera a tabela relacional e guarda no dicionário para as abas do Excel
                        aba_nome = re.sub(r'[\\/*?:\[\]]', '', nome_exibicao)[:31] # Excel limita a 31 chars
                        relatorios_para_excel[aba_nome] = gerar_tabela_hierarquica(sub)
                        
                        stack_regional.append(sub)
                        summary += f"✔ {nome_exibicao}: {len(sub):,} registros\n"
                    else:
                        summary += f"✖ {nome_exibicao}: Nenhum registro retornado\n"

            # Se mais de um município foi buscado no total, gera o relatório regional
            if len(stack_regional) > 1:
                self.set_status("Gerando Relatório Regional...")
                df_regional = pd.concat(stack_regional, ignore_index=True)
                
                aba_regional = {"Regional": gerar_tabela_hierarquica(df_regional)}
                # Adiciona o regional na primeira posição (como primeira aba)
                relatorios_para_excel = {**aba_regional, **relatorios_para_excel}
                
                summary += f"\n✔ Consolidado Regional gerado ({len(df_regional):,} vínculos no total)."

            if relatorios_para_excel:
                self.set_status("Criando planilha Excel consolidada...")
                timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                nome_arquivo_final = f"Relatorios_RAIS_Consolidado_{timestamp}.xlsx"
                salvar_relatorio_consolidado_excel(relatorios_para_excel, nome_arquivo_final)

            self.set_status("Finalizado.")
            self.root.after(0, lambda s=summary: messagebox.showinfo("Extração Concluída", s))

        except Exception as err:
            self.set_status("Erro no motor DuckDB.")
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
        # Lê o arquivo de Mapeamento de IBGE
        df_ibge = pd.read_csv(ARQUIVO_MUNICIPIOS, sep=';', dtype=str, encoding='utf-8')
        df_ibge.columns = [c.strip() for c in df_ibge.columns]
        
        # Inicia o App
        main_root = tk.Tk()
        ufs_disponiveis = sorted(df_ibge['UF'].unique())
        mapa_mun = df_ibge.set_index(['UF', 'Nome Municipio'])['Codigo Municipio'].to_dict()
        
        AppRAIS(main_root, ufs_disponiveis, df_ibge, mapa_mun)
        main_root.mainloop()
        
    except Exception as e:
        print(f"Erro Fatal na inicialização do extrator: {e}")