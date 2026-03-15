import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
import threading
import json
import sys
import re
from pathlib import Path
from datetime import datetime

# ==========================================
# --- CARREGAMENTO DE CONFIGURAÇÕES ---
# ==========================================
def carregar_configuracoes():
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"ERRO FATAL: Não foi possível ler 'config.json'.\nDetalhes: {e}")
        sys.exit(1)

CONFIG = carregar_configuracoes()
GLOBais = CONFIG['configuracoes_globais']
CORRESPONDENTES_CFG = CONFIG.get('correspondentes_excel_config', {})

PASTA_DADOS = Path(GLOBais['pasta_dados']).expanduser()
PASTA_SAIDA = Path(GLOBais['pasta_saida']).expanduser()
ARQUIVO_MUNICIPIOS = PASTA_DADOS / GLOBais['arquivo_municipios']
CODIFICACAO = GLOBais.get('codificacao', 'latin-1')

PASTA_SAIDA.mkdir(parents=True, exist_ok=True)

# ==========================================
# --- FUNÇÕES DE SUPORTE ---
# ==========================================
def detectar_linha_cabecalho(caminho_arquivo: Path) -> int:
    try:
        if caminho_arquivo.suffix.lower() == '.xlsx':
            df_sample = pd.read_excel(caminho_arquivo, header=None, nrows=30, engine='calamine')
        else:
            df_sample = pd.read_csv(caminho_arquivo, sep=';', encoding=CODIFICACAO, header=None, nrows=30)
        
        for index, row in df_sample.iterrows():
            linha_texto = " ".join(row.dropna().astype(str)).upper()
            if "CNPJ" in linha_texto or "ENDEREÇO" in linha_texto or "ENDERECO" in linha_texto:
                return index
        return 0
    except Exception:
        return 0

def formatar_cnpj_corresp(row, indices):
    """Mascara o CNPJ assumindo as posições fixas: Base (8), Filial (4) e Dígito (2)."""
    b_idx, f_idx, d_idx = indices
    
    # Restaurado o .strip() para garantir que espaços invisíveis não quebrem o CNPJ
    base = str(row.iloc[b_idx]).strip().replace('.0', '').zfill(8)
    filial = str(row.iloc[f_idx]).strip().replace('.0', '').zfill(4)
    digito = str(row.iloc[d_idx]).strip().replace('.0', '').zfill(2)
    
    if base == "00000000" and filial == "0000": return ""
    return f"{base[:2]}.{base[2:5]}.{base[5:8]}/{filial}-{digito}"

def construir_endereco(row, indices):
    """Constrói o endereço concatenando as colunas, confiando nos índices fixos."""
    partes = []
    for idx in indices:
        val = str(row.iloc[idx]).strip()
        if val.endswith('.0'):  # Remove zeros decimais caso o pandas leia como float
            val = val[:-2]
        # Restaurada a verificação estrita de Nulos/NaN da versão anterior
        if val and val.upper() not in ['NAN', 'NONE', '<NA>']:
            # Formata diretamente o CEP sabendo que ele está no índice 14 (Coluna O)
            if idx == 14 and re.match(r'^\d{8}$', val):
                val = f"{val[:5]}-{val[5:]}"
            partes.append(val)
    return ", ".join(partes)

# ==========================================
# --- MOTOR DE PROCESSAMENTO ---
# ==========================================
def processar_correspondentes(municipios_alvo, app_instance):
    try:
        app_instance.atualizar_status("A procurar o ficheiro de Correspondentes...")
        arquivos = list(PASTA_DADOS.glob("*CORRESPONDENTES*"))
        arquivos_validos = [f for f in arquivos if f.suffix.lower() in ['.csv', '.txt', '.xlsx']]
        
        if not arquivos_validos:
            raise FileNotFoundError("Nenhum ficheiro 'CORRESPONDENTES' encontrado na pasta de dados.")
            
        arquivo_recente = sorted(arquivos_validos, reverse=True)[0]
        index_cabecalho = detectar_linha_cabecalho(arquivo_recente)
        
        app_instance.atualizar_status(f"A carregar (via Calamine): {arquivo_recente.name}...")
        
        if arquivo_recente.suffix.lower() == '.xlsx':
            df_raw = pd.read_excel(arquivo_recente, dtype=str, skiprows=index_cabecalho, engine='calamine')
        else:
            df_raw = pd.read_csv(arquivo_recente, sep=';', encoding=CODIFICACAO, dtype=str, skiprows=index_cabecalho)
        
        if df_raw.empty: raise ValueError("O ficheiro lido está vazio.")

        # Limpeza básica de colunas e dados gerais
        df_raw.columns = [str(col).strip() for col in df_raw.columns]
        colunas_texto = df_raw.select_dtypes(include=['object', 'string']).columns
        df_raw[colunas_texto] = df_raw[colunas_texto].apply(lambda x: x.str.strip())

        # Coluna IBGE para filtro (Sempre a última no BCB)
        col_municipio_codigo = df_raw.columns[-1]
        df_raw['_mun_temp'] = df_raw[col_municipio_codigo].astype(str).str.slice(0, 6)
        
        df_filtrado = df_raw[df_raw['_mun_temp'].isin(municipios_alvo.keys())].copy()

        # Filtro de exclusão via regex do JSON
        app_instance.atualizar_status("A aplicar filtros de exclusão...")
        padrao_exclusao = CORRESPONDENTES_CFG.get('padrao_exclusao_nomes', '')
        if padrao_exclusao:
            # Coluna F (índice 5) fixo para o nome do correspondente conforme BCB
            nomes_com_espacos = " " + df_filtrado.iloc[:, 5].astype(str).fillna("") + " "
            mascara = nomes_com_espacos.str.contains(padrao_exclusao, case=False, regex=True)
            df_filtrado = df_filtrado[~mascara].copy()

        if df_filtrado.empty:
            app_instance.atualizar_interface(messagebox.showwarning, "Aviso", "Nenhum resultado após os filtros.")
            return

        app_instance.atualizar_status("A estruturar colunas conforme JSON...")
        
        # Preenche vazios para evitar 'nan' na string
        df_filtrado = df_filtrado.fillna('')
        
        df_apresentacao = pd.DataFrame()
        colunas_ordenadas = []
        colunas_info_corresp = [] # Colunas que manterão o primeiro valor (não concatenadas)
        chaves_agregacao = []     # Colunas que serão concatenadas

        # Construção das colunas dinamicamente a partir do JSON com STRIP restaurado
        for cfg_col in CORRESPONDENTES_CFG['colunas_saida']:
            nome = cfg_col['nome']
            tipo = cfg_col['origem_tipo']
            val = cfg_col['origem_val']
            
            colunas_ordenadas.append(nome)
            
            if tipo == 'index':
                s = df_filtrado.iloc[:, val].astype(str).replace(['nan', 'None', '<NA>', 'NAN'], '')
                df_apresentacao[nome] = s.str.strip()
                colunas_info_corresp.append(nome)
            
            elif tipo == 'cnpj_corresp':
                df_apresentacao[nome] = df_filtrado.apply(lambda r: formatar_cnpj_corresp(r, val), axis=1)
                colunas_info_corresp.append(nome)
            
            elif tipo == 'endereco':
                df_apresentacao[nome] = df_filtrado.apply(lambda r: construir_endereco(r, val), axis=1)
                colunas_info_corresp.append(nome)
                
            elif tipo == 'agrupado':
                # Colunas de Contratante que serão unidas com vírgula depois
                s = df_filtrado.iloc[:, val].astype(str).replace(['nan', 'None', '<NA>', 'NAN'], '')
                df_apresentacao[nome] = s.str.strip()
                chaves_agregacao.append(nome)

        # Manter o ID do município para separar as abas
        df_apresentacao['_mun_temp'] = df_filtrado['_mun_temp']
        colunas_info_corresp.append('_mun_temp')

        # Criação da Chave Mestra baseada na solicitação: CNPJ (Base, Filial, Digito) + Endereço Coluna K (Índice 10)
        chave_cnpj = df_filtrado.apply(lambda r: formatar_cnpj_corresp(r, [2, 3, 4]), axis=1)
        chave_logradouro = df_filtrado.iloc[:, 10].astype(str).str.strip().str.upper().replace(['NAN', 'NONE', '<NA>'], '')
        df_apresentacao['_chave_mestra'] = chave_cnpj + "|" + chave_logradouro + "|" + df_apresentacao['_mun_temp']

        app_instance.atualizar_status("A agregar correspondentes (CNPJ + Logradouro)...")
        
        # Define as regras de agrupamento
        regras_agg = {}
        # 1. Agrega as contratantes
        for col in chaves_agregacao:
            regras_agg[col] = lambda x: ", ".join(sorted([v for v in x.unique() if str(v).strip()]))
            
        # 2. Mantém a primeira ocorrência do resto (Endereço completo com as demais colunas, Incisos, etc)
        for col in colunas_info_corresp:
            regras_agg[col] = 'first'

        df_agg = df_apresentacao.groupby('_chave_mestra', as_index=False, dropna=False).agg(regras_agg)

        # Reordenar para a ordem EXATA estipulada no JSON
        df_agg = df_agg[colunas_ordenadas + ['_mun_temp']]
        
        # Gerar o nome do arquivo com a Data e Hora atual
        data_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
        if len(municipios_alvo) > 1:
            nome_final = f"Correspondentes_Consolidado_{data_hora}.xlsx"
        else:
            mun_nome = re.sub(r'[\\/*?:"<>|]', '', list(municipios_alvo.values())[0])
            nome_final = f"Correspondentes_{mun_nome}_{data_hora}.xlsx"
            
        caminho_saida = PASTA_SAIDA / nome_final
        
        app_instance.atualizar_status("A gerar ficheiro Excel...")

        try:
            with pd.ExcelWriter(caminho_saida, engine='xlsxwriter') as writer:
                workbook = writer.book
                
                # Setup de Estilos lidos do JSON
                font_name = CORRESPONDENTES_CFG.get('fonte_padrao', 'Montserrat')
                font_size_body = CORRESPONDENTES_CFG.get('font_size_corpo', 9)
                head_cfg = CORRESPONDENTES_CFG.get('estilo_cabecalho', {})
                margens = CORRESPONDENTES_CFG.get('margens', {})
                
                fmt_header = workbook.add_format({
                    'font_name': font_name, 'bold': head_cfg.get('bold', True),
                    'align': head_cfg.get('align', 'center'), 'font_size': head_cfg.get('font_size', 10),
                    'bg_color': head_cfg.get('bg_color', '#ADD8E6'), 
                    'bottom': 1, 'border': 1, 'text_wrap': head_cfg.get('text_wrap', True),
                    'valign': 'vcenter'
                })
                
                # Criamos dois estilos de corpo: um padrão à esquerda e outro centralizado
                fmt_body_left = workbook.add_format({'font_name': font_name, 'font_size': font_size_body, 'text_wrap': True, 'valign': 'top'})
                fmt_body_center = workbook.add_format({'font_name': font_name, 'font_size': font_size_body, 'text_wrap': True, 'valign': 'top', 'align': 'center'})

                def formatar_aba(ws, df_sheet, aplicar_larguras_json=True):
                    ws.freeze_panes(1, 0)
                    ws.autofilter(0, 0, len(df_sheet), len(df_sheet.columns) - 1)
                    ws.set_zoom(100)
                    ws.set_margins(
                        left=margens.get('left', 0.5), right=margens.get('right', 0.5),
                        top=margens.get('top', 0.75), bottom=margens.get('bottom', 0.75)
                    )
                    
                    # Se for a aba principal, puxa as larguras do JSON na ordem
                    if aplicar_larguras_json:
                        for col_num, cfg_col in enumerate(CORRESPONDENTES_CFG['colunas_saida']):
                            ws.write(0, col_num, cfg_col['nome'], fmt_header)
                            # Verifica no JSON se a coluna exige alinhamento centralizado
                            fmt_usado = fmt_body_center if cfg_col.get('align') == 'center' else fmt_body_left
                            ws.set_column(col_num, col_num, cfg_col.get('largura', 15), fmt_usado)
                    else:
                        # Aba de dados brutos genérica
                        for col_num, col_name in enumerate(df_sheet.columns):
                            ws.write(0, col_num, col_name, fmt_header)
                            ws.set_column(col_num, col_num, 15, fmt_body_left)

                # 1. Abas por Município
                for cod6, nome_mun in municipios_alvo.items():
                    df_mun_agg = df_agg[df_agg['_mun_temp'] == cod6].copy()
                    if not df_mun_agg.empty:
                        df_mun_agg = df_mun_agg.drop(columns=['_mun_temp'])
                        # Ordena pelo CNPJ do Correspondente para melhor visualização (coluna 1 base 0)
                        df_mun_agg = df_mun_agg.sort_values(by=colunas_ordenadas[1])
                        
                        nome_aba = re.sub(r'[\\/*?:"<>|]', '', nome_mun)[:31]
                        df_mun_agg.to_excel(writer, sheet_name=nome_aba, index=False)
                        formatar_aba(writer.sheets[nome_aba], df_mun_agg, aplicar_larguras_json=True)
                
                # 2. Aba de Dados Brutos (mantendo intocado)
                colunas_originais = [c for c in df_raw.columns if c != '_mun_temp']
                df_brutos_out = df_filtrado.drop(columns=['_mun_temp'])[colunas_originais]
                df_brutos_out = df_brutos_out.sort_values(by=[col_municipio_codigo, df_brutos_out.columns[0]])
                df_brutos_out.to_excel(writer, sheet_name="Dados Brutos", index=False)
                formatar_aba(writer.sheets["Dados Brutos"], df_brutos_out, aplicar_larguras_json=False)

        except PermissionError:
            app_instance.atualizar_status("Erro: Arquivo em uso.")
            app_instance.atualizar_interface(messagebox.showerror, "Arquivo Aberto", 
                f"Feche o ficheiro '{nome_final}' e tente novamente.")
            return

        app_instance.atualizar_status("Processo concluído!")
        app_instance.atualizar_interface(messagebox.showinfo, "Sucesso", f"Relatório gerado em:\n{caminho_saida}")

    except Exception as e:
        app_instance.atualizar_status("Erro no processamento.")
        app_instance.atualizar_interface(messagebox.showerror, "Erro", f"Falha crítica: {e}")
    finally:
        app_instance.atualizar_interface(app_instance.start_button.config, state="normal")
        app_instance.atualizar_interface(app_instance.root.config, cursor="")

# ==========================================
# --- INTERFACE GRÁFICA ---
# ==========================================
class AppFiltradorCorrespondentes:
    def __init__(self, root, ufs, df_mun, mun_map):
        self.root, self.ufs, self.df_municipios, self.municipios_map = root, ufs, df_mun, mun_map
        self.root.title("Filtrador de Correspondentes BCB - v2.1")
        self.root.geometry("600x680")

        main_frame = ttk.Frame(root, padding="20")
        main_frame.pack(expand=True, fill=tk.BOTH)
        
        ttk.Label(main_frame, text="Selecione a UF:", font=("Arial", 10, "bold")).pack(anchor="w")
        self.uf_var = tk.StringVar()
        self.uf_combo = ttk.Combobox(main_frame, textvariable=self.uf_var, values=self.ufs, state="readonly")
        self.uf_combo.pack(fill="x", pady=(5, 15))
        self.uf_combo.bind("<<ComboboxSelected>>", self.update_mun)

        ttk.Label(main_frame, text="Selecione o Município:", font=("Arial", 10, "bold")).pack(anchor="w")
        self.mun_var = tk.StringVar()
        self.mun_combo = ttk.Combobox(main_frame, textvariable=self.mun_var, state="disabled")
        self.mun_combo.pack(fill="x", pady=(5, 10))

        ttk.Button(main_frame, text="Incluir Município ⬇", command=self.add_mun).pack(pady=5)
        self.lista_mun = tk.Listbox(main_frame, height=8, font=("Arial", 9))
        self.lista_mun.pack(fill="both", expand=True, pady=5)

        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill="x")
        ttk.Button(btn_frame, text="Remover Selecionado", command=lambda: self.lista_mun.delete(tk.ANCHOR)).pack(side="left", padx=2)
        ttk.Button(btn_frame, text="Limpar Tudo", command=lambda: self.lista_mun.delete(0, tk.END)).pack(side="left", padx=2)

        self.start_button = ttk.Button(main_frame, text="GERAR RELATÓRIO DE CORRESPONDENTES", command=self.start)
        self.start_button.pack(pady=20)
        self.status_label = ttk.Label(main_frame, text="Aguardando comandos...", foreground="gray")
        self.status_label.pack()

    def atualizar_interface(self, func, *args, **kwargs): self.root.after(0, lambda: func(*args, **kwargs))
    def atualizar_status(self, t): self.atualizar_interface(self.status_label.config, text=t)

    def update_mun(self, e):
        muns = self.df_municipios[self.df_municipios['UF'] == self.uf_var.get()]['Nome Municipio'].tolist()
        self.mun_combo.config(values=sorted(muns), state="readonly")
        self.mun_combo.set("")

    def add_mun(self):
        u, m = self.uf_var.get(), self.mun_var.get()
        if u and m:
            item = f"{u} - {m}"
            if item not in self.lista_mun.get(0, tk.END): self.lista_mun.insert(tk.END, item)

    def start(self):
        itens = self.lista_mun.get(0, tk.END)
        if not itens: return messagebox.showerror("Erro", "Selecione municípios para filtrar.")
        m_alvo = {}
        for it in itens:
            uf, nome = it.split(" - ")
            cod = self.municipios_map.get((uf, nome))
            if cod: m_alvo[re.sub(r'\D', '', str(cod))[:6]] = nome
        self.start_button.config(state="disabled"); self.root.config(cursor="watch")
        threading.Thread(target=processar_correspondentes, args=(m_alvo, self), daemon=True).start()

if __name__ == "__main__":
    if not ARQUIVO_MUNICIPIOS.exists():
        print(f"Erro: {ARQUIVO_MUNICIPIOS} não encontrado. Certifique-se de que a pasta e o ficheiro existem.")
    else:
        try:
            df_mun = pd.read_csv(ARQUIVO_MUNICIPIOS, delimiter=';', dtype=str, encoding='utf-8')
            df_mun.columns = [c.strip().strip(',') for c in df_mun.columns]
            ufs_lista = sorted(df_mun['UF'].unique())
            mapa_indices = df_mun.set_index(['UF', 'Nome Municipio'])['Codigo Municipio'].to_dict()
            root_tk = tk.Tk()
            AppFiltradorCorrespondentes(root_tk, ufs_lista, df_mun, mapa_indices)
            root_tk.mainloop()
        except Exception as err: print(f"Erro ao iniciar GUI: {err}")