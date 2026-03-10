import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
import threading
import os
import re
import json
import sys
from pathlib import Path

# ==========================================
# --- CARREGAMENTO DE CONFIGURAÇÕES ---
# ==========================================
def carregar_configuracoes():
    """Carrega o ficheiro de configuração partilhado do projeto."""
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"ERRO FATAL: Não foi possível ler 'config.json'.\nDetalhes: {e}")
        sys.exit(1)

CONFIG = carregar_configuracoes()

# Mapeamento de Configurações Globais
GLOBais = CONFIG['configuracoes_globais']
PASTA_DADOS = Path(GLOBais['pasta_dados']).expanduser()
PASTA_SAIDA = Path(GLOBais['pasta_saida']).expanduser()
ARQUIVO_MUNICIPIOS = PASTA_DADOS / GLOBais['arquivo_municipios']
CODIFICACAO = GLOBais.get('codificacao', 'latin-1')

# Configurações de Excel específicas para Postos
POSTOS_EXCEL_CFG = CONFIG.get('postos_excel_config', {})

# Garante que a pasta de relatórios existe
PASTA_SAIDA.mkdir(parents=True, exist_ok=True)

# ==========================================
# --- FUNÇÕES DE SUPORTE (ROBUSTEZ) ---
# ==========================================

def detectar_linha_cabecalho(caminho_arquivo: Path) -> int:
    """
    Analisa as primeiras linhas do ficheiro para detetar onde começa a tabela real.
    Identifica a linha com a maior densidade de colunas preenchidas.
    """
    try:
        if caminho_arquivo.suffix.lower() == '.xlsx':
            df_sample = pd.read_excel(caminho_arquivo, header=None, nrows=30)
        else:
            df_sample = pd.read_csv(caminho_arquivo, sep=';', encoding=CODIFICACAO, header=None, nrows=30)
        
        contagem_colunas = df_sample.count(axis=1)
        return int(contagem_colunas.idxmax())
    except Exception:
        return 0

# ==========================================
# --- MOTOR DE PROCESSAMENTO ---
# ==========================================
def processar_postos(municipios_alvo, app_instance):
    """
    Sub-rotina de processamento: carga, limpeza, filtragem e geração de Excel.
    """
    try:
        app_instance.atualizar_status("A procurar o ficheiro de Postos mais recente...")
        
        # 1. Localização do ficheiro
        arquivos = list(PASTA_DADOS.glob("*POSTOS*"))
        arquivos_validos = [f for f in arquivos if f.suffix.lower() in ['.csv', '.txt', '.xlsx']]
        
        if not arquivos_validos:
            raise FileNotFoundError("Nenhum ficheiro 'POSTOS' encontrado na pasta de dados.")
            
        arquivo_recente = sorted(arquivos_validos, reverse=True)[0]
        index_cabecalho = detectar_linha_cabecalho(arquivo_recente)
        
        app_instance.atualizar_status(f"A carregar: {arquivo_recente.name}...")
        
        # 2. Leitura dos dados
        if arquivo_recente.suffix.lower() == '.xlsx':
            df = pd.read_excel(arquivo_recente, dtype=str, skiprows=index_cabecalho)
        else:
            df = pd.read_csv(arquivo_recente, sep=';', encoding=CODIFICACAO, dtype=str, skiprows=index_cabecalho)
        
        if df.empty: raise ValueError("O ficheiro lido parece estar vazio.")

        # --- LIMPEZA DE ESPAÇOS NO CABEÇALHO ---
        df.columns = [str(col).strip() for col in df.columns]

        # 3. LIMPEZA GLOBAL DE ESPAÇOS NOS DADOS (Vetorizada)
        app_instance.atualizar_status("A remover espaços em branco de todos os campos...")
        df = df.apply(lambda x: x.str.strip() if hasattr(x, "str") else x)

        # 4. FILTRAGEM POR SEGMENTO (Coluna C / Índice 2)
        app_instance.atualizar_status("A filtrar por Segmento (Coluna C)...")
        termos_permitidos = [
            "COOPERATIVA DE CRÉDITO",
            "SOCIEDADE DE CRÉDITO AO MICROEMPREENDEDOR",
            "SOCIEDADE DE CRÉDITO DIRETO",
            "SOCIEDADE DE CRÉDITO, FINANCIAMENTO E INVESTIMENTO"
        ]

        def validar_segmento(val):
            if pd.isna(val): return True
            v = str(val).upper()
            return v in ["", "NAN"] or v in termos_permitidos

        if len(df.columns) > 2:
            df = df[df.iloc[:, 2].apply(validar_segmento)].copy()
            
        # 5. FILTRAGEM POR MUNICÍPIO (Última Coluna, 6 dígitos)
        ultima_col = df.columns[-1]
        df['_mun_temp'] = df[ultima_col].str.slice(0, 6)
        
        nome_final = "Postos_Filtrados_Consolidado.xlsx" if len(municipios_alvo) > 1 else f"Postos_{list(municipios_alvo.values())[0]}.xlsx"
        caminho_saida = PASTA_SAIDA / nome_final
        
        app_instance.atualizar_status("A aplicar estilos e gerar Excel...")
        
        sucesso_global = False
        with pd.ExcelWriter(caminho_saida, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Estilos baseados no config.json
            font_name = POSTOS_EXCEL_CFG.get('fonte_padrao', 'Montserrat')
            font_size_body = POSTOS_EXCEL_CFG.get('font_size_corpo', 9)
            
            head_cfg = POSTOS_EXCEL_CFG.get('estilo_cabecalho', {})
            fmt_header = workbook.add_format({
                'font_name': font_name,
                'bold': head_cfg.get('bold', True),
                'align': head_cfg.get('align', 'center'),
                'font_size': head_cfg.get('font_size', 10),
                'bg_color': head_cfg.get('bg_color', '#D3D3D3'),
                'bottom': head_cfg.get('bottom', 1),
                'text_wrap': head_cfg.get('text_wrap', True),
                'border': 1
            })
            
            fmt_body = workbook.add_format({'font_name': font_name, 'font_size': font_size_body})
            margens = POSTOS_EXCEL_CFG.get('margens', {})

            for cod6, nome_mun in municipios_alvo.items():
                df_sub = df[df['_mun_temp'] == cod6].copy()
                if not df_sub.empty:
                    sucesso_global = True
                    df_sub = df_sub.drop(columns=['_mun_temp'])
                    
                    # --- ORDENAÇÃO POR CNPJ (Coluna A / Índice 0) ---
                    df_sub = df_sub.sort_values(by=df_sub.columns[0])
                    
                    nome_aba = re.sub(r'[\\/*?:"<>|]', '', nome_mun)[:31]
                    df_sub.to_excel(writer, sheet_name=nome_aba, index=False)
                    ws = writer.sheets[nome_aba]
                    
                    ws.set_margins(
                        left=margens.get('left', 0.5),
                        right=margens.get('right', 0.5),
                        top=margens.get('top', 0.75),
                        bottom=margens.get('bottom', 0.75)
                    )
                    
                    larguras_map = POSTOS_EXCEL_CFG.get('largura_colunas_especificas', {})
                    largura_padrao = larguras_map.get('Default', 18)

                    for col_num, col_name in enumerate(df_sub.columns.values):
                        largura = largura_padrao
                        for key, val in larguras_map.items():
                            if key.upper() in col_name.upper():
                                largura = val
                                break
                        ws.write(0, col_num, col_name, fmt_header)
                        ws.set_column(col_num, col_num, largura, fmt_body)

        if not sucesso_global:
            if caminho_saida.exists(): caminho_saida.unlink()
            app_instance.atualizar_interface(messagebox.showwarning, "Aviso", "Sem resultados encontrados.")
        else:
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
class AppFiltrador:
    def __init__(self, root, ufs, df_mun, mun_map):
        self.root, self.ufs, self.df_municipios, self.municipios_map = root, ufs, df_mun, mun_map
        self.root.title("Filtrador de Postos - Limpeza e Segmento")
        self.root.geometry("600x680")

        main_frame = ttk.Frame(root, padding="20")
        main_frame.pack(expand=True, fill=tk.BOTH)
        
        ttk.Label(main_frame, text="UF:", font=("Arial", 10, "bold")).pack(anchor="w")
        self.uf_var = tk.StringVar()
        self.uf_combo = ttk.Combobox(main_frame, textvariable=self.uf_var, values=self.ufs, state="readonly")
        self.uf_combo.pack(fill="x", pady=(5, 15))
        self.uf_combo.bind("<<ComboboxSelected>>", self.update_mun)

        ttk.Label(main_frame, text="Município:", font=("Arial", 10, "bold")).pack(anchor="w")
        self.mun_var = tk.StringVar()
        self.mun_combo = ttk.Combobox(main_frame, textvariable=self.mun_var, state="disabled")
        self.mun_combo.pack(fill="x", pady=(5, 10))

        ttk.Button(main_frame, text="Incluir Município ⬇", command=self.add_mun).pack(pady=5)
        self.lista_mun = tk.Listbox(main_frame, height=8)
        self.lista_mun.pack(fill="both", expand=True, pady=5)

        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill="x")
        ttk.Button(btn_frame, text="Remover Último", command=lambda: self.lista_mun.delete(tk.END)).pack(side="left", padx=2)
        ttk.Button(btn_frame, text="Limpar Lista", command=lambda: self.lista_mun.delete(0, tk.END)).pack(side="left", padx=2)

        self.start_button = ttk.Button(main_frame, text="GERAR RELATÓRIO DE POSTOS", command=self.start)
        self.start_button.pack(pady=20)
        self.status_label = ttk.Label(main_frame, text="Pronto.", foreground="blue")
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
        if not itens: return messagebox.showerror("Erro", "Selecione municípios.")
        m_alvo = {}
        for it in itens:
            uf, nome = it.split(" - ")
            cod = self.municipios_map.get((uf, nome))
            if cod: m_alvo[re.sub(r'\D', '', str(cod))[:6]] = nome
        self.start_button.config(state="disabled"); self.root.config(cursor="watch")
        threading.Thread(target=processar_postos, args=(m_alvo, self), daemon=True).start()

if __name__ == "__main__":
    if not ARQUIVO_MUNICIPIOS.exists():
        print(f"Erro: {ARQUIVO_MUNICIPIOS} não encontrado.")
    else:
        try:
            df_mun = pd.read_csv(ARQUIVO_MUNICIPIOS, delimiter=';', dtype=str, encoding='utf-8')
            df_mun.columns = [c.strip().strip(',') for c in df_mun.columns]
            ufs_lista = sorted(df_mun['UF'].unique())
            mapa_indices = df_mun.set_index(['UF', 'Nome Municipio'])['Codigo Municipio'].to_dict()
            root_tk = tk.Tk()
            AppFiltrador(root_tk, ufs_lista, df_mun, mapa_indices)
            root_tk.mainloop()
        except Exception as err: print(f"Erro: {err}")