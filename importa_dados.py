import zipfile
import datetime
import logging
import re
import time
from pathlib import Path
from typing import Tuple, Optional, Iterator, List
from dataclasses import dataclass

import requests

# ==========================================
# --- CONFIGURAÇÕES E LOGGING ---
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Pasta onde os dados serão armazenados
PASTA_DESTINO = Path("~/repos/ramo/dados").expanduser()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# ==========================================
# --- ESTRUTURA DE DADOS ---
# ==========================================
@dataclass
class BaseBCB:
    """Define as propriedades de cada base de dados do BCB."""
    id: str
    nome: str
    sufixo_arquivo: str
    url_base: str

@dataclass
class ResultadoProcessamento:
    """Armazena o resultado da execução para o relatório final."""
    base_nome: str
    versao_anterior: int
    versao_nova: int
    status: str
    mensagem: str = ""

# Lista de bases configuradas
BASES_DE_DADOS = [
    BaseBCB(
        id="correspondentes",
        nome="Correspondentes no País",
        sufixo_arquivo="CORRESPONDENTES",
        url_base="https://www.bcb.gov.br/content/estabilidadefinanceira/relacao_instituicoes_funcionamento/correspondentes_pais"
    ),
    BaseBCB(
        id="postos",
        nome="Postos de Atendimento",
        sufixo_arquivo="POSTOS",
        url_base="https://www.bcb.gov.br/content/estabilidadefinanceira/agenciasconsorcio/postos"
    ),
    BaseBCB(
        id="cooperativas",
        nome="Cooperativas de Crédito",
        sufixo_arquivo="COOPERATIVAS",
        url_base="https://www.bcb.gov.br/content/estabilidadefinanceira/relacao_instituicoes_funcionamento/Cooperativas-de-credito"
    )
]

# ==========================================
# --- UTILITÁRIOS ---
# ==========================================
def iterar_ultimos_meses(quantidade: int = 12) -> Iterator[int]:
    """Gera o formato YYYYMM dos últimos X meses de forma decrescente."""
    hoje = datetime.date.today()
    ano, mes = hoje.year, hoje.month
    
    for _ in range(quantidade):
        yield int(f"{ano}{mes:02d}")
        mes -= 1
        if mes == 0:
            mes = 12
            ano -= 1

def obter_versao_local(base: BaseBCB) -> int:
    """Identifica a maior versão YYYYMM já baixada (checa apenas .xlsx)."""
    maior_versao = 0
    if not PASTA_DESTINO.exists():
        return 0
        
    sufixo_alvo = base.sufixo_arquivo.upper()
    for ficheiro in PASTA_DESTINO.iterdir():
        # Verifica se é um arquivo Excel desta base
        if ficheiro.is_file() and ficheiro.suffix.lower() == ".xlsx" and sufixo_alvo in ficheiro.name.upper():
            match = re.search(r'(\d{6})', ficheiro.name)
            if match:
                versao = int(match.group(1))
                if versao > maior_versao:
                    maior_versao = versao
    return maior_versao

def remover_arquivos_antigos(base: BaseBCB, versao_atual: int):
    """Remove arquivos Excel com versão inferior à atual da mesma base."""
    sufixo_alvo = base.sufixo_arquivo.upper()
    for ficheiro in PASTA_DESTINO.iterdir():
        if ficheiro.is_file() and ficheiro.suffix.lower() == ".xlsx" and sufixo_alvo in ficheiro.name.upper():
            match = re.search(r'(\d{6})', ficheiro.name)
            if match and int(match.group(1)) < versao_atual:
                try:
                    ficheiro.unlink()
                    logger.info(f"Removido Excel antigo: {ficheiro.name}")
                except Exception as e:
                    logger.warning(f"Erro ao remover {ficheiro.name}: {e}")

# ==========================================
# --- CLIENTE DE DOWNLOAD ---
# ==========================================
class BCBDownloader:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def request_com_retry(self, url: str, stream: bool = False, timeout: int = 10) -> Optional[requests.Response]:
        """Realiza a requisição com até 3 tentativas simples."""
        for i in range(3):
            try:
                response = self.session.get(url, stream=stream, timeout=timeout)
                return response
            except requests.RequestException:
                wait = (i + 1) * 2
                time.sleep(wait)
        return None

    def buscar_link(self, base: BaseBCB) -> Tuple[Optional[int], Optional[str]]:
        """Tenta encontrar o link de download testando variações de URL nos últimos meses."""
        logger.info(f"Pesquisando versões para: {base.nome}")
        for versao in iterar_ultimos_meses(12):
            v_str = str(versao)
            urls = [
                f"{base.url_base}/{v_str}{base.sufixo_arquivo}.zip",
                f"{base.url_base}/{v_str}{base.sufixo_arquivo}%20.zip",
                f"{base.url_base}/{v_str}{base.sufixo_arquivo} .zip"
            ]
            for url in urls:
                resp = self.request_com_retry(url, stream=True)
                if resp and resp.status_code == 200:
                    return versao, url
        return None, None

    def baixar_e_extrair(self, url: str, versao: int, base: BaseBCB) -> bool:
        """Faz o download do ZIP e extrai o Excel para a pasta destino."""
        temp_zip = PASTA_DESTINO / f"temp_{base.id}_{versao}.zip"
        try:
            resp = self.request_com_retry(url, stream=True, timeout=60)
            if not resp or resp.status_code != 200:
                return False

            with temp_zip.open('wb') as f:
                for chunk in resp.iter_content(chunk_size=16384):
                    f.write(chunk)
            
            with zipfile.ZipFile(temp_zip, 'r') as z:
                # Extrai tudo diretamente na pasta destino
                z.extractall(PASTA_DESTINO)
            
            logger.info(f"✅ Download e extração concluídos para {base.id}.")
            return True
        except Exception as e:
            logger.error(f"Erro no download/extração de {base.id}: {e}")
            return False
        finally:
            if temp_zip.exists():
                temp_zip.unlink()

# ==========================================
# --- EXECUÇÃO PRINCIPAL ---
# ==========================================
def executar_atualizacao():
    logger.info("=== INICIANDO ATUALIZADOR BCB (MODO DOWNLOAD DIRETO .XLSX) ===")
    PASTA_DESTINO.mkdir(parents=True, exist_ok=True)
    
    downloader = BCBDownloader()
    relatorio: List[ResultadoProcessamento] = []

    for base in BASES_DE_DADOS:
        logger.info("-" * 40)
        versao_local = obter_versao_local(base)
        versao_web, link_web = downloader.buscar_link(base)
        
        if not versao_web:
            relatorio.append(ResultadoProcessamento(base.nome, versao_local, 0, "FALHA", "Arquivo não encontrado no servidor"))
            continue

        if versao_web > versao_local:
            logger.info(f"Atualização disponível: {versao_web} (Local: {versao_local if versao_local > 0 else 'Nenhuma'})")
            
            if downloader.baixar_e_extrair(link_web, versao_web, base):
                remover_arquivos_antigos(base, versao_web)
                relatorio.append(ResultadoProcessamento(base.nome, versao_local, versao_web, "ATUALIZADO"))
            else:
                relatorio.append(ResultadoProcessamento(base.nome, versao_local, versao_web, "ERRO", "Falha no download"))
        else:
            logger.info(f"Base '{base.nome}' já está atualizada ({versao_local}).")
            relatorio.append(ResultadoProcessamento(base.nome, versao_local, versao_local, "OK"))

    # Exibição do Log Final
    logger.info("=" * 40)
    logger.info("RESUMO DA OPERAÇÃO:")
    for item in relatorio:
        msg = f"- {item.base_nome}: {item.status}"
        if item.status == "ATUALIZADO":
            msg += f" (Versão Baixada: {item.versao_nova})"
        elif item.mensagem:
            msg += f" [{item.mensagem}]"
        logger.info(msg)
    logger.info("=" * 40)

if __name__ == "__main__":
    executar_atualizacao()