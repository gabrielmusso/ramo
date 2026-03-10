import zipfile
import datetime
import logging
import re
from pathlib import Path
from typing import Tuple, Optional, Iterator
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

PASTA_DESTINO = Path("~/repos/ramo/dados").expanduser()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# ==========================================
# --- ESTRUTURA DE DADOS (DATACLASSES) ---
# ==========================================
@dataclass
class BaseBCB:
    """Estrutura que define as propriedades de cada base de dados a ser atualizada."""
    id: str
    nome: str
    sufixo_arquivo: str
    url_base: str

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
    )
]

# ==========================================
# --- FUNÇÕES AUXILIARES ---
# ==========================================
def iterar_ultimos_meses(quantidade: int = 12) -> Iterator[int]:
    """Gera iterativamente os formatos YYYYMM dos últimos X meses."""
    hoje = datetime.date.today()
    ano, mes = hoje.year, hoje.month
    
    for _ in range(quantidade):
        yield int(f"{ano}{mes:02d}")
        mes -= 1
        if mes == 0:
            mes = 12
            ano -= 1

def obter_versao_local(base: BaseBCB) -> int:
    """
    Verifica fisicamente na pasta de destino qual é a versão mais recente
    já extraída para esta base de dados (procura ficheiros com o sufixo e YYYYMM).
    """
    maior_versao = 0
    sufixo_alvo = base.sufixo_arquivo.upper()
    
    if PASTA_DESTINO.exists():
        for ficheiro in PASTA_DESTINO.iterdir():
            if ficheiro.is_file():
                nome_arquivo = ficheiro.name.upper()
                
                # Verifica se o ficheiro pertence a esta base (ex: tem 'CORRESPONDENTES' no nome)
                if sufixo_alvo in nome_arquivo:
                    # Extrai os 6 dígitos (YYYYMM) do nome do ficheiro
                    match = re.search(r'(\d{6})', nome_arquivo)
                    if match:
                        versao_ficheiro = int(match.group(1))
                        if versao_ficheiro > maior_versao:
                            maior_versao = versao_ficheiro
                            
    return maior_versao

# ==========================================
# --- MOTOR PRINCIPAL ---
# ==========================================
def buscar_link_diretamente_no_servidor(session: requests.Session, base: BaseBCB) -> Tuple[Optional[int], Optional[str]]:
    """Tenta aceder diretamente às URLs ocultas usando uma sessão HTTP persistente."""
    logger.info(f"A procurar ficheiros para a base: {base.nome}...")
    
    for versao_int in iterar_ultimos_meses(12):
        versao_str = str(versao_int)
        logger.info(f"A verificar a existência do ficheiro de {versao_str}...")
        
        # O BCB costuma cometer erros de digitação (espaços antes do .zip).
        variacoes_url = [
            f"{base.url_base}/{versao_str}{base.sufixo_arquivo}.zip",
            f"{base.url_base}/{versao_str}{base.sufixo_arquivo}%20.zip",
            f"{base.url_base}/{versao_str}{base.sufixo_arquivo} .zip"
        ]
        
        for url in variacoes_url:
            try:
                # O bloco 'with' garante que o fluxo é fechado, mas a conexão subjacente da 'session' continua aberta
                with session.get(url, stream=True, timeout=5) as resposta:
                    if resposta.status_code == 200:
                        tipo_conteudo = resposta.headers.get('Content-Type', '')
                        
                        if 'zip' in tipo_conteudo.lower() or 'octet-stream' in tipo_conteudo.lower():
                            return versao_int, url
                            
            except requests.RequestException:
                continue

    return None, None

def baixar_e_extrair(session: requests.Session, url_download: str, versao: int, base: BaseBCB) -> bool:
    """Faz o download do arquivo ZIP, extrai e substitui a base local."""
    caminho_zip = PASTA_DESTINO / f"temp_{base.id}_{versao}.zip"
    
    logger.info(f"[{base.nome}] A iniciar o download da versão {versao}...")
    logger.info(f"URL: {url_download}")
    
    try:
        with session.get(url_download, stream=True, timeout=30) as r:
            r.raise_for_status()
            with caminho_zip.open('wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
        logger.info(f"[{base.nome}] Download concluído! A descompactar...")
        
        with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
            zip_ref.extractall(PASTA_DESTINO)
            
        logger.info(f"[{base.nome}] Ficheiros extraídos com sucesso na pasta '{PASTA_DESTINO}'.")
        return True
        
    except Exception as e:
        logger.error(f"[{base.nome}] Erro durante o download ou extração: {e}")
        return False
        
    finally:
        if caminho_zip.exists():
            caminho_zip.unlink()

def executar_atualizacao() -> None:
    logger.info("=== INÍCIO DA VERIFICAÇÃO BCB (MÉTODO DIRETO / URL GUESSING) ===")
    PASTA_DESTINO.mkdir(parents=True, exist_ok=True)
    
    # Inicia a sessão HTTP única para todas as requisições
    with requests.Session() as session:
        session.headers.update(HEADERS)
        
        for base in BASES_DE_DADOS:
            logger.info("-" * 50)
            logger.info(f"PROCESSANDO BASE: {base.nome.upper()}")
            
            # O robô agora vasculha a pasta real para ver o que tem lá dentro
            versao_local = obter_versao_local(base)
            
            logger.info(f"Versão local atual (encontrada na pasta): {versao_local if versao_local > 0 else 'Nenhuma'}")
            
            versao_web, link_web = buscar_link_diretamente_no_servidor(session, base)
            
            if not versao_web or not link_web:
                logger.warning(f"Não foi possível localizar o ficheiro para '{base.nome}'.")
                continue
                
            logger.info(f"✅ Encontrada! Versão mais recente nos servidores: {versao_web}")
            
            if versao_web > versao_local:
                logger.info("ATUALIZAÇÃO NECESSÁRIA! A iniciar processo...")
                
                sucesso = baixar_e_extrair(session, link_web, versao_web, base)
                
                if sucesso:
                    logger.info(f"Base '{base.nome}' atualizada para a versão {versao_web} com sucesso!")
                else:
                    logger.error(f"A atualização da base '{base.nome}' falhou.")
            else:
                logger.info(f"A base '{base.nome}' já tem a versão mais atual na sua pasta. Nenhuma ação necessária.")
            
    logger.info("=" * 50)
    logger.info("VERIFICAÇÃO TOTAL CONCLUÍDA!")

if __name__ == "__main__":
    executar_atualizacao()