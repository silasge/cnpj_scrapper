import json
import time

import duckdb
import pandas as pd
import requests
from tqdm import tqdm



def api_rfb(cnpj):
    try:
        url = 'https://brasilapi.com.br/api/cnpj/v1/' + str(cnpj)
        header = {
            "User-Agent": "Mozilla/5.0 (X11; Linuxx86_64) AppleWebKit/537.36 (KHTML, likeGecko) Chrome/50.0.2661.75 Safari/537.36",
            "X-Requested-With":"XMLHttpRequest"
        }
        r = requests.get(url, headers=header)
        api_response = json.loads(r.text)
        time.sleep(0.5)
        target = ('cnae_fiscal','cnae_fiscal_descricao','codigo_natureza_juridica', 'natureza_juridica','codigo_porte', 'porte', 'opcao_pelo_mei')
        cnpj_ = {"cnpj": cnpj}
        infos = {k: api_response[k] for k in target}
        dados = {**cnpj_, **infos}
    except:
        dados = {
            'cnpj': cnpj,
            'cnae_fiscal': None,
            'cnae_fiscal_descricao': None,
            'codigo_natureza_juridica': None,
            'natureza_juridica': None,
            'codigo_porte': None,
            'porte': None,
            'opcao_pelo_mei': None
        }     
    return dados


def duckdbconn():
    return duckdb.connect("./data/cnpjs.duckdb")


def create_table(conn):
    conn.execute(
        """ 
        CREATE TABLE IF NOT EXISTS tb_cnpjs (
            cnpj VARCHAR,
            cnae_fiscal VARCHAR,
            cnae_fiscal_descricao VARCHAR,
            codigo_natureza_juridica VARCHAR,
            natureza_juridica VARCHAR,
            codigo_porte VARCHAR,
            porte VARCHAR,
            opcao_pelo_mei VARCHAR
        );
        """
    )

def get_and_insert_cnpj_info_in_db():
    cnpjs = pd.read_csv("./data/cnpjs.csv", dtype={"nrCpfCnpjCliente": str})
    conn = duckdbconn()
    create_table(conn=conn)
    existing_cnpjs = [
        cnpj_[0] for cnpj_ in conn.execute(
            "SELECT DISTINCT cnpj FROM tb_cnpjs;"
        ).fetchall()
    ]
    new_cnpjs = cnpjs.query("nrCpfCnpjCliente not in @existing_cnpjs")["nrCpfCnpjCliente"]
    try:
        with tqdm(new_cnpjs) as pbar:
            for new_cnpj in pbar:
                pbar.set_description(f"Processando CNPJ {new_cnpj}...")
                info_cnpj = api_rfb(new_cnpj)
                conn.execute(
                    """ 
                    INSERT INTO tb_cnpjs VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    
                    """,
                    [pj for pj in info_cnpj.values()]
                )
                conn.commit()
    except Exception as e:
        print(e)
        conn.close()
    
    conn.close()
    