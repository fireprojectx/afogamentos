
import pandas as pd
import requests
from io import StringIO
import sqlite3

# Conexão com banco de dados SQLite
conn = sqlite3.connect("afogamentos.db")
cursor = conn.cursor()

# Cria a tabela (dropa se já existir)
cursor.execute("DROP TABLE IF EXISTS obitos")
cursor.execute("""
CREATE TABLE obitos (
    codigo_municipio TEXT,
    municipio TEXT,
    mes TEXT,
    ano TEXT,
    sexo TEXT,
    faixa_etaria TEXT,
    obitos INTEGER
)
""")
conn.commit()

# Dicionário com alguns links de exemplo (adicione todos os links reais aqui)
urls = {
    ("Menor de 1 ano", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A13132110_14_8_2.csv",
    ("1 a 4 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A13130210_14_8_2.csv",
    ("Menor de 1 ano", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A13335310_14_8_2.csv",
    ("1 a 4 anos", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A13342210_14_8_2.csv",
}

# Processamento
for (faixa, sexo), url in urls.items():
    try:
        r = requests.get(url)
        r.encoding = 'latin1'
        csv_data = r.text
        df = pd.read_csv(StringIO(csv_data), sep=";", skiprows=6, skipfooter=8, engine="python")

        if "Total" in df.columns:
            df.drop(columns=["Total"], inplace=True)
        df.replace("-", 0, inplace=True)
        df[['Codigo municipio', 'municipio']] = df['Município'].str.extract(r'"?(\d+)"?\s*(.*)')
        df.drop(columns=["Município"], inplace=True)
        df_long = df.melt(id_vars=["Codigo municipio", "municipio"], var_name="mes_ano", value_name="Óbitos")
        df_long["mes_ano"] = df_long["mes_ano"].str.replace("..", "", regex=False)
        df_long[["mês", "ano"]] = df_long["mes_ano"].str.extract(r'(\w+)\/(\d{4})')
        df_long = df_long.dropna(subset=["Codigo municipio", "municipio"])
        df_long["Óbitos"] = pd.to_numeric(df_long["Óbitos"], errors="coerce").fillna(0).astype(int)
        df_long["sexo"] = sexo
        df_long["faixa etária"] = faixa
        df_final = df_long[["Codigo municipio", "municipio", "mês", "ano", "sexo", "faixa etária", "Óbitos"]]
        df_final.columns = ["codigo_municipio", "municipio", "mes", "ano", "sexo", "faixa_etaria", "obitos"]

        df_final.to_sql("obitos", conn, if_exists="append", index=False)
    except Exception as e:
        print(f"Erro ao processar {faixa} - {sexo}: {e}")

conn.close()
print("✅ Banco de dados atualizado com sucesso.")
