from fastapi import FastAPI
from fastapi.responses import JSONResponse
import pandas as pd
import requests
from io import StringIO

app = FastAPI()

@app.get("/dados_afogamentos")
def get_dados():
    urls = {
        ("Menor de 1 ano", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A13132110_14_8_2.csv",
        ("1 a 4 anos", "Masculino"): "http://tabnet.saude.mg.gov.br/csv/A13130210_14_8_2.csv",
        ("Menor de 1 ano", "Feminino"): "http://tabnet.saude.mg.gov.br/csv/A13335310_14_8_2.csv"
    }

    todos_dfs = []

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
            todos_dfs.append(df_final)

        except Exception as e:
            continue

    df_total = pd.concat(todos_dfs, ignore_index=True)
    result = df_total.to_dict(orient="records")
    return JSONResponse(content=result)
