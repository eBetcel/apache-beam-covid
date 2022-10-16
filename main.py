"""
This script reads two csv files with covid and Brazilian states data, 
processing the data and producing a csv and json file using apache beam 
"""
import json
import apache_beam as beam

csv_header = "Regiao, Estado, UF, Governador, TotalCasos, TotalObitos"

def join_dicts(a, b):
    a.update(b.get(a["coduf"]) or {})
    return a
"""
    Args: 
        a: dict containing covid data
        b: dict containing states data
    
    Return: 
        a: dict containing covid data with states data information

"""

with beam.Pipeline() as pipeline:
    dados_covid = (
        pipeline
        | "Read csv hist painel covid"
        >> beam.io.ReadFromText("hist_painel_covid.csv", skip_header_lines=True)
        | "Split painel covid" >> beam.Map(lambda x: x.split(";"))
        | "Select columns painel covid"
        >> beam.Map(lambda x: ((x[0], x[1], x[3]), (x[11], x[13])))
        | "Group by state and sum deaths and cases"
        >> beam.CombinePerKey(
            lambda l: (sum([int(a) for a, _ in l]), sum([int(b) for _, b in l]))
        )
        | "Format output"
        >> beam.Map(
            lambda x: {
                "coduf": x[0][2],
                "Regiao": x[0][0],
                "UF": x[0][1],
                "TotalCasos": x[1][0],
                "TotalObitos": x[1][1],
            }
        )
    )

    estados_ibge = (
        pipeline
        | "Read csv estados ibge"
        >> beam.io.ReadFromText("EstadosIBGE.csv", skip_header_lines=True)
        | "Split estados_ibge file" >> beam.Map(lambda x: x.split(";"))
        | "Select columns from ibge files"
        >> beam.Map(lambda x: (x[1], {"Estado": x[0], "Governador": x[3]}))
    )

    #Transforms estados_ibge in a asdict-wrapper around the Pcol of estados_ibge
    estados_dict = beam.pvalue.AsDict(estados_ibge)

    joined_dicts = dados_covid | "Left join of dicts" >> beam.Map(join_dicts, b=estados_dict)

    data_to_csv = (
        joined_dicts
        | "Unpack dict to csv format"
        >> beam.Map(
            lambda x: f'{x["Regiao"]},{x.get("Estado", "")},{x.get("UF", "")},{x.get("Governador", "")},{x["TotalCasos"]},{x["TotalObitos"]}'
        )
        | "Write CSV file"
        >> beam.io.WriteToText(
            "covid_brasil", file_name_suffix=".csv", header=csv_header
        )
    )

    prepare_data_to_json = joined_dicts | "Format dict to be converted to json" >> beam.Map(
        lambda x: {
            "Regiao": x["Regiao"],
            "Estado": x.get("Estado", ""),
            "UF": x.get("UF", ""),
            "Governador": x.get("Governador", ""),
            "TotalCasos": x["TotalCasos"],
            "TotalObitos": x["TotalObitos"],
        }
    )

    data_to_json = (
        prepare_data_to_json
        | "Generates bounded data" >> beam.Map(lambda x: (1, x))
        | "GroupByKey to generate a list and fake key" >> beam.GroupByKey()
        | "Dumps json" >> beam.Map(lambda x: json.dumps(x[1], ensure_ascii=False).encode("utf8"))
        | "Writing json file" >> beam.io.WriteToText("covid_brasil", file_name_suffix=".json")
    )

