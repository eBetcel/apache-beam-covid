import apache_beam as beam
import csv

csv_header = "Regiao, Estado, UF, Governador, TotalCasos, TotalObitos"


def select_columns_ibge(element):
    element.pop(2)
    element = element[0:3]
    return element[1], element


with beam.Pipeline() as pipeline:
    dados_covid = (
        pipeline
        | "Read csv hist painel covid"
        >> beam.io.ReadFromText("hist_painel_covid.csv", skip_header_lines=True)
        | "Split painel covid" >> beam.Map(lambda x: x.split(";"))
        | "Select columns painel covid"
        >> beam.Map(lambda x: ((x[0], x[1], x[3]), (x[11], x[13])))
        | "Group by UF"
        >> beam.CombinePerKey(
            lambda l: (sum([int(a) for a, _ in l]), sum([int(b) for _, b in l]))
        )
        | "Formmat output"
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
        | "Split" >> beam.Map(lambda x: x.split(";"))
        | "Select columns"
        >> beam.Map(lambda x: (x[1], {"Estado": x[0], "Governador": x[3]}))
    )

    estados_dict = beam.pvalue.AsDict(estados_ibge)

    def join_dicts(a, b):
        a.update(b.get(a["coduf"]) or {})
        return a

    joined_dicts = dados_covid | beam.Map(join_dicts, b=estados_dict)

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

    data_to_json = joined_dicts | beam.Map(print)
