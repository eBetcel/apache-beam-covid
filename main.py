import apache_beam as beam
import json

def select_columns_ibge(element):
    element.pop(2)
    element = element[0:3]
    return tuple(element)

# def select_columns_hist_painel_covid(element):
#     indices = [0, 1, 3, 11, 13]
#     selected_columns = [element[index] for index in indices]

#     return selected_columns


class create_column_columns_csv(beam.DoFn):
    def __init__(self):
        return
    def process(self, line):
        pass

class sum_deaths(beam.DoFn):
    def __init__(self):
        return
    def process(self, tup):
        values = tup[1]
        total_obitos = 0
        total_casos = 0

        for value in values:
            total_obitos += int(value[4])
            total_casos += int(value[3])
        
        values = [values[0][0], values[0][1], values[0][2], total_casos, total_obitos]
        new_tuple_to_list = list(tup)
        new_tuple_to_list[1] = values
        yield new_tuple_to_list
        
with beam.Pipeline() as pipeline:
    estados_ibge = (
        pipeline
        | "Read csv estados ibge" >> beam.io.ReadFromText("EstadosIBGE.csv", skip_header_lines=True)
        | "Split" >> beam.Map(lambda x: x.split(';'))
        | "Select columns" >> beam.Map(select_columns_ibge)
        | beam.Map(print)
    )

    # dados_covid = (
    #     pipeline
    #     | "Read csv hist painel covid" >> beam.io.ReadFromText("hist_painel_covid.csv", skip_header_lines= True)
    #     | "Split painel covid" >> beam.Map(lambda x: x.split(';'))
    #     | "Select columns painel covid" >> beam.Map(lambda x: [x[index] for index in [0, 1, 3, 11, 13]])
    #     | 'Group by UF' >> beam.GroupBy(lambda x: x[2])
    #     | 'Add the amounts for each group' >> beam.ParDo(sum_deaths())
    #     | 'Get grouped covid deaths' >> beam.Values()
    #     # | "Prepare columns" >> beam.Map()
    # )

    # first_merge = (
    #     (estados_ibge, dados_covid)
    #     | 'Merge datasets' >> beam.CoGroupByKey()
    # )

# pipeline.run()