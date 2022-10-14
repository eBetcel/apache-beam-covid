from itertools import groupby
import apache_beam as beam
import json

def select_columns_ibge(element):
    element.pop(2)
    element = element[0:3]
    return element[1], element

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
    

    dados_covid = (
        pipeline
        | "Read csv hist painel covid" >> beam.io.ReadFromText("hist_painel_covid.csv", skip_header_lines= True)
        | "Split painel covid" >> beam.Map(lambda x: x.split(';'))
        | "Select columns painel covid" >> beam.Map(lambda x: ((x[0], x[1], x[3]), (x[11], x[13])))
        | 'Group by UF' >> beam.CombinePerKey(lambda l: (sum([int(a) for a, _ in l]), sum([int(b) for _, b in l])) )
        # | 'Get grouped covid deaths' >> beam.Values()
        # | 'Add the amounts for each group' >> beam.ParDo(sum_deaths())
        | "Formmat output" >> beam.Map(lambda x: 
        {
        'coduf': x[0][2],
        'Regiao': x[0][0], 
        'UF': x[0][1], 
        'TotalCasos': x[1][0], 
        'TotalObitos' :x[1][1]
        }
        )
        # | 'Group by UF code' >> beam.GroupBy(lambda s: s[2])
    )


    estados_ibge = (
        pipeline
        | "Read csv estados ibge" >> beam.io.ReadFromText("EstadosIBGE.csv", skip_header_lines=True)
        | "Split" >> beam.Map(lambda x: x.split(';'))
        | "Select columns" >> beam.Map(lambda x: (x[1],
            {
                'Estado': x[0],
                'Governador': x[3]
            }
        )
        )
        )

    estados_dict = beam.pvalue.AsDict(estados_ibge)

    def join_dicts(a, b):
        a.update(b.get(a['coduf']) or {})
        return a
    joined_dicts = ( dados_covid 
    | beam.Map(join_dicts, b=estados_dict)
    # first_merge = ((estados_ibge, dados_covid)
        # | 'Merge datasets' >> beam.CoGroupByKey()
        | beam.FlatMap(print)
    )
        # | beam.Map(lambda x: {
        #     'Estado': x[1][0][0][0],
        #     'Governador': x[1][0][0][2],
        #     'UF': x[1][0][0]

        # })
    

# pipeline.run()