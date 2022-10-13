import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_estados_ibge = [
                        'Estado',
                        'coduf',
                        'Governador'
]

estados_ibge = (
    pipeline
    | "Dataset reading" >> beam.io.ReadFromText('EstadosIBGE.csv', skip_header_lines=True)

)

def select_columns_ibge(element):
    element.pop(2)
    element = element[0:3]
    return element

def list_to_dictionary_ibge(elemento, columns):
    return dict(zip(columns, elemento))

# def covid_data_pipeline()
ip = ( pipeline
    | "Read csv" >> beam.io.ReadFromText("EstadosIBGE.csv", skip_header_lines=True)
    | "Split" >> beam.Map(lambda x: x.split(';'))
    # | "Select columns" >> beam.Map()
    | "Select columns" >> beam.Map(select_columns_ibge)
    | "Convert list to dict" >> beam.Map(list_to_dictionary_ibge, colunas_estados_ibge)
    # | beam.io.WriteToText("output.csv")
    | beam.Map(print)
)

pipeline.run()