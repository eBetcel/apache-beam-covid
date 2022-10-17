# Data Engineering Challenge - Apache Beam

The objective of this challenge is to develop a data pipeline using [Apache Beam](https://beam.apache.org/). What should be delivered is a python file capable of delivering the requested results.

The necessary files are available at this repository, in which there are two public archives that
were extracted from the IBGE and Corona Virus Brazil website:

1. EstadosIBGE.csv – General state information
2. hist_painel_covid.csv - Dados históricos do Covid-19 no Brasil

## Files expected:
### csv and json file
|Regiao| Estado| UF| Governador| TotalCasos | TotalObitos |
|---|---|---|---|---|---|
|Sudeste|Rio de Janeiro|RJ|Wilson José Witzel|99999|99999|

```javascript
[
  { 
    "Regiao": "Sudeste",
    "Estado": "Rio de Janeiro",
    "UF": "RJ",
    "Governador": "Wilson José Witzel",
    "TotalCasos": "99999",
    "TotalObitos": "99999",
  }
]
```

# Running script
## Installing dependencies 
`pip install -r requirements.txt` 

## Then the script can be run with the command:
`python main.py`

### The output are the files 
`covid_brasil-00000-of-00001.csv` and `covid_brasil-00000-of-00001.json`

# Code explanation
## What to be done
After an exploratory data analysis, it was noticed that the key that connects the two datasets is the state code (integer number). And two things need to be done: Get the data on the governor's name and the state's name from the IBGE base and aggregate the covid base entries, adding the new cases and new deaths to get the total. Some entries were not associated with a city/state and it was decided that they would be added to the 'Brasil' region

## Why use Apache Beam
Beam is particularly useful for parallel data processing tasks, in which the problem can be decomposed into many smaller bundles of data that can be processed independently and in parallel. This task is basicaly an ETL and beam can be used for optimizing the process, in addition to having clear advantages as the size of the databases increases.

The first step of the pipeline was to read the covid data file and generate a Pcollections in the format `(('Norte', 'TO', '17'), ('2', '0'))`, being the (Regiao, UF, coduf) as key and (new cases, new deaths) as values. After that, beam.CombinePerKey was used with a lambda function to sum all the cases and deaths, obtaining the following result:

```
...
(('Sudeste', 'SP', '35'), (1946284, 70250))
(('Sul', 'PR', '41'), (351964, 8756))
(('Sul', 'SC', '42'), (426132, 5530))
(('Sul', 'RS', '43'), (371556, 9296))
(('Centro-Oeste', 'MS', '50'), (136650, 2556))
(('Centro-Oeste', 'MT', '51'), (242290, 6744))
(('Centro-Oeste', 'GO', '52'), (405508, 9058))
...
``` 

After that, a Ptransform was made to have a dictionary like this
```
{'coduf': '51', 'Regiao': 'Centro-Oeste', 'UF': 'MT', 'TotalCasos': 242290, 'TotalObitos': 6744}
{'coduf': '52', 'Regiao': 'Centro-Oeste', 'UF': 'GO', 'TotalCasos': 405508, 'TotalObitos': 9058}
{'coduf': '53', 'Regiao': 'Centro-Oeste', 'UF': 'DF', 'TotalCasos': 381010, 'TotalObitos': 6406}
```

## IBGE data
To get the date from the IBGE file a PTransform that generated a Pcollection that had the state code as a key and a tuple with the state and the governor as value was used

```
('42', {'Estado': 'Santa Catarina', 'Governador': 'CARLOS MOISÉS DA SILVA'})
('35', {'Estado': 'São Paulo', 'Governador': 'JOÃO AGRIPINO DA COSTA DORIA JUNIOR'})
('28', {'Estado': 'Sergipe', 'Governador': 'BELIVALDO CHAGAS SILVA'})
```
## Combining the data
After using beam.pvalue.AsDict to wrap the data, basically a junction was made joining data from one dictionary to another

## Last step 
Generating the final file basically consisted of formatting the data and writing to the files


> Useful resources for this challange:
* https://stackoverflow.com/questions/45287530/join-two-json-in-google-cloud-platform-with-dataflow/45290372#45290372
* https://beam.apache.org/documentation/sdks/python/

