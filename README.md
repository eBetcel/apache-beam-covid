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
### After an exploratory data analysis, it was noticed that the key that connects the two datasets is the state code (integer number). And two things need to be done: Get the data on the governor's name and the state's name from the IBGE base and aggregate the covid base entries, adding the new cases and new deaths to get the total. Some entries were not associated with a city/state and it was decided that they would be added to the 'Brasil' region

### 
