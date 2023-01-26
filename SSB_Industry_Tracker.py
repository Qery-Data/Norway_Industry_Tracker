from pyjstat import pyjstat
import requests
import os
import json
from datetime import datetime
import locale
import io
import pandas as pd
import pycountry
os.makedirs('data', exist_ok=True)

#Enterprises NACE1
ssburl = 'https://data.ssb.no/api/v0/no/table/12817/'
query = {
  "query": [
    {
      "code": "NACE2007",
      "selection": {
        "filter": "vs:NACE2007StrHoved",
        "values": [
          "B",
          "C",
          "D",
          "E",
          "F",
          "G",
          "H",
          "I",
          "J",
          "L",
          "M",
          "N",
          "P",
          "Q",
          "R",
          "S"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Foretak"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='næring (SN2007)', columns='år', values='value')
df_new.to_csv('data/NACE1_Enterprises.csv', index=True)
df_new['pct_change_1y'] = df_new['2021'].div(df_new['2020']).sub(1).mul(100)
df_new['pct_change_3y'] = df_new['2021'].div(df_new['2018']).sub(1).mul(100)
df_new = df_new.dropna()
df_new.to_csv('data/NACE1_Enterprises_Pct_Change.csv', index=True)

#Employment NACE1
ssburl = 'https://data.ssb.no/api/v0/no/table/12817/'
query = {
  "query": [
    {
      "code": "NACE2007",
      "selection": {
        "filter": "vs:NACE2007StrHoved",
        "values": [
          "B",
          "C",
          "D",
          "E",
          "F",
          "G",
          "H",
          "I",
          "J",
          "L",
          "M",
          "N",
          "P",
          "Q",
          "R",
          "S"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Sysselsetting"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='næring (SN2007)', columns='år', values='value')
df_new.to_csv('data/NACE1_Employment.csv', index=True)
df_new['pct_change_1y'] = df_new['2021'].div(df_new['2020']).sub(1).mul(100)
df_new['pct_change_3y'] = df_new['2021'].div(df_new['2018']).sub(1).mul(100)
df_new = df_new.dropna()
df_new.to_csv('data/NACE1_Employment_Pct_Change.csv', index=True)

#Turnover NACE1
ssburl = 'https://data.ssb.no/api/v0/no/table/12817/'
query = {
  "query": [
    {
      "code": "NACE2007",
      "selection": {
        "filter": "vs:NACE2007StrHoved",
        "values": [
          "B",
          "C",
          "D",
          "E",
          "F",
          "G",
          "H",
          "I",
          "J",
          "L",
          "M",
          "N",
          "P",
          "Q",
          "R",
          "S"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Oms"
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='næring (SN2007)', columns='år', values='value')
df_new.to_csv('data/NACE1_Turnover.csv', index=True)
df_new['pct_change_1y'] = df_new['2021'].div(df_new['2020']).sub(1).mul(100)
df_new['pct_change_3y'] = df_new['2021'].div(df_new['2018']).sub(1).mul(100)
df_new = df_new.dropna()
df_new.to_csv('data/NACE1_Turnover_Pct_Change.csv', index=True)

#Employees NACE1
ssburl = 'https://data.ssb.no/api/v0/no/table/12910/'
query = {
  "query": [
    {
      "code": "NACE2007",
      "selection": {
        "filter": "vs:NACE2007StrHoved",
        "values": [
          "B",
          "C",
          "D",
          "E",
          "F",
          "G",
          "H",
          "I",
          "J",
          "L",
          "M",
          "N",
          "P",
          "Q",
          "R",
          "S"
        ]
      }
    },
    {
      "code": "Enhet",
      "selection": {
        "filter": "item",
        "values": [
          "1"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Lonnstakere"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          5
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='næring (SN2007)', columns='år', values='value')
df_new.to_csv('data/NACE1_Employees.csv', index=True)
df_new['pct_change_1y'] = df_new['2020'].div(df_new['2019']).sub(1).mul(100)
df_new['pct_change_3y'] = df_new['2020'].div(df_new['2017']).sub(1).mul(100)
df_new = df_new.dropna()
df_new.to_csv('data/NACE1_Employees_Pct_Change.csv', index=True)

#Man years NACE1
ssburl = 'https://data.ssb.no/api/v0/no/table/12910/'
query = {
  "query": [
    {
      "code": "NACE2007",
      "selection": {
        "filter": "vs:NACE2007StrHoved",
        "values": [
          "B",
          "C",
          "D",
          "E",
          "F",
          "G",
          "H",
          "I",
          "J",
          "L",
          "M",
          "N",
          "P",
          "Q",
          "R",
          "S"
        ]
      }
    },
    {
      "code": "Enhet",
      "selection": {
        "filter": "item",
        "values": [
          "1"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Arsverk"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          5
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='næring (SN2007)', columns='år', values='value')
df_new.to_csv('data/NACE1_Man_Years.csv', index=True)
df_new['pct_change_1y'] = df_new['2020'].div(df_new['2019']).sub(1).mul(100)
df_new['pct_change_3y'] = df_new['2020'].div(df_new['2017']).sub(1).mul(100)
df_new = df_new.dropna()
df_new.to_csv('data/NACE1_Man_Years_Pct_Change.csv', index=True)

#Personell costs NACE1
ssburl = 'https://data.ssb.no/api/v0/no/table/12910/'
query = {
  "query": [
    {
      "code": "NACE2007",
      "selection": {
        "filter": "vs:NACE2007StrHoved",
        "values": [
          "B",
          "C",
          "D",
          "E",
          "F",
          "G",
          "H",
          "I",
          "J",
          "L",
          "M",
          "N",
          "P",
          "Q",
          "R",
          "S"
        ]
      }
    },
    {
      "code": "Enhet",
      "selection": {
        "filter": "item",
        "values": [
          "1"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "Lonnskost"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          5
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='næring (SN2007)', columns='år', values='value')
df_new.to_csv('data/NACE1_Personell_Costs.csv', index=True)
df_new['pct_change_1y'] = df_new['2020'].div(df_new['2019']).sub(1).mul(100)
df_new['pct_change_3y'] = df_new['2020'].div(df_new['2017']).sub(1).mul(100)
df_new = df_new.dropna()
df_new.to_csv('data/NACE1_Personell_Costs_Pct_Change.csv', index=True)

#Payment for agency workers NACE1
ssburl = 'https://data.ssb.no/api/v0/no/table/12910/'
query = {
  "query": [
    {
      "code": "NACE2007",
      "selection": {
        "filter": "vs:NACE2007StrHoved",
        "values": [
          "B",
          "C",
          "D",
          "E",
          "F",
          "G",
          "H",
          "I",
          "J",
          "L",
          "M",
          "N",
          "P",
          "Q",
          "R",
          "S"
        ]
      }
    },
    {
      "code": "Enhet",
      "selection": {
        "filter": "item",
        "values": [
          "1"
        ]
      }
    },
    {
      "code": "ContentsCode",
      "selection": {
        "filter": "item",
        "values": [
          "UtgAvlonVikar"
        ]
      }
    },
    {
      "code": "Tid",
      "selection": {
        "filter": "top",
        "values": [
          5
        ]
      }
    }
  ],
  "response": {
    "format": "json-stat2"
  }
}
result = requests.post(ssburl, json = query)
dataset = pyjstat.Dataset.read(result.text)
type(dataset)
df = dataset.write('dataframe')
df_new = df.pivot(index='næring (SN2007)', columns='år', values='value')
df_new.to_csv('data/NACE1_Agency_Worker_Costs.csv', index=True)
df_new['pct_change_1y'] = df_new['2020'].div(df_new['2019']).sub(1).mul(100)
df_new['pct_change_3y'] = df_new['2020'].div(df_new['2017']).sub(1).mul(100)
df_new = df_new.dropna()
df_new.to_csv('data/NACE1_Agency_Worker_Costs_Pct_Change.csv', index=True)