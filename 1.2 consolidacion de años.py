import pandas as pd
regiones = pd.DataFrame({
    "region_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
    "Región": [
        "Tarapacá",
        "Antofagasta",
        "Atacama",
        "Coquimbo",
        "Valparaíso",
        "Libertador General Bernardo O'Higgins",
        "Maule",
        "Biobío",
        "La Araucanía",
        "Los Lagos",
        "Aysén del General Carlos Ibáñez del Campo",
        "Magallanes y de la Antártica Chilena",
        "Metropolitana de Santiago",
        "Los Ríos",
        "Arica y Parinacota",
        "Ñuble"
    ]
})

#############################################################################
############################## CARGA DE DATOS ###############################
#############################################################################

consolidated_total = pd.read_excel('./Consolidado estadísticas hospitalarias 2014-2023.xlsx', sheet_name='2023',skiprows=1)
consolidated_2023 = pd.read_csv('./2023/2023_consolidated_data.csv', encoding='utf-8', delimiter=';', low_memory=False)
financial_2023 = pd.read_csv('./2023/2023_financial_data.csv', encoding='utf-8', delimiter=',')
hospitals_2023 = pd.read_csv('./2023/2023_hospitals.csv', encoding='utf-8', delimiter=',')
predictions_2023 = pd.read_csv('./2023/2023_prediciones_grd.txt', encoding='utf-8', delimiter=',')
consultas_2023 = pd.read_csv('./2023/variables/2023_consultas.txt', delimiter=',')
quirofanos_2023 = pd.read_csv('./2023/variables/2023_quirofanos.txt', delimiter=',')
consultas= consultas_2023.columns.values
quirofanos = quirofanos_2023.columns.values

#copiar columnas hospital_id, region_id, hospital_name, hospital_alternative_name, latitud y longitud de hospitals_2023 en df nuevo
df_2023 = hospitals_2023[['hospital_id', 'region_id', 'hospital_name', 'hospital_alternative_name', 'latitud', 'longitud']].copy()


## OBTENER CONSULTAS Y QUIROFANOS

consolidated_2023_consultas = consolidated_2023[consultas]
consolidated_2023_quirofanos = consolidated_2023[quirofanos]

# pasar columnas a numerico
consolidated_2023_consultas.iloc[:, 1:] = consolidated_2023_consultas.iloc[:, 1:].apply(pd.to_numeric, errors='coerce')
consolidated_2023_quirofanos.iloc[:, 1:] = consolidated_2023_quirofanos.iloc[:, 1:].apply(pd.to_numeric, errors='coerce')

# sumar valores de todas las columnas de consolidated_2023_consultas y asignar a una nueva columna 'consultas'
consolidated_2023_consultas['Consultas'] = consolidated_2023_consultas.iloc[:, 1:].sum(axis=1)
# consolidated_2023_consultas.loc[:, 'Consultas'] = consolidated_2023_consultas.sum(axis=1)
# sumar valores de todas las columnas de consolidated_2023_quirofanos y asignar a una nueva columna 'quirofanos'
consolidated_2023_quirofanos['Quirofanos'] = consolidated_2023_quirofanos.iloc[:, 1:].sum(axis=1)
# consolidated_2023_quirofanos.loc[:, 'Quirofanos'] = consolidated_2023_quirofanos.sum(axis=1)

# pegar las columnas 'Consultas' y 'Quirofanos' a consolidated_2023 segun el hospital_id
# consolidated_2023 = pd.merge(consolidated_2023, consolidated_2023_consultas[['idEstablecimiento', 'Consultas']], on='idEstablecimiento', how='left')
# consolidated_2023 = pd.merge(consolidated_2023, consolidated_2023_quirofanos[['idEstablecimiento', 'Quirofanos']], on='idEstablecimiento', how='left')
df_2023 = pd.merge(df_2023, consolidated_2023_consultas[['idEstablecimiento', 'Consultas']], left_on='hospital_id', right_on='idEstablecimiento', how='left')
df_2023.drop(columns='idEstablecimiento', inplace=True)
df_2023 = pd.merge(df_2023, consolidated_2023_quirofanos[['idEstablecimiento', 'Quirofanos']], left_on='hospital_id',right_on='idEstablecimiento', how='left')
df_2023.drop(columns='idEstablecimiento', inplace=True)


## OBTENER EGRESOSGRD

# definir columna grd de predictions_2023 como el valor de la columna OriginalGRD y si es igual a -, usar el valor de la columna Prediction
predictions_2023['grd'] = predictions_2023.apply(lambda x: x['OriginalGRD'] if x['OriginalGRD'] != '-' else x['Prediction'], axis=1)

# seleccionar valor de consolidated_total con glosa igual Numero de Egresos y Dias Cama Disponibles
# egresos = consolidated_total[consolidated_total['Glosa'].isin(['Numero de Egresos'])] # , 'Días Cama Disponibles'

egresos = consolidated_total[(consolidated_total['Glosa'] == 'Numero de Egresos') & 
                             (consolidated_total['Nombre Nivel Cuidado'] == 'Datos Establecimiento')]

# pegar columna Acum de egresos en predictions_2023
predictions_2023 = pd.merge(predictions_2023, egresos[['Cód. Estab.', 'Acum']], left_on='IdEstablecimiento', right_on='Cód. Estab.', how='left')
# renombrar columna Acum a Egresos
predictions_2023.rename(columns={'Acum': 'Egresos'}, inplace=True)

# pasar grd a float 
predictions_2023['grd'] = predictions_2023['grd'].astype(float)
# pasar Egresos a float
predictions_2023['Egresos'] = predictions_2023['Egresos'].astype(float)

# multiplicar grd por Egresos y asignar a columna Prediccion
predictions_2023['GRDxEgreso'] = predictions_2023['grd'] * predictions_2023['Egresos']

# pegar columna GRDxEgreso a consolidated_2023 por idEstablecimiento
# consolidated_2023 = pd.merge(consolidated_2023, predictions_2023[['IdEstablecimiento', 'GRDxEgreso']], left_on='idEstablecimiento', right_on='IdEstablecimiento', how='left')
df_2023 = pd.merge(df_2023, predictions_2023[['IdEstablecimiento', 'GRDxEgreso']], left_on='hospital_id', right_on='IdEstablecimiento', how='left')
df_2023.drop(columns='IdEstablecimiento', inplace=True)


## OBTENER X21 Y X22 VALUE

# pegar valores 21_value y 22_value de financial_2023 a consolidated_2023 por idEstablecimiento
financial_2023.rename(columns={'21_value': 'Bienes y servicios', '22_value': 'Remuneraciones'}, inplace=True)
# consolidated_2023 = pd.merge(consolidated_2023, financial_2023[['hospital_id','Bienes y servicios', 'Remuneraciones']], left_on='idEstablecimiento', right_on='hospital_id', how='left')
df_2023 = pd.merge(df_2023, financial_2023[['hospital_id','Bienes y servicios', 'Remuneraciones']], on='hospital_id', how='left')


## OBTENER DIAS CAMAS DISPONIBLES
# seleccionar valor de consolidated_total con glosa igual Dias Cama Disponibles
dias_cama_disponible = consolidated_total[(consolidated_total['Glosa'] == 'Dias Cama Disponibles') & 
                             (consolidated_total['Nombre Nivel Cuidado'] == 'Datos Establecimiento')]

# renombrar columna Acum a Dias Cama Disponibles
dias_cama_disponible.rename(columns={'Acum': 'Dias Cama Disponibles'}, inplace=True)

# pegar columna Dias Cama Disponibles de dias_cama_disponible en predictions_2023
# predictions_2023 = pd.merge(predictions_2023, dias_cama_disponible[['Cód. Estab.', 'Dias Cama Disponibles']], left_on='IdEstablecimiento', right_on='Cód. Estab.', how='left')
df_2023 = pd.merge(df_2023, dias_cama_disponible[['Cód. Estab.', 'Dias Cama Disponibles']], left_on='hospital_id', right_on='Cód. Estab.', how='left')
df_2023.drop(columns='Cód. Estab.', inplace=True)

## OBTENER EXAMENES Y CONSULTAS DE EMERGENCIA

import pandas as pd
consolidated_2023 = pd.read_csv('./2023/2023_consolidated_data.csv', encoding='utf-8', delimiter=';', low_memory=False)

consultas_urgencias_2023 = pd.read_csv('./2023/variables/2023_consultas_urgencias.txt', header=None).transpose()
consultas_urgencias_2023.columns = consultas_urgencias_2023.iloc[0]
consultas_urgencias = consultas_urgencias_2023.columns

examenes_2023 = pd.read_csv('./2023/variables/2023_examenes.txt', header=None).transpose()
examenes_2023.columns = examenes_2023.iloc[0]
examenes = examenes_2023.columns

consolidated_2023_consultas_urgencias = consolidated_2023[consultas_urgencias]
consolidated_2023_examenes = consolidated_2023[examenes]

consolidated_2023_consultas_urgencias.iloc[:, 1:] = consolidated_2023_consultas_urgencias.iloc[:, 1:].apply(pd.to_numeric, errors='coerce')
consolidated_2023_examenes.iloc[:, 1:] = consolidated_2023_examenes.iloc[:, 1:].apply(pd.to_numeric, errors='coerce')

# sumar valores de todas las columnas de consolidated_2023_consultas y asignar a una nueva columna 'consultas'
consolidated_2023_consultas_urgencias['Consultas Urgencias'] = consolidated_2023_consultas_urgencias.iloc[:, 1:].sum(axis=1)
# consolidated_2023_consultas.loc[:, 'Consultas'] = consolidated_2023_consultas.sum(axis=1)
# sumar valores de todas las columnas de consolidated_2023_quirofanos y asignar a una nueva columna 'quirofanos'
consolidated_2023_examenes['Examenes'] = consolidated_2023_examenes.iloc[:, 1:].sum(axis=1)
# consolidated_2023_quirofanos.loc[:, 'Quirofanos'] = consolidated_2023_quirofanos.sum(axis=1)

# pegar las columnas 'Consultas' y 'Quirofanos' a consolidated_2023 segun el hospital_id
# consolidated_2023 = pd.merge(consolidated_2023, consolidated_2023_consultas[['idEstablecimiento', 'Consultas']], on='idEstablecimiento', how='left')
# consolidated_2023 = pd.merge(consolidated_2023, consolidated_2023_quirofanos[['idEstablecimiento', 'Quirofanos']], on='idEstablecimiento', how='left')
df_2023 = pd.merge(df_2023, consolidated_2023_consultas_urgencias[['idEstablecimiento', 'Consultas Urgencias']], left_on='hospital_id', right_on='idEstablecimiento', how='left')
df_2023.drop(columns='idEstablecimiento', inplace=True)
df_2023 = pd.merge(df_2023, consolidated_2023_examenes[['idEstablecimiento', 'Examenes']], left_on='hospital_id',right_on='idEstablecimiento', how='left')
df_2023.drop(columns='idEstablecimiento', inplace=True)

# exportar df_2023 a csv
df_2023.to_csv('./df_2023.csv', index=False, encoding='utf-8')