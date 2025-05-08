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
consolidated_2014 = pd.read_csv('./2014/2014_consolidated_data.csv', encoding='utf-8', delimiter=';', low_memory=False)
financial_2014 = pd.read_csv('./2014/2014_financial_data.csv', encoding='utf-8', delimiter=',')
hospitals_2014 = pd.read_csv('./2014/2014_hospitals.csv', encoding='utf-8', delimiter=',')
predictions_2014 = pd.read_csv('./2014/2014_prediciones_grd.txt', encoding='utf-8', delimiter=',')
consultas_2014 = pd.read_csv('./2014/variables/2014_consultas.txt', delimiter=',')
quirofanos_2014 = pd.read_csv('./2014/variables/2014_quirofanos.txt', delimiter=',')
consultas= consultas_2014.columns.values
quirofanos = quirofanos_2014.columns.values

#copiar columnas hospital_id, region_id, hospital_name, hospital_alternative_name, latitud y longitud de hospitals_2014 en df nuevo
df_2014 = hospitals_2014[['hospital_id', 'region_id', 'hospital_name', 'hospital_alternative_name', 'latitud', 'longitud']].copy()


## OBTENER CONSULTAS Y QUIROFANOS

consolidated_2014_consultas = consolidated_2014[consultas]
consolidated_2014_quirofanos = consolidated_2014[quirofanos]

# sumar valores de todas las columnas de consolidated_2014_consultas y asignar a una nueva columna 'consultas'
consolidated_2014_consultas['Consultas'] = consolidated_2014_consultas.iloc[:, 1:].sum(axis=1)
# consolidated_2014_consultas.loc[:, 'Consultas'] = consolidated_2014_consultas.sum(axis=1)
# sumar valores de todas las columnas de consolidated_2014_quirofanos y asignar a una nueva columna 'quirofanos'
consolidated_2014_quirofanos['Quirofanos'] = consolidated_2014_quirofanos.iloc[:, 1:].sum(axis=1)
# consolidated_2014_quirofanos.loc[:, 'Quirofanos'] = consolidated_2014_quirofanos.sum(axis=1)

# pegar las columnas 'Consultas' y 'Quirofanos' a consolidated_2014 segun el hospital_id
# consolidated_2014 = pd.merge(consolidated_2014, consolidated_2014_consultas[['idEstablecimiento', 'Consultas']], on='idEstablecimiento', how='left')
# consolidated_2014 = pd.merge(consolidated_2014, consolidated_2014_quirofanos[['idEstablecimiento', 'Quirofanos']], on='idEstablecimiento', how='left')
df_2014 = pd.merge(df_2014, consolidated_2014_consultas[['idEstablecimiento', 'Consultas']], left_on='hospital_id', right_on='idEstablecimiento', how='left')
df_2014.drop(columns='idEstablecimiento', inplace=True)
df_2014 = pd.merge(df_2014, consolidated_2014_quirofanos[['idEstablecimiento', 'Quirofanos']], left_on='hospital_id',right_on='idEstablecimiento', how='left')
df_2014.drop(columns='idEstablecimiento', inplace=True)


## OBTENER EGRESOSGRD

# definir columna grd de predictions_2014 como el valor de la columna OriginalGRD y si es igual a -, usar el valor de la columna Prediction
predictions_2014['grd'] = predictions_2014.apply(lambda x: x['OriginalGRD'] if x['OriginalGRD'] != '-' else x['Prediction'], axis=1)

# seleccionar valor de consolidated_total con glosa igual Numero de Egresos y Dias Cama Disponibles
# egresos = consolidated_total[consolidated_total['Glosa'].isin(['Numero de Egresos'])] # , 'Días Cama Disponibles'

egresos = consolidated_total[(consolidated_total['Glosa'] == 'Numero de Egresos') & 
                             (consolidated_total['Nombre Nivel Cuidado'] == 'Datos Establecimiento')]

# pegar columna Acum de egresos en predictions_2014
predictions_2014 = pd.merge(predictions_2014, egresos[['Cód. Estab.', 'Acum']], left_on='IdEstablecimiento', right_on='Cód. Estab.', how='left')
# renombrar columna Acum a Egresos
predictions_2014.rename(columns={'Acum': 'Egresos'}, inplace=True)

# pasar grd a float 
predictions_2014['grd'] = predictions_2014['grd'].astype(float)
# pasar Egresos a float
predictions_2014['Egresos'] = predictions_2014['Egresos'].astype(float)

# multiplicar grd por Egresos y asignar a columna Prediccion
predictions_2014['GRDxEgreso'] = predictions_2014['grd'] * predictions_2014['Egresos']

# pegar columna GRDxEgreso a consolidated_2014 por idEstablecimiento
# consolidated_2014 = pd.merge(consolidated_2014, predictions_2014[['IdEstablecimiento', 'GRDxEgreso']], left_on='idEstablecimiento', right_on='IdEstablecimiento', how='left')
df_2014 = pd.merge(df_2014, predictions_2014[['IdEstablecimiento', 'GRDxEgreso']], left_on='hospital_id', right_on='IdEstablecimiento', how='left')
df_2014.drop(columns='IdEstablecimiento', inplace=True)


## OBTENER X21 Y X22 VALUE

# pegar valores 21_value y 22_value de financial_2014 a consolidated_2014 por idEstablecimiento
financial_2014.rename(columns={'21_value': 'Bienes y servicios', '22_value': 'Remuneraciones'}, inplace=True)
# consolidated_2014 = pd.merge(consolidated_2014, financial_2014[['hospital_id','Bienes y servicios', 'Remuneraciones']], left_on='idEstablecimiento', right_on='hospital_id', how='left')
df_2014 = pd.merge(df_2014, financial_2014[['hospital_id','Bienes y servicios', 'Remuneraciones']], on='hospital_id', how='left')


## OBTENER DIAS CAMAS DISPONIBLES
# seleccionar valor de consolidated_total con glosa igual Dias Cama Disponibles
dias_cama_disponible = consolidated_total[(consolidated_total['Glosa'] == 'Dias Cama Disponibles') & 
                             (consolidated_total['Nombre Nivel Cuidado'] == 'Datos Establecimiento')]

# renombrar columna Acum a Dias Cama Disponibles
dias_cama_disponible.rename(columns={'Acum': 'Dias Cama Disponibles'}, inplace=True)

# pegar columna Dias Cama Disponibles de dias_cama_disponible en predictions_2014
# predictions_2014 = pd.merge(predictions_2014, dias_cama_disponible[['Cód. Estab.', 'Dias Cama Disponibles']], left_on='IdEstablecimiento', right_on='Cód. Estab.', how='left')
df_2014 = pd.merge(df_2014, dias_cama_disponible[['Cód. Estab.', 'Dias Cama Disponibles']], left_on='hospital_id', right_on='Cód. Estab.', how='left')
df_2014.drop(columns='Cód. Estab.', inplace=True)

## OBTENER EXAMENES Y CONSULTAS DE EMERGENCIA

import pandas as pd
consolidated_2014 = pd.read_csv('./2014/2014_consolidated_data.csv', encoding='utf-8', delimiter=';', low_memory=False)

consultas_urgencias_2014 = pd.read_csv('./2014/variables/2014_consultas_urgencias.txt', header=None).transpose()
consultas_urgencias_2014.columns = consultas_urgencias_2014.iloc[0]
consultas_urgencias = consultas_urgencias_2014.columns

examenes_2014 = pd.read_csv('./2014/variables/2014_examenes.txt', header=None).transpose()
examenes_2014.columns = examenes_2014.iloc[0]
examenes = examenes_2014.columns

consolidated_2014_consultas_urgencias = consolidated_2014[consultas_urgencias]
consolidated_2014_examenes = consolidated_2014[examenes]

consolidated_2014_consultas_urgencias.iloc[:, 1:] = consolidated_2014_consultas_urgencias.iloc[:, 1:].apply(pd.to_numeric, errors='coerce')
consolidated_2014_examenes.iloc[:, 1:] = consolidated_2014_examenes.iloc[:, 1:].apply(pd.to_numeric, errors='coerce')

# sumar valores de todas las columnas de consolidated_2014_consultas y asignar a una nueva columna 'consultas'
consolidated_2014_consultas_urgencias['Consultas Urgencias'] = consolidated_2014_consultas_urgencias.iloc[:, 1:].sum(axis=1)
# consolidated_2014_consultas.loc[:, 'Consultas'] = consolidated_2014_consultas.sum(axis=1)
# sumar valores de todas las columnas de consolidated_2014_quirofanos y asignar a una nueva columna 'quirofanos'
consolidated_2014_examenes['Examenes'] = consolidated_2014_examenes.iloc[:, 1:].sum(axis=1)
# consolidated_2014_quirofanos.loc[:, 'Quirofanos'] = consolidated_2014_quirofanos.sum(axis=1)

# pegar las columnas 'Consultas' y 'Quirofanos' a consolidated_2014 segun el hospital_id
# consolidated_2014 = pd.merge(consolidated_2014, consolidated_2014_consultas[['idEstablecimiento', 'Consultas']], on='idEstablecimiento', how='left')
# consolidated_2014 = pd.merge(consolidated_2014, consolidated_2014_quirofanos[['idEstablecimiento', 'Quirofanos']], on='idEstablecimiento', how='left')
df_2014 = pd.merge(df_2014, consolidated_2014_consultas_urgencias[['idEstablecimiento', 'Consultas Urgencias']], left_on='hospital_id', right_on='idEstablecimiento', how='left')
df_2014.drop(columns='idEstablecimiento', inplace=True)
df_2014 = pd.merge(df_2014, consolidated_2014_examenes[['idEstablecimiento', 'Examenes']], left_on='hospital_id',right_on='idEstablecimiento', how='left')
df_2014.drop(columns='idEstablecimiento', inplace=True)

# exportar df_2014 a csv
df_2014.to_csv('./df_2014.csv', index=False, encoding='utf-8')