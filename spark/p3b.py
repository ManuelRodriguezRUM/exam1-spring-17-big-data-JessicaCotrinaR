#Code here
# Para correr el ejercicio
#pyspark --master yarn --deploy-mode client --conf="sparkexecutorEnv.PYTHONHASHSEED=223"

import csv

escuela_file = sc.textFile("/home/jessica/escuelasPR.csv")
escuela_datos=escuela_file.map(lambda x: [x]).map(lambda x : list(csv.reader(x))[0])
ponce = escuela_datos.filter(lambda x: x[2] == 'Ponce')
cabo_rojo = escuela_datos.filter(lambda x: x[2] == 'Cabo Rojo')
dorado = escuela_datos.filter(lambda x: x[2] == 'Dorado')
resultado = ponce.union(Cabo_Rojo).union(Dorado)
resultado.foreach(print)

# Resultados
"""
['Ponce', 'Ponce', 'Ponce', '52274', 'SUPERIOR JUAN SERRALL�S', 'Superior', '1587']
['Ponce', 'Ponce', 'Ponce', '52514', 'PONCE HIGH SCHOOL', 'Superior', '1570']
['Ponce', 'Ponce', 'Ponce', '52688', 'DR. PILA', 'Superior', '1575']
['Ponce', 'Ponce', 'Ponce', '52696', 'BERNARDINO CORDERO BERNARD', 'Superior', '1576']
['Ponce', 'Ponce', 'Ponce', '52712', 'THOMAS ARMSTRONG TORO', 'Superior', '1585']
['Ponce', 'Ponce', 'Ponce', '56069', 'BETHZAIDA VELAZQUEZ ANDUJAR', 'Superior', '1583']
['Ponce', 'Ponce', 'Ponce', '56432', 'SUP. JARDINES DE PONCE', 'Superior', '1586']
['Ponce', 'Ponce', 'Ponce', '58511', 'LILA MAR�A MERCEDES MAYORAL', 'Superior', '1580']
['Mayag�ez', 'Cabo Rojo', 'Cabo Rojo', '46821', 'IN�S MAR�A MENDOZA DE MU�OZ MARIN', 'Superior', '1120']
['Mayag�ez', 'Cabo Rojo', 'Cabo Rojo', '46987', 'MONSERRATE LE�N DE IRIZARRY', 'Superior', '1125']
['Arecibo', 'Vega Alta', 'Dorado', '71092', 'JOS� SANTOS ALEGR�A', 'Superior', '1240']
"""
