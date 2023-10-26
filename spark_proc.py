from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *

def melt(
        df,
        id_vars,
        value_vars,
        var_name="variable",
        value_name="value"):
    """Convert DataFrame from wide to long format."""

    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name))
        for c in value_vars))

    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [
        col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)

# Inicializa una sesion de Spark
spark = SparkSession.builder.getOrCreate()

# Leer data
df = spark.read.option('header', 'true').option('delimiter', '|').csv('/user/maria_dev/proyecto_de/Data.txt')

# Seleccionar las columnas deseadas en el DataFrame df
columnas_deseadas = [
    "FECHAREGISTRO", "GRUPO", "REGION", "PROVINCIA", "DISTRITO",
    "ZC_UCI_ADUL_CAM_INOPERATIVOS", "ZC_UCI_ADUL_CAM_TOT_OPER", "ZC_UCI_ADUL_CAM_TOT_DISP",
    "ZC_UCI_ADUL_CAM_TOT_OCUP", "ZC_UCI_NEONATAL_CAM_INOPERATIVOS", "ZC_UCI_NEONATAL_CAM_TOT_OPER",
    "ZC_UCI_NEONATAL_CAM_TOT_DISP", "ZC_UCI_NEONATAL_CAM_TOT_OCUP",
    "ZC_UCI_PEDIA_CAM_INOPERATIVOS", "ZC_UCI_PEDIA_CAM_TOT_OPER", "ZC_UCI_PEDIA_CAM_TOT_DISP",
    "ZC_UCI_PEDIA_CAM_TOT_OCUP", "ZNC_UCI_ADUL_CAM_INOPERATIVO", "ZNC_UCI_ADUL_CAM_OPERATIVO",
    "ZNC_UCI_ADUL_CAM_DISPONIBLE", "ZNC_UCI_ADUL_CAM_OCUPADO", "ZNC_UCI_NEONATAL_CAM_INOPERATIVO",
    "ZNC_UCI_NEONATAL_CAM_OPERATIVO", "ZNC_UCI_NEONATAL_CAM_DISPONIBLE", "ZNC_UCI_NEONATAL_CAM_OCUPADO",
    "ZNC_UCI_PEDIA_CAM_INOPERATIVO", "ZNC_UCI_PEDIA_CAM_OPERATIVO", "ZNC_UCI_PEDIA_CAM_DISPONIBLE", "ZNC_UCI_PEDIA_CAM_OCUPADO"
]

df = df.select(columnas_deseadas)

# Eliminar filas nulas solo de las columnas mas significativas
df = df.na.drop(subset=['FECHAREGISTRO', 'GRUPO', 'REGION', 'PROVINCIA', 'DISTRITO'])

# Llenar valores nulos con ceros en todo el DataFrame
df = df.fillna('0')

# Cambiar el tipo de datos de la columna FECHAREGISTRO con el formato adecuado
date_format = "yyyy/MM/dd h:mm:ss a"
df = df.withColumn("FECHAREGISTRO", to_timestamp(df["FECHAREGISTRO"], date_format))

# Agregar la columna Anio
df = df.withColumn("ANIO", year("FECHAREGISTRO"))

# Diccionario que mapea los nombres de los meses en espanol
meses_en_espanol = {
    1: 'Enero',
    2: 'Febrero',
    3: 'Marzo',
    4: 'Abril',
    5: 'Mayo',
    6: 'Junio',
    7: 'Julio',
    8: 'Agosto',
    9: 'Septiembre',
    10: 'Octubre',
    11: 'Noviembre',
    12: 'Diciembre'
}

# Agregar la columna Mes
df = df.withColumn("MES", lit(None))  # Crear la columna MES inicialmente como nula
for mes_num, mes_nombre in meses_en_espanol.items():
    df = df.withColumn("MES", when(month("FECHAREGISTRO") == mes_num, mes_nombre).otherwise(df["MES"]))

# Agregar la columna Dia
df = df.withColumn("DIA", dayofmonth("FECHAREGISTRO"))

# Eliminar la columna FECHAREGISTRO
df = df.drop("FECHAREGISTRO")

# Hacer el procedimiento melt en las columnas respectivas
df = melt(df, id_vars=['GRUPO', 'REGION', 'PROVINCIA', 'DISTRITO', 'ANIO', 'MES', 'DIA'], value_vars=df.columns[4:-3], var_name="ATRIBUTO", value_name="CAMAS")

# Crear una vista temporal para hacer consultas sql
df.createOrReplaceTempView("temp_table")

# Hacer consulta sql que divide la columna 'ATRIBUTO' en las columnas 'USO', 'GRUPO ETARIO' Y 'ESTADO'
df = spark.sql('''
SELECT
    ANIO,
    MES,
    DIA,
    GRUPO AS AMBITO,
    REGION,
    PROVINCIA,
    DISTRITO,
    SUBSTRING_INDEX(ATRIBUTO, '_', 1) AS USO,
    SUBSTRING_INDEX(SUBSTRING_INDEX(ATRIBUTO, '_', 3), '_', -1) AS `GRUPO ETARIO`,
    SUBSTRING_INDEX(ATRIBUTO, '_', -1) AS ESTADO,
    CAMAS
FROM temp_table
''')

# Reemplaza los valores en la columna 'USO'
df = df.withColumn('USO',
    when(col('USO') == 'ZC', 'COVID')
    .when(col('USO') == 'ZNC', 'NO COVID')
    .otherwise(col('USO')))

# Reemplaza los valores en la columna 'GRUPO ETARIO'
df = df.withColumn('GRUPO ETARIO',
    when(col('GRUPO ETARIO') == 'ADUL', 'ADULTOS')
    .when(col('GRUPO ETARIO') == 'NEONATAL', 'NEONATAL')
    .when(col('GRUPO ETARIO') == 'PEDIA', 'PEDIATRICA')
    .otherwise(col('GRUPO ETARIO'))
)

# Reemplaza los valores en la columna 'ESTADO'
df = df.withColumn('ESTADO',
    when(col('ESTADO') == 'OCUP', 'OCUPADO')
    .when(col('ESTADO') == 'DISP', 'DISPONIBLE')
    .when(col('ESTADO') == 'INOPERATIVOS', 'INOPERATIVO')
    .when(col('ESTADO') == 'OPER', 'OPERATIVO')
    .otherwise(col('ESTADO'))
)

# Configura la propiedad para el analizador de fecha y hora
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Guarda el DataFrame en CSV sin encabezado
df.write.csv("/user/maria_dev/proyecto_de/DataProcesada", header=False)
