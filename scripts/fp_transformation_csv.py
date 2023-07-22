from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def read_transform_load():
    '''
    Fungsi ini digunakan dengan tujuan untuk membentuk dimension table. Melalui fungsi ini, pengguna dapat
    memanggil tabel Hive, melakukan beberapa transformasi, hingga nantinya me-load tabel-tabel dimensi
    kembali ke Data Warehouse di Hive.

    Return:
        None
    '''

    #buat SparkSession
    spark = SparkSession.builder\
        .appName('Flights Project')\
        .config('spark.sql.catalogImplementation', 'hive')\
        .config('hive.metastore.uris', 'thrift://localhost:9083')\
        .config('spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation','true')\
        .enableHiveSupport()\
        .getOrCreate()

    #tentukan database Hive
    spark.sql('use flights_staging')

    #inisiasi setiap tabel Hive
    df1 = spark.table('aircraft_all')
    df2 = spark.sql('''
        SELECT DISTINCT
            AircraftDescription, EngineCount, EngineType, Designator
        FROM aircraft_types
        ''') #ambil data AircraftDescription, EngineCount, dan EngineType berdasarkan Designator
    df3 = spark.table('airports')

    #optimization: repartition dan coalesce
    df1 = df1.repartition(52)
    df2 = df2.coalesce(1)
    df3 = df3.repartition(8)

    #tabel dimensi aircraft
    df1 = df1.withColumn('row_id', monotonically_increasing_id())
    df2 = df2.withColumn('row_id', monotonically_increasing_id())

    df1 = df1.dropDuplicates().orderBy('row_id').drop('row_id') #drop data duplikat di df1 (jika ada), urutkan data, kemudian drop kolom `row_id`
    df2 = df2.dropDuplicates().orderBy('row_id').drop('row_id') #drop data duplikat di df2 (jika ada), urutkan data, kemudian drop kolom `row_id`

    df1.createOrReplaceTempView('df1')
    df2.createOrReplaceTempView('df2')

    dim_aircraft = spark.sql('''
        SELECT * from df1
        left join df2
        on df1.typecode = df2.Designator
        ''') #join dim_aircraft dengan df2_designator

    dim_aircraft = dim_aircraft.selectExpr('icao24', 'manufacturericao', 'registration', 'operator', 'owner',
                                           'model', 'serialnumber', 'AircraftDescription', 'engines', 'EngineType',
                                           'EngineCount', 'built', 'registered', 'reguntil') #pilih kolom-kolom yang dibutuhkan

    dim_aircraft = dim_aircraft.withColumnRenamed('manufacturericao', 'manufacturer_code')\
        .withColumnRenamed('registration', 'registration_code')\
        .withColumnRenamed('serialnumber', 'serial_number')\
        .withColumnRenamed('AircraftDescription', 'aircraft_desc')\
        .withColumnRenamed('engines', 'engine')\
        .withColumnRenamed('EngineType', 'engine_type')\
        .withColumnRenamed('EngineCount', 'engine_count')\
        .withColumnRenamed('built', 'built_date')\
        .withColumnRenamed('registered', 'registration_start')\
        .withColumnRenamed('reguntil', 'registration_end') #ganti beberapa nama kolom
    
    dim_aircraft = dim_aircraft.withColumn('engine_count', dim_aircraft['engine_count'].cast('INTEGER'))\
        .withColumn('built_date', dim_aircraft['built_date'].cast('DATE'))\
        .withColumn('registration_start', dim_aircraft['registration_start'].cast('DATE'))\
        .withColumn('registration_end', dim_aircraft['registration_end'].cast('DATE')) #ganti beberapa tipe data kolom
    
    dim_aircraft = dim_aircraft.withColumn('data_updated_at', current_date()) #buat kolom tanggal hari ini

    #tabel dimensi airports
    df3 = df3.withColumn('row_id', monotonically_increasing_id())
    df3 = df3.dropDuplicates() #drop data duplikat di df4 (jika ada)
    df3 = df3.orderBy('row_id').drop('row_id') #urutkan data, kemudian drop kolom `row_id`
    dim_airports = df3.selectExpr('icao', 'iata', 'name', 'country', 'municipality',
                                  'latitude', 'longitude', 'altitude', 'type') #susun urutan kolom
    dim_airports = dim_airports.withColumn('data_updated_at', current_date()) #buat kolom tanggal hari ini

    #load dimension table ke Hive
    spark.sql('CREATE DATABASE IF NOT EXISTS flights_dwh')
    spark.sql('USE flights_dwh')

    dim_aircraft.write.mode('overwrite').saveAsTable('dim_aircraft')
    dim_airports.write.mode('overwrite').saveAsTable('dim_airports')


    # dim_aircraft.show(5)
    # dim_aircraft.printSchema()
    # print(dim_aircraft.count())

    # dim_airports.show(5)
    # dim_airports.printSchema()
    # print(dim_airports.count())

# read_transform_load()