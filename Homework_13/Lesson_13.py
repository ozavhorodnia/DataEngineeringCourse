#!/usr/bin/env python
# coding: utf-8

# In[104]:


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank


# In[2]:


spark = SparkSession.builder    .config('spark.driver.extraClassPath'
            , '/home/user/shared_folder/postgresql-42.3.1.jar')\
    .master('local')\
    .appName("lesson_13")\
    .getOrCreate()


# In[3]:


pg_url = "jdbc:postgresql://127.0.0.1:5432/postgres"
pg_creds = {"user": "pguser", "password": "secret"}


# In[4]:


df_category = spark.read.jdbc(pg_url, table = 'category', properties = pg_creds)


# In[5]:


df_film_category = spark.read.jdbc(pg_url, table = 'film_category', properties = pg_creds)


# In[6]:


df_film = spark.read.jdbc(pg_url, table = 'film', properties = pg_creds)


# In[27]:


df_film_actor = spark.read.jdbc(pg_url, table = 'film_actor', properties = pg_creds)


# In[60]:


df_actor = spark.read.jdbc(pg_url, table = 'actor', properties = pg_creds)


# In[61]:


df_inventory = spark.read.jdbc(pg_url, table = 'inventory', properties = pg_creds)


# In[122]:


df_customer = spark.read.jdbc(pg_url, table = 'customer', properties = pg_creds)


# In[123]:


df_address = spark.read.jdbc(pg_url, table = 'address', properties = pg_creds)


# In[124]:


df_city = spark.read.jdbc(pg_url, table = 'city', properties = pg_creds)


# In[163]:


df_rental = spark.read.jdbc(pg_url, table = 'rental', properties = pg_creds)


# In[195]:


df_1 = df_film_category.join(df_category
    ,df_film_category.category_id == df_category.category_id)\
    .select(df_film_category.film_id, df_category.name.alias('category_name'))\
    .groupBy('category_name').count().orderBy(F.col("count").desc()).show()


# In[196]:


df_2 = df_film.join(df_film_actor, df_film.film_id == df_film_actor.film_id)                .join(df_actor, df_film_actor.actor_id == df_actor.actor_id)                .select(F.col('rental_duration'), F.concat(
                             F.col('first_name')
                             , F.lit(' ')
                             ,F.col('last_name')
                         ).alias("actor_name"))\
    .groupBy('actor_name').sum('rental_duration').orderBy(F.col("actor_name").desc()).show(10)


# In[197]:


df_3 = df_category.join(df_film_category, df_category.category_id == df_film_category.category_id)                .join(df_film, df_film_category.film_id == df_film.film_id)                .select(
                F.col('name').alias('category_name')
                ,(F.col('rental_duration')*F.col('rental_rate')).alias('cost')
                )\
    .groupBy('category_name')\
    .agg(F.sum('cost').alias('cost'))\
    .sort(F.desc('cost')).show(1)


# In[202]:


df_4 = df_film.join(df_inventory, df_film.film_id == df_inventory.film_id, 'left')    .filter(df_inventory.film_id.isNull())    .select(F.col('title')).show(100)    


# In[203]:


windowSpec  = Window.orderBy(F.col("count").desc())

df_5 = df_category.join(df_film_category, df_category.category_id == df_film_category.category_id)        .join(df_film, df_film_category.film_id == df_film.film_id)        .join(df_film_actor, df_film.film_id == df_film_actor.film_id)        .join(df_actor, df_film_actor.actor_id == df_actor.actor_id)        .where(df_category.name == 'Children')        .select(F.concat(F.col('first_name'), F.lit(' '), F.col('last_name')).alias("actor_name")
               ,df_film.film_id)\
        .groupBy('actor_name')\
        .agg(F.countDistinct('film_id').alias('count'))\
        .withColumn('dense_rank', dense_rank().over(windowSpec))\
        .select(F.col('actor_name'))\
        .where(F.col('dense_rank') <= 3) \
        .show()
                


# In[208]:


df_6 = df_customer.join(df_address, df_customer.address_id == df_address.address_id)    .join(df_city, df_address.city_id == df_city.city_id)    .withColumn('active_cnt', F.when(F.col('active') == 1, 1).otherwise(0))    .withColumn('inactive_cnt', F.when(F.col('active') == 0, 1).otherwise(0))    .select('city', 'active_cnt', 'inactive_cnt')    .groupBy('city')    .agg(F.sum('active_cnt').alias('active_cnt')
         , F.sum('inactive_cnt').alias('inactive_cnt'))\
    .sort(F.desc('inactive_cnt'))
df_6.show(df_6.count(), False)


# In[210]:


windowSpec  = Window.orderBy(F.col("rental_duration").desc())
df_7_1 = df_rental.join(df_inventory, df_rental.inventory_id == df_inventory.inventory_id)        .join(df_film, df_inventory.film_id == df_film.film_id)        .join(df_film_category, df_film_category.film_id == df_film.film_id)        .join(df_category, df_film_category.category_id == df_category.category_id)        .where(F.col('title').like("A%"))        .select(
            df_category.name.alias('category_name')
            ,df_film.rental_duration
            )\
        .groupBy('category_name')\
        .agg(F.sum('rental_duration').alias('rental_duration'))\
        .withColumn('rank', dense_rank().over(windowSpec))

df_7_2 = df_rental.join(df_customer, df_rental.customer_id == df_customer.customer_id)        .join(df_address, df_customer.address_id == df_address.address_id)        .join(df_city, df_address.city_id == df_city.city_id)        .join(df_inventory, df_rental.inventory_id == df_inventory.inventory_id)        .join(df_film, df_inventory.film_id == df_film.film_id)        .join(df_film_category, df_film_category.film_id == df_film.film_id)        .join(df_category, df_film_category.category_id == df_category.category_id)        .where(F.col('city').like("%-%"))        .select(
            df_category.name.alias('category_name')
            ,df_film.rental_duration
            )\
        .groupBy('category_name')\
        .agg(F.sum('rental_duration').alias('rental_duration'))\
        .withColumn('rank', dense_rank().over(windowSpec))

df_7 = df_7_1.unionAll(df_7_2)    .where(F.col('rank') == 1)    .select(F.col('category_name'))    .show()


# In[ ]:




