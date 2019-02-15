import pandas as pd

#Get the previous date
from datetime import date, timedelta, datetime
import psycopg2
import time

#importing database
from database_config import postgres as cfg

todays_date = str(datetime.today())[:10]
class Reports:
    def __init__(self,conn):
        self.conn = psycopg2.connect(**cfg)
        

    def client_na_report(self,customer_id):
        """
        Description: Run a null(n/a) report for a specific client
            This can be run with a loop

        """
        conn = self.conn
        client_na_query = (
        '''SELECT
                ct."id" AS curation_task_id,
                ct.customer_id,
                ct.resolution,
                --- truncated the timestamp from the date for aggregation.
                date_trunc('day',ct.started_at) as "started_at",
                cpf.product_id,
                cpf."name" AS attribute_name,
                a.attribute_id,
                cpf."Total_NAs",
                cpf.curation_tasks_count
            FROM "curation_tasks" AS ct
            INNER JOIN
                (SELECT
                    cpf.curation_task_id,
                    cpf.customer_id,
                    cpf.product_id,
                    cpf.name,
                    SUM(CASE WHEN cpf.value = 'n/a' THEN 1 ELSE 0 END) as "Total_NAs",
                    COUNT(cpf.id) AS curation_tasks_count
                FROM "public"."curated_product_fields"  as cpf
                WHERE customer_id = {0}
                GROUP BY
                    cpf.curation_task_id,
                    cpf.customer_id,
                    cpf.product_id,
                    cpf.name
                ) AS cpf
            ON ct.id = cpf.curation_task_id
            INNER JOIN (SELECT
                            id as attribute_id
                            ,snake_case_name as attribute_name
                        FROM attributes) AS a
            ON cpf.name = a.attribute_name
            WHERE started_at >= CURRENT_TIMESTAMP - INTERVAL '1 week'
            --- specific resolution that are not taken care of my rules or bulk
            AND (ct.resolution IS NULL OR ct.resolution = 'misclassified')
            AND ct.customer_id = {0}
            ORDER BY started_at;
        ''')


        #Returning the data in pandas to export it as a CSV.
        data = pd.read_sql(client_na_query.format(customer_id),self.conn)

        #if there is data convert the date to a date element and append the customer name
        if data.shape[0] != 0:
        else:
            print('No data available in your time frame for Cust_id ',customer_id)
            pass
        return data

    def client_products(self, customer_id):
        df_na_report = self.client_na_report(customer_id)
        conn = self.conn
       
        unique_product_list = list(df_na_report['product_id'].unique())
        unique_products_sql = str(unique_product_list).replace("[",'').replace("]",'')
    
        query_product= ('''
            SELECT
                p.customer_id,
                pbb.bucket_name,
                p.product_id,
                p.product_name,
                p.active,
                p.external_id,
                p.image_url,
                sba.family_friendly
            FROM (SELECT
                        p.id as product_id,
                        p.active,
                        p.customer_id,
                        p.name as product_name,
                        p.external_id,
                        p.image_url
                    FROM products as p
                    WHERE p.customer_id  = {}
                    AND p.active = 't'
                    AND p.id in ({})) AS p
            INNER JOIN (SELECT
                        pb.product_id
                        ,pb.bucket_id
                        ,buc.bucket_name
                        FROM products_buckets AS pb
                        INNER JOIN (SELECT
                                        id AS bucket_id
                                        ,name AS bucket_name
                                    FROM buckets) AS buc
                        ON buc.bucket_id = pb.bucket_id) AS pbb
            ON p.product_id = pbb.product_id
            INNER JOIN (SELECT
                            sb.bucket_id,
                            sxa.attribute_id,
                            family_friendly
                            FROM strategy_buckets as sb
                                INNER JOIN (SELECT
                                                id,
                                                strategy_bucket_id,
                                                attribute_id,
                                                family_friendly
                                                FROM strategy_buckets_attributes) as sxa
                                ON sb.id = sxa.strategy_bucket_id) as sba
            ON pbb.bucket_id = sba.bucket_id
            ''')


        agg_data = pd.DataFrame()
        for data_prod in pd.read_sql(query_product.format(customer_id,unique_products_sql),conn,chunksize=100000):
            if data_prod.shape[0] == 0:
                print('No data_prod available in your time frame for Cust_ID ',cust)
            else:
                print(str(data_prod.shape[0])+" Being Processed")
                agg_data = agg_data.append(data_prod)
        return agg_data
