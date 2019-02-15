def client_products(customer_id,df_na_report):

    unique_prod_list = list(df_na_report['product_id'].unique())
    uni_prod_sql = str(unique_prod_list).replace("[",'').replace("]",'')

    query_product= ('''SELECT
        p.customer_id,
        pbb.bucket_name,
        p.product_id,
            p.product_name,
            p.active,
            p.external_id,
            p.image_url,
            sba.attribute_id,
            sba.bucket_id,
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
        for data_prod in pd.read_sql(query_product.format(customer_id,unique_products_sql),conn,chunksize=100000):
            if data_prod.shape[0] == 0:
                print('No data_prod available in your time frame for Cust_ID ',cust)
            temp_data = pd.merge(df_na_report,data_prod,how='inner',on=['product_id','company_id'])
            #data_prod.to_csv('C:/Users/groupby/Documents/Python Scripts/NA_Reports/data/Weekly_NA_products.csv',mode='a',index=False,header=False)
            agg_data = agg_data.append(temp_data)
        return agg_data
        print('Cust_id ',cust, 'Product query and data clean completed')
