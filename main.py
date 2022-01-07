import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

VIEWS_SCHEMA={
    'fields':[
        {'name':'cust_tier_code', 'type':'STRING', 'mode':'REQUIRED'},
        {'name':'sku', 'type':'INTEGER', 'mode':'REQUIRED'},
        {'name':'total_no_of_product_views', 'type':'INTEGER', 'mode':'REQUIRED'}
    ]
}

SALES_SCHEMA={
    'fields':[
        {'name':'cust_tier_code', 'type':'STRING', 'mode':'REQUIRED'},
        {'name':'sku', 'type':'INTEGER', 'mode':'REQUIRED'},
        {'name':'total_sales_amount', 'type':'FLOAT', 'mode':'REQUIRED'}
    ]
}

def run():
    pipeline_options = PipelineOptions(
        project='york-cdf-start',
        region='us-central1',
        temp_location='gs://pas_df_bucket/dataflow/tmp/',
        staging_location='gs://pas_df_bucket/dataflow/staging/',
        save_main_session=True,
        job_name='pa-test'
    )

    with beam.Pipeline(options=pipeline_options, runner='DataflowRunner') as p:
        raw_data = p | 'Read in from BigQuery' >> beam.io.ReadFromBigQuery(
            query="""
            SELECT CUST_TIER_CODE, ord.SKU, SUM(ORDER_AMT) AS total_sales_amount, COUNT(EVENT_TM) AS total_no_of_product_veiws \
            FROM `york-cdf-start.final_input_data.customers` AS cust \
            JOIN `york-cdf-start.final_input_data.orders` AS ord ON cust.CUSTOMER_ID = ord.CUSTOMER_ID \
            JOIN `york-cdf-start.final_input_data.product_views` AS pv ON cust.CUSTOMER_ID = pv.CUSTOMER_ID \
            GROUP BY ord.SKU, CUST_TIER_CODE \
            ORDER BY ord.SKU
            """,
            use_standard_sql=True
        )

        # views | 'Print to BQ Table' >> beam.io.WriteToBigQuery(
        #     table="york-cdf-start:",
        #     schema=VIEWS_SCHEMA,
        #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        pass

if __name__ == "__main__":
    run()
    pass