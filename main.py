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

class sales_filter(beam.DoFn):
    sf={}
    def process(self, element):
        sf = {
            'cust_tier_code': str(element['CUST_TIER_CODE']),
            'sku': int(element['SKU']),
            'total_sales_amount': element['total_sales_amount']
        }
        yield sf

class view_filter(beam.DoFn):
    vf={}
    def process(self, element):
        vf= {
            'cust_tier_code': str(element['CUST_TIER_CODE']),
            'sku': int(element['SKU']),
            'total_no_of_product_views': element['total_no_of_product_views']
        }
        yield vf

def run():
    pipeline_options = PipelineOptions(
        project='york-cdf-start',
        region='us-central1',
        temp_location='gs://pas_df_bucket/dataflow/tmp/',
        staging_location='gs://pas_df_bucket/dataflow/staging/',
        save_main_session=True,
        job_name='patrick-andresen-final-job'
    )

    with beam.Pipeline(options=pipeline_options, runner='DataflowRunner') as p:
        raw_view_data = p | 'Views Query from BigQuery' >> beam.io.ReadFromBigQuery(
            query="""
            SELECT CUST_TIER_CODE, SKU, COUNT(EVENT_TM) AS total_no_of_product_views
            FROM `york-cdf-start.final_input_data.customers` AS cust
            JOIN `york-cdf-start.final_input_data.product_views` AS pv ON cust.CUSTOMER_ID = pv.CUSTOMER_ID
            GROUP BY SKU, CUST_TIER_CODE
            """,
            use_standard_sql=True
        )

        raw_sales_data = p | 'Sales Query from BigQuery' >> beam.io.ReadFromBigQuery(
            query="""
            SELECT CUST_TIER_CODE, SKU, SUM(ORDER_AMT)
            FROM `york-cdf-start.final_input_data.customers` AS cust
            JOIN `york-cdf-start.final_input_data.orders` AS o ON cust.CUSTOMER_ID = o.CUSTOMER_ID
            GROUP BY SKU, CUST_TIER_CODE
            """,
            use_standard_sql=True
        )

        raw_view_data | 'Transform for views table' >> beam.ParDo(view_filter()) | 'Print to Views Table' >> beam.io.WriteToBigQuery(
            table='york-cdf-start:final_patrick_andresen.cust_tier_code-sku-total_no_of_product_views',
            schema=VIEWS_SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

        raw_sales_data | 'Transform for sales table' >> beam.ParDo(sales_filter()) | 'Print to Sales Table' >> beam.io.WriteToBigQuery(
            table='york-cdf-start:final_patrick_andresen.cust_tier_code-sku-total_sales_amount',
            schema=VIEWS_SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        pass

if __name__ == "__main__":
    run()
    pass