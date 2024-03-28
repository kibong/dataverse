from dataverse.etl import ETLPipeline
etl_pipeline = ETLPipeline()
job0 = etl_pipeline.get('data_ingestion___test___generate_fake_ufl')
data = job0()(spark, n=50, etl_name='data_ingestion___test___generate_fake_ufl')
job0 = etl_pipeline.get('deduplication___minhash___lsh_jaccard')
data = job0()(spark, data, etl_name='deduplication___minhash___lsh_jaccard')
job0 = etl_pipeline.get('data_save___huggingface___ufl2hf_obj')
data = job0()(spark, data, etl_name='data_save___huggingface___ufl2hf_obj')