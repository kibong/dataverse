from dataverse.etl import ETLPipeline
etl_pipeline = ETLPipeline()

from pyspark.sql import SparkSession
spark = (
    SparkSession.builder.appName("Python Spark SQL basic example")
    .getOrCreate()
)

jsonl_files = ['s3://team-model-data-preprocess/moreh_corous_jsonl/cc-100_한국어데이터셋_cc-100_part1.jsonl', # 653MB
               's3://team-model-data-preprocess/moreh_corous_jsonl/모레_하반기_데이터수집_네이버블로그_data116.jsonl', # 745MB
               's3://team-model-data-preprocess/moreh_corous_jsonl/모레_하반기_데이터수집_국내특허본문_키프리스_2023.jsonl', # 823MB
               ]
save_path = 's3://team-model-data-preprocess/moreh_corpus_processed/test_0401'

# load dataset from S3
load = etl_pipeline.get('data_ingestion___jsonl___jsonl2raw')
data = load()(spark, path=jsonl_files)

# language filter 
lang_filter = etl_pipeline.get('quality___language___fasttext_filter')
data = lang_filter()(spark, data, whitelist=['ko'], threshold=0.5)


# heuristic filter

# cleaning___length___word_len_filter
# between 50 and 100,000 words
word_len_filter = etl_pipeline.get('cleaning___length___word_len_filter')
data = word_len_filter()(spark, data, min_len=50, max_len=100000)

# cleaning___length___mean_word_len_filter
# mean word length filter
mean_word_len_filter = etl_pipeline.get('cleaning___length___mean_word_len_filter')
data = mean_word_len_filter()(spark, data, min_len=2, max_len=8)

# cleaning___html___extract_plain_text
# Extracts plain text from HTML
html_filter = etl_pipeline.get('cleaning___html___extract_plain_text')
data = html_filter()(spark, data)

# cleaning___korean___reduce_emoticon
reduce_emoticon_filter = etl_pipeline.get('cleaning___korean___reduce_emoticon')
data = reduce_emoticon_filter()(spark, data)



# dedup filter

# deduplication___exact___column
# exact dedup
exact_dedup = etl_pipeline.get('deduplication___exact___column')
data = exact_dedup()(spark, data)

# deduplication___minhash___lsh_jaccard
# fussy dedup
fussy_dedup = etl_pipeline.get('deduplication___minhash___lsh_jaccard')
data = fussy_dedup()(spark, data)


# bad word filter
# cleaning___content___bad_words_filter
bad_word_filter = etl_pipeline.get('cleaning___content___bad_words_filter')
data = bad_word_filter()(spark, data)


# save dataset to S3
save = etl_pipeline.get('data_save___jsonl___ufl2jsonl')
data = save()(spark, data, save_path, repartition=10)