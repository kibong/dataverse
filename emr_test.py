from dataverse.etl import ETLPipeline
etl_pipeline = ETLPipeline()

from pyspark.sql import SparkSession, Row
spark = (
    SparkSession.builder.appName("Python Spark SQL basic example")
    .getOrCreate()
)

jsonl_files = ['s3://team-model-data-preprocess/moreh_corous_jsonl/cc-100_한국어데이터셋_cc-100_part1.jsonl', # 653MB
               's3://team-model-data-preprocess/moreh_corous_jsonl/모레_하반기_데이터수집_네이버블로그_data116.jsonl', # 745MB
               's3://team-model-data-preprocess/moreh_corous_jsonl/모레_하반기_데이터수집_국내특허본문_키프리스_2023.jsonl', # 823MB
               ]
save_path = 's3://team-model-data-preprocess/moreh_corpus_processed/0401'

# Function to write filter statistics to S3
def write_log_to_s3(filter_name, pre_filter_count, post_filter_count, initial_count):
    filtered_out_this_step = pre_filter_count - post_filter_count
    percent_filtered_this_step = (filtered_out_this_step / pre_filter_count) * 100 if pre_filter_count else 0
    filtered_out_total = initial_count - post_filter_count
    percent_filtered_total = (filtered_out_total / initial_count) * 100 if initial_count else 0

    log_message = f"Filter: {filter_name}\n" \
                  f"Before this filter: {pre_filter_count}, After this filter: {post_filter_count}\n" \
                  f"Filtered out by this step: {filtered_out_this_step} rows ({percent_filtered_this_step:.2f}%)\n" \
                  f"Initial count: {initial_count}, Current total filtered: {filtered_out_total} rows ({percent_filtered_total:.2f}% of initial)"

    # Generate a timestamp for the log file to ensure uniqueness
    
    log_file_path = f"{save_path}/{filter_name.replace(' ', '_')}_log.txt"
    
    # Create a DataFrame with the log message
    log_df = spark.createDataFrame([Row(log_message=log_message)])
    
    # Write the log message to S3
    log_df.coalesce(1).write.mode("overwrite").text(log_file_path)

# load dataset from S3
load = etl_pipeline.get('data_ingestion___jsonl___jsonl2raw')
data = load()(spark, path=jsonl_files)
initial_count = data.count()
post_count = initial_count

'''
# language filter 
lang_filter = etl_pipeline.get('quality___language___fasttext_filter')
data = lang_filter()(spark, data, whitelist=['ko'], threshold=0.5)
pre_count = post_count
post_count = data.count()
write_log_to_s3('Language Filter', pre_count, post_count, initial_count)
'''



# heuristic filter

# cleaning___length___word_len_filter
# between 50 and 100,000 words
word_len_filter = etl_pipeline.get('cleaning___length___word_len_filter')
data = word_len_filter()(spark, data, min_len=50, max_len=100000)
pre_count = post_count 
post_count = data.count()

write_log_to_s3('Word Length Filter', pre_count, post_count, initial_count)

# cleaning___length___mean_word_len_filter
# mean word length filter
mean_word_len_filter = etl_pipeline.get('cleaning___length___mean_word_len_filter')
data = mean_word_len_filter()(spark, data, min_len=2, max_len=8)
pre_count = post_count
post_count = data.count()
write_log_to_s3('Mean Word Length Filter', pre_count, post_count, initial_count)

# cleaning___html___extract_plain_text
# Extracts plain text from HTML
html_filter = etl_pipeline.get('cleaning___html___extract_plain_text')
data = html_filter()(spark, data)
pre_count = post_count
post_count = data.count()
write_log_to_s3('HTML Extract Filter', pre_count, post_count, initial_count)

# cleaning___korean___reduce_emoticon
reduce_emoticon_filter = etl_pipeline.get('cleaning___korean___reduce_emoticon')
data = reduce_emoticon_filter()(spark, data)
pre_count = post_count
post_count = data.count()
write_log_to_s3('Reduce Emoticon Filter', pre_count, post_count, initial_count)


# dedup filter

# deduplication___exact___column
# exact dedup
exact_dedup = etl_pipeline.get('deduplication___exact___column')
data = exact_dedup()(spark, data)
pre_count = post_count
post_count = data.count()
write_log_to_s3('Exact Dedup Filter', pre_count, post_count, initial_count)

# deduplication___minhash___lsh_jaccard
# fussy dedup
fussy_dedup = etl_pipeline.get('deduplication___minhash___lsh_jaccard')
data = fussy_dedup()(spark, data)
pre_count = post_count
post_count = data.count()
write_log_to_s3('Fussy Dedup Filter', pre_count, post_count, initial_count)



# bad word filter
# cleaning___content___bad_words_filter
bad_word_filter = etl_pipeline.get('cleaning___content___bad_words_filter')
data = bad_word_filter()(spark, data)
pre_count = post_count
post_count = data.count()
write_log_to_s3('Bad Word Filter', pre_count, post_count, initial_count)


# save dataset to S3
save = etl_pipeline.get('data_save___jsonl___ufl2jsonl')
data = save()(spark, data, save_path, repartition=1)


