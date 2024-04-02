import boto3
import datetime
from dataverse.etl import ETLPipeline
etl_pipeline = ETLPipeline()

from pyspark.sql import SparkSession, Row
spark = (
    SparkSession.builder.appName("Python Spark SQL basic example")
    .getOrCreate()
)


from moreh_corpus_jsonl_files import jsonl_files
timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
save_path = f's3://team-model-data-preprocess/moreh_corpus_processed/emr_final_{timestamp}'
log_messages = []

# Modified function to accumulate log messages instead of writing them immediately
def accumulate_log_message(filter_name, pre_filter_count, post_filter_count, initial_count):
    filtered_out_this_step = pre_filter_count - post_filter_count
    percent_filtered_this_step = (filtered_out_this_step / pre_filter_count) * 100 if pre_filter_count else 0
    filtered_out_total = initial_count - post_filter_count
    percent_filtered_total = (filtered_out_total / initial_count) * 100 if initial_count else 0

    log_message = f"Filter: {filter_name}\n" \
                  f"Before this filter: {pre_filter_count}, After this filter: {post_filter_count}\n" \
                  f"Filtered out by this step: {filtered_out_this_step} rows ({percent_filtered_this_step:.2f}%)\n" \
                  f"Initial count: {initial_count}, Current total filtered: {filtered_out_total} rows ({percent_filtered_total:.2f}% of initial)"
    
    log_messages.append(log_message)

# Function to write all accumulated log messages to a single text file in S3
def write_logs_to_s3():
    s3 = boto3.resource('s3')
    full_log_message = "\n\n".join(log_messages)
    bucket_name = save_path.split('/')[2]
    log_file_path = '/'.join(save_path.split('/')[3:]) + "/filter_log.txt"
    
    obj = s3.Object(bucket_name, log_file_path)
    obj.put(Body=full_log_message.encode('utf-8'))

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
accumulate_log_message('Language Filter', pre_count, post_count, initial_count)
'''

# heuristic filter

# cleaning___length___word_len_filter
# between 50 and 100,000 words
word_len_filter = etl_pipeline.get('cleaning___length___word_len_filter')
data = word_len_filter()(spark, data, min_len=50, max_len=100000)
pre_count = post_count 
post_count = data.count()
accumulate_log_message('Word Length Filter', pre_count, post_count, initial_count)

# cleaning___length___mean_word_len_filter
# mean word length filter
mean_word_len_filter = etl_pipeline.get('cleaning___length___mean_word_len_filter')
data = mean_word_len_filter()(spark, data, min_len=2, max_len=8)
pre_count = post_count
post_count = data.count()
accumulate_log_message('Mean Word Length Filter', pre_count, post_count, initial_count)

# cleaning___html___extract_plain_text
# Extracts plain text from HTML
'''
html_filter = etl_pipeline.get('cleaning___html___extract_plain_text')
data = html_filter()(spark, data)
pre_count = post_count
post_count = data.count()
accumulate_log_message('HTML Extract Filter', pre_count, post_count, initial_count)
'''

# cleaning___korean___reduce_emoticon
reduce_emoticon_filter = etl_pipeline.get('cleaning___korean___reduce_emoticon')
data = reduce_emoticon_filter()(spark, data)
pre_count = post_count
post_count = data.count()
accumulate_log_message('Reduce Emoticon Filter', pre_count, post_count, initial_count)


# dedup filter

# deduplication___exact___column
# exact dedup
exact_dedup = etl_pipeline.get('deduplication___exact___column')
data = exact_dedup()(spark, data)
pre_count = post_count
post_count = data.count()
accumulate_log_message('Exact Dedup Filter', pre_count, post_count, initial_count)

# deduplication___minhash___lsh_jaccard
# fussy dedup
fussy_dedup = etl_pipeline.get('deduplication___minhash___lsh_jaccard')
data = fussy_dedup()(spark, data)
pre_count = post_count
post_count = data.count()
accumulate_log_message('Fussy Dedup Filter', pre_count, post_count, initial_count)



# bad word filter
# cleaning___content___bad_words_filter
bad_word_filter = etl_pipeline.get('cleaning___content___bad_words_filter')
data = bad_word_filter()(spark, data)
pre_count = post_count
post_count = data.count()
accumulate_log_message('Bad Word Filter', pre_count, post_count, initial_count)


# save dataset to S3
save = etl_pipeline.get('data_save___jsonl___ufl2jsonl')
data = save()(spark, data, save_path, repartition=512)
write_logs_to_s3()

