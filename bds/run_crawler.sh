# # Crawler parameters
# export spider_name=bds_spider;
# export min_page=1;
# export max_page=100;
# export province="ha-noi";
# export jump_to_page=$min_page;
# # Kafka parameters
# export kafka_bootstrap_servers="localhost:9192,localhost:9292,localhost:9392"
# Run crawler
scrapy crawl bds_spider -a min_page=1 -a max_page=100 -a province="ha-noi" -a jump_to_page=1 -s KAFKA_BOOTSTRAP_SERVERS="localhost:9192,localhost:9292,localhost:9392"
scrapy crawl bds_spider -a min_page=1 -a max_page=100 -a province="ha-noi" -a jump_to_page=1