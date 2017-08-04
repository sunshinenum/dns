# project description
Detection and suppression of Water Torture DDoS in DNS with Machine Learning Method.

# requirements
- ubuntu 16.04 x64
- java 1.7
- python 2.7.0
- hadoop 2.6.0
- hive 1.2
- dbvis https://www.dbvis.com/features/tour/dbvisualizer-editions/
- spark 2.0.0
- memory: 12G
- swap 8G
- CPU: Intel® Core™ i5-3317U CPU @ 1.70GHz × 4
- hard disk: 120G
# steps of experiment
- import logs
- import names
- score names train model
- generate attack queries
- python feature extraction, generate sql, execute sql with dbvis [import raw data]
- score name load model, score queries
- execute sql again [extract features]
- train test store model
- load model, performance test
