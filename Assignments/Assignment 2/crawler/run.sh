python tweet_harvester.py --city adelaide --since 2021-05-16 --until 2021-05-24 --max_tweets 2500 > adelaide.log &
sleep 10s
python tweet_harvester.py --city brisbane --since 2021-05-16 --until 2021-05-24 --max_tweets 2500 > brisbane.log &
sleep 10s
python tweet_harvester.py --city melbourne --since 2021-05-16 --until 2021-05-24 --max_tweets 2500 > melbourne.log &
sleep 10s
python tweet_harvester.py --city sydney --since 2021-05-16 --until 2021-05-24 --max_tweets 2500 > sydney.log &
