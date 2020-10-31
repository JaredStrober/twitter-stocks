# Twitter & Stocks
This repo contains the source codes for streaming and storing the tweets containing stock symbols.

## Requirements
### Python packages
```
pip3 install tweepy sqlite3 logging yaml --user

```
### Twitter API
For obtaining Twitter's API Keys and Access Tokens, please follow [this](https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/quick-start) instruction.
Once you obtained the keys, put them into a data/creds.yml:
```python
access_token: "X"
access_token_secret: "X"
consumer_key: "X"
consumer_secret: "X" 
```

## How to Run?

```
python twitter/stream_listener.py --db=stocks.db
                                  --api=data/creds.yml
                                  --stocks=data/stocks.yml
```
