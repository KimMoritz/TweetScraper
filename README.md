# TweetScraper
Collects and compiles hashtags found in tweets with specified hashtag(s), using Apache Storm. Passes tuples on using jms messaging. Logs using Log4j. No tests written yet (Storm uses Clojure). I have been using this application in association with Apache ActiveMQ.

Before you compile the files, create a .properties file based on the .properties.example file provided under resources.
