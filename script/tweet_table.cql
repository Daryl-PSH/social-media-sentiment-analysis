
CREATE KEYSPACE social_media
WITH REPLICATION = {
    'class': 'SimpleStrategy', 'replication_factor': 1
};

USE social_media;

CREATE TABLE tweets (
    year int,
    month int,
    day int,
    hour int,
    minute int,
    second int,
    ticker text,
    sentiment_score float,
    cleaned_text text,
    PRIMARY KEY((year, month, day), hour, minute, second, cleaned_text)
    WITH CLUSTERING ORDER BY(hour DESC, minute DESC, second DESC)
);