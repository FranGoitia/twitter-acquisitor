from datetime import datetime
from sqlalchemy import (create_engine, Column, Integer, String, ForeignKey, Date,
                        Boolean, DateTime, UniqueConstraint, PrimaryKeyConstraint)
from sqlalchemy.orm import scoped_session, sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()
DB_URL = 'sqlite:///antoine.db'


def create_session():
    Engine = create_engine(DB_URL, echo=False)
    SessionMaker = sessionmaker(bind=Engine, autoflush=False)
    Session = scoped_session(SessionMaker)
    return Session


def create_db():
    Engine = create_engine(DB_URL, echo=False)
    Base.metadata.create_all(Engine, checkfirst=True)


def get_or_create(session, model, **kwargs):
    instance = session.query(model).filter_by(**kwargs).first()
    if instance:
        return instance
    else:
        instance = model(**kwargs)
        session.add(instance)
        session.flush()
        return instance


class Country(Base):
    __tablename__ = 'countries'

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)

    def __repr__(self):
        return 'Country({0})'.format(self.name)


class City(Base):
    __tablename__ = 'cities'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    country_id = Column(ForeignKey('countries.id'), index=True)
    country = relationship('Country', backref='cities')

    UniqueConstraint(name, country_id)

    def __repr__(self):
        return 'City({0}, {1})'.format(self.name, self.country.name)


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    twitter_id = Column(Integer, unique=True)
    handle = Column(String)
    name = Column(String)
    description = Column(String)
    city_id = Column(ForeignKey('cities.id'), index=True)
    city = relationship('City', backref='users')
    created_at = Column(Date)
    days_since_tweet = Column(Integer)
    tweets_n = Column(Integer)
    favourites_n = Column(Integer)
    followers_n = Column(Integer)
    following_n = Column(Integer)

    def __init__(self, twitter_id, handle, name, description, created_at, last_tweet,
                 followers_n, following_n, favourites_n, tweets_n, lang, city):
        self.city = city
        self.twitter_id = twitter_id
        self.handle = handle
        self.name = name
        self.description = description
        self.created_at = created_at
        if last_tweet:
            self.days_since_tweet = abs(datetime.today() - last_tweet.created_at).days
        self.followers_n = followers_n
        self.following_n = following_n
        self.favourites_n = favourites_n
        self.tweets_n = tweets_n

    def __repr__(self):
        return 'User({0}, {1}, {2}, {3})'.format(self.twitter_id, self.handle, self.name, self.created_at)


class Follower(Base):
    __tablename__ = 'followers'

    follower_id = Column(ForeignKey('users.id'), index=True)
    follower = relationship('User', foreign_keys=[follower_id], backref='following')
    followed_id = Column(ForeignKey('users.id'), index=True)
    followed = relationship('User', foreign_keys=[followed_id], backref='followers')
    datetime = Column(DateTime, nullable=True)

    PrimaryKeyConstraint(follower_id, followed_id)

    def __repr__(self):
        return 'Follower({0}, {1})'.format(self.follower_id, self.followed_id)


class Keyword(Base):
    __tablename__ = 'keywords'

    id = Column(Integer, primary_key=True)
    text = Column(String, unique=True)

    def __repr__(self):
        return 'Keyword({0})'.format(self.text)


class Tweet(Base):
    __tablename__ = 'tweets'

    id = Column(Integer, primary_key=True)
    author_id = Column(ForeignKey('users.id'), index=True)
    author = relationship('User', foreign_keys=[author_id])
    created_at = Column(DateTime)
    favourites_n = Column(Integer)
    retweets_n = Column(Integer)
    text = Column(String)
    reply = Column(Boolean)
    tweet_id = Column(Integer, unique=True)

    def __repr__(self):
        return 'Tweet({0}, {1}, {2})'.format(self.text, self.author.name, self.created_at)


class Search(Base):
    __tablename__ = 'searches'

    keyword_id = Column(ForeignKey('keywords.id'), index=True)
    keyword = relationship('Keyword', backref='searches')
    tweet_id = Column(ForeignKey('tweets.id'), index=True)
    tweet = relationship('Tweet')
    lang = Column(String)

    PrimaryKeyConstraint(keyword_id, tweet_id)

    def __repr__(self):
        return 'Search({0}, {1})'.format(self.keyword.text, self.tweet.text)
