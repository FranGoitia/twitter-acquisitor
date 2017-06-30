from tweepy import API, AppAuthHandler, Cursor
from geonamescache import GeonamesCache

from model import (User, Follower, Keyword, Tweet, Search, Country, City,
                   create_session, get_or_create)
from keys import CONSUMER_KEY, CONSUMER_SECRET, TWITTER_HANDLE


class Geo:

    def __init__(self, session):
        self.session = session
        self.geonames = GeonamesCache()
        self.countries = set([c['name'] for c in self.geonames.get_countries().values()])
        self.cities = set([c['name'] for c in self.geonames.get_cities().values()])

        # map cities names to country. when collisions, it maps the biggest city in the world
        self.cities_to_countries = {}
        codenames = {code: country['name'] for code, country in self.geonames.get_countries().items()}
        cities_pop = {}
        for c in self.geonames.get_cities().values():
            country = self.cities_to_countries.get(c['name'])
            if not country or c['population'] > cities_pop[c['name']]:
                cities_pop[c['name']] = c['population']
                self.cities_to_countries[c['name']] = codenames[c['countrycode']]

    def get_place(self, text_loc):
        """
        gets most likely place in the world from text_loc
        """
        city, country = None, None
        for w in text_loc.replace('/', ', ').replace('&', ', ').strip().split(', '):
            if w in self.cities:
                city = w
            if w in self.countries:
                country = w
        country = self.cities_to_countries[city] if (city and not country) else country
        if country:
            country = get_or_create(self.session, Country, name=country).id
        city = get_or_create(self.session, City, name=city, country_id=country)
        return city


class Acquisitor:

    def __init__(self, consumer_key, consumer_secret, handle, session):
        auth = AppAuthHandler(consumer_key, consumer_secret)
        self.api = API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        self.geo = Geo(session)
        self.handle = handle
        self.session = session

    def register_followers(self, user_handle):
        """
        registers every follower of given User
        """
        user = self.api.get_user(user_handle)
        followed = self._get_or_create_user(user, user.__dict__.get('status'))
        i = 0
        for follower in Cursor(self.api.followers, id=user_handle).items():
            i += 1
            f = self._get_or_create_user(follower, follower.__dict__.get('status'))
            relationship = self.session.query(Follower).filter_by(followed=followed, follower=f).first()
            if not relationship:
                self.session.add(Follower(follower=f, followed=followed))
                self.session.commit()

    def _get_or_create_user(self, user, status):
        u = self.session.query(User).filter_by(handle=user.screen_name).first()
        if not u:
            city = None
            if len(user.location.strip()) != 0:
                city = self.geo.get_place(user.location)

            u = User(user.id, user.screen_name, user.name, user.description,
                     user.created_at, status, user.followers_count,
                     user.friends_count, user.favourites_count,
                     user.statuses_count, user.lang, city)
            self.session.add(u)
            self.session.flush()
        return u

    def register_the_search(self, phrase, language):
        """
        register tweets associated with phrase. only gets tweets a week older
        """
        since_tweet = None
        max_id = None
        tweets_n = 0
        keyword = get_or_create(self.session, Keyword, text=phrase)
        while tweets_n < 10000000:
            kwargs = {'q': phrase, 'count': 100, 'lang': language}
            if max_id:
                kwargs['max_id'] = str(max_id - 1)
            if since_tweet:
                kwargs['since_id'] = since_tweet
            new_tweets = self.api.search(**kwargs)
            if not new_tweets:
                print("No more tweets found")
                break

            for tweet in new_tweets:
                t = self._create_tweet(tweet, keyword, language)
                search = Search(keyword=keyword, tweet=t, lang=language)
                self.session.add(search)
                self.session.commit()
            tweets_n += len(new_tweets)
            max_id = new_tweets[-1].id

    def _create_tweet(self, tweet, keyword, language):
        author = self.session.query(User).filter_by(twitter_id=tweet.user.id).first()
        if not author:
            author = self._get_or_create_user(tweet.user, tweet)
        t = Tweet(author=author, created_at=tweet.created_at, favourites_n=tweet.favorite_count,
                  retweets_n=tweet.retweet_count, text=tweet.text, tweet_id=tweet.id,
                  reply=tweet.in_reply_to_status_id is not None)
        self.session.add(t)
        self.session.flush()
        return t

    def follow(self, ids):
        """
        follows every user in ids
        """
        for user_id in ids:
            self.api.create_friendship(user_id)

    def unfollow(self, ids):
        """
        unfollow every user in ids
        """
        for user_id in ids:
            self.api.destroy_friendship(user_id)


if __name__ == '__main__':
    acquisitor = Acquisitor(CONSUMER_KEY, CONSUMER_SECRET, TWITTER_HANDLE, create_session())
    acquisitor.register_followers('chanchavia')  # burningman
    # acquisitor.register_the_search('#grandt', 'es')
