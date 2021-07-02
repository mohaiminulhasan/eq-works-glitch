# -*- coding: utf-8 -*-

import os, time, sqlalchemy
from redis import Redis

redis = Redis(host=os.getenv('REDIS_HOSTNAME'), port=os.getenv('REDIS_PORT'), password=os.getenv('REDIS_PASSWORD'))

from functools import update_wrapper
from flask import json, request, g
from flask import Flask, jsonify
from flask_cors import CORS

# web app
app = Flask(__name__)
CORS(app)

class RateLimit(object):
    expiration_window = 10

    def __init__(self, key_prefix, limit, per, send_x_headers) -> None:
        self.reset = (int(time.time()) // per) * per + per
        self.key = key_prefix + str(self.reset)
        self.limit = limit
        self.per = per
        self.send_x_headers = send_x_headers
        p = redis.pipeline()
        p.incr(self.key)
        p.expireat(self.key, self.reset + self.expiration_window)
        self.current = min(p.execute()[0], limit)
        super().__init__()
    
    remaining = property(lambda x: x.limit - x.current)
    over_limit = property(lambda x: x.current >= x.limit)

def get_view_rate_limit():
    return getattr(g, '_view_rate_limit', None)

def on_over_limit(limit):
    return (jsonify({'data': 'You hit the rate limit', 'error': '429'}), 429)

def ratelimit(limit, per=300, send_x_headers=True,
                over_limit=on_over_limit,
                scope_func=lambda: request.remote_addr,
                key_func=lambda: request.endpoint):
    def decorator(f):
        def rate_limited(*args, **kwargs):
            key = 'rate-limit/%s/%s/' % (key_func(), scope_func())
            rlimit = RateLimit(key, limit, per, send_x_headers)
            g._view_rate_limit = rlimit
            if over_limit is not None and rlimit.over_limit:
                return over_limit(rlimit)
            return f(*args, **kwargs)
        return update_wrapper(rate_limited, f)
    return decorator

@app.after_request
def inject_x_rate_headers(response):
    limit = get_view_rate_limit()
    if limit and limit.send_x_headers:
        h = response.headers
        h.add('X-RateLimit-Remaining', str(limit.remaining))
        h.add('X-RateLimit-Limit', str(limit.limit))
        h.add('X-RateLimit-Reset', str(limit.reset))
    return response

# database engine
engine = sqlalchemy.create_engine(os.getenv('SQL_URI'))


@app.route('/')
def index():
    return 'Welcome to EQ Works 😎'


@app.route('/events/hourly')
@ratelimit(limit=100, per=60 * 1)
def events_hourly():
    return query_helper('''
        SELECT date, hour, events
        FROM public.hourly_events
        ORDER BY date, hour
        LIMIT 168;
    ''')


@app.route('/events/daily')
@ratelimit(limit=100, per=60 * 1)
def events_daily():
    return query_helper('''
        SELECT date, SUM(events) AS events
        FROM public.hourly_events
        GROUP BY date
        ORDER BY date
        LIMIT 7;
    ''')


@app.route('/stats/hourly')
@ratelimit(limit=100, per=60 * 1)
def stats_hourly():
    return query_helper('''
        SELECT date, hour, impressions, clicks, revenue
        FROM public.hourly_stats
        ORDER BY date, hour
        LIMIT 168;
    ''')


@app.route('/stats/daily')
@ratelimit(limit=100, per=60 * 1)
def stats_daily():
    return query_helper('''
        SELECT date,
            SUM(impressions) AS impressions,
            SUM(clicks) AS clicks,
            SUM(revenue) AS revenue
        FROM public.hourly_stats
        GROUP BY date
        ORDER BY date
        LIMIT 7;
    ''')

@app.route('/poi')
@ratelimit(limit=10, per=20 * 1)
def poi():
    return query_helper('''
        SELECT *
        FROM public.poi;
    ''')

@app.route('/stats/geo')
@ratelimit(limit=10, per=20 * 1)
def geostats():
    return geostats_helper('''
        SELECT  public.hourly_stats.date, 
                public.hourly_stats.hour, 
                public.hourly_stats.impressions, 
                public.hourly_stats.clicks, 
                public.hourly_stats.revenue,
                public.poi.lon,
                public.poi.lat
        FROM public.hourly_stats
        JOIN public.poi ON public.hourly_stats.poi_id = public.poi.poi_id
        ORDER BY date, hour;
    ''')

@app.route('/events/geo')
@ratelimit(limit=10, per=20 * 1)
def geoevents():
    return geoevents_helper('''
        SELECT  public.hourly_events.date, 
                public.hourly_events.hour, 
                public.hourly_events.events,
                public.poi.lon,
                public.poi.lat
        FROM public.hourly_events
        JOIN public.poi ON public.hourly_events.poi_id = public.poi.poi_id
        ORDER BY date, hour
        LIMIT 168;
    ''')

def query_helper(query):
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()
        return jsonify([dict(row.items()) for row in result])

def geostats_helper(query):
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()

        features = []
        for row in result:
            obj = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [row[-2], row[-1]]
                },
                "properties": {
                    "date": row[0],
                    "hour": row[1],
                    "impressions": row[2],
                    "clicks": row[3],
                    "revenue": row[4]
                }
            }

            features.append(obj)

        output = {
            "type": "FeatureCollection",
            "crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:OGC:1.3:CRS84" } },
            "features": features
        }

        return jsonify(output)

def geoevents_helper(query):
    with engine.connect() as conn:
        result = conn.execute(query).fetchall()

        features = []
        for row in result:
            obj = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [row[-2], row[-1]]
                },
                "properties": {
                    "date": row[0],
                    "hour": row[1],
                    "events": row[2]
                }
            }

            features.append(obj)

        output = {
            "type": "FeatureCollection",
            "crs": { "type": "name", "properties": { "name": "urn:ogc:def:crs:OGC:1.3:CRS84" } },
            "features": features
        }

        return jsonify(output)