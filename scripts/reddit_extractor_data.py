import praw
import pandas as pd
import time
import logging
import configparser
import json
from datetime import datetime

import boto3
import io
import os

import argparse

logging.basicConfig(filename='reddit.log', level=logging.INFO, format='%(asctime)s - %(message)s')

class RedditExtractor:
    def __init__(self, config_file, max_request_per_minute=60):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        self.reddit = praw.Reddit(
            client_id=self.config['reddit']['client_id'],
            client_secret=self.config['reddit']['client_secret'],
            user_agent=self.config['reddit']['user_agent']
        )

        self.request_interval = 60 / max_request_per_minute
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

        self.aws = boto3.client(
            's3',
            aws_access_key_id=self.config['aws']['aws_access_key_id'],
            aws_secret_access_key=self.config['aws']['aws_secret_access_key'],
            region_name=self.config['aws']['region_name']
        )
        self.s3_bucket = self.config['aws']['s3_bucket']
    
    def json_serializer(self, obj):

        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif hasattr(obj, 'name'):
            return obj.name  # Handle objects like Reddit author or subreddit
        elif hasattr(obj, '__dict__'):
            return str(obj)  # Fallback to string for unknown objects
        return obj
    
    def get_all_stored_post_ids(self):

        try:
            archive_key = "reddit-data-json/stored_post_ids.json"
            response = self.aws.list_objects_v2(Bucket=self.s3_bucket, Prefix=archive_key)
            if 'Contents' not in response:
                logging.info("No existing post ID archive found. Creating a new one.")
                return set()
            
            s3_object = self.aws.get_object(Bucket=self.s3_bucket, Key=archive_key)
            file_content = s3_object['Body'].read().decode('utf-8')
            stored_ids = json.loads(file_content)

            logging.info(f"Loaded {len(stored_ids)} stored post IDs from S3 archive.")
            return set(stored_ids)
        except Exception as e:
            logging.error(f"Error fetching stored post IDs: {str(e)}")
            return set()
    
    def update_stored_posts_ids(self, new_post_ids):

        try:
            archive_key = "reddit-data-json/stored_post_ids.json"
            existing_ids = self.get_all_stored_post_ids()

            updated_ids = existing_ids.union(set(new_post_ids))

            self.aws.put_object(
                Bucket= self.s3_bucket,
                Key = archive_key,
                Body= json.dumps(list(updated_ids), indent=4).encode('utf-8'),
                ContentType='application/json'
            )
            logging.info(f"Updated post ID archive with {len(new_post_ids)} new entries.")
    
        except Exception as e:
            logging.error(f"Error updating post ID archive: {str(e)}")

    def extract_posts(self, subreddit_name, post_limit=50):

        # print(subreddit_name)
        data = []
        data_json = []

        stored_post_ids = self.get_all_stored_post_ids()

        subreddit = self.reddit.subreddit(subreddit_name)
        logging.info(f"Extracing {post_limit} posts from subreddits: {subreddit_name}")

        try:
            for post in subreddit.hot(limit=post_limit):
                time.sleep(self.request_interval) # rate limit
                # data_json.append(vars(post))

                if post.id in stored_post_ids:
                    logging.info(f"Skipping duplicate post: {post.id}")
                    continue

                post_json = vars(post).copy()
                post_json['author'] = post.author.name if post.author else None
                post_json['subreddit'] = post.subreddit.display_name
                post_json = { k : self.json_serializer(v) for k, v in post_json.items() if not callable(v)}
                data_json.append(post_json)

                data.append(
                    {
                        "id" : post.id,
                        "title":post.title,
                        "selftext":post.selftext,
                        "score":post.score,
                        "num_comments":post.num_comments,
                        "url" : post.url,
                        "created_utc":datetime.fromtimestamp(post.created_utc)\
                            .strftime('%Y-%m-%d %H:%M:%S'),
                        "sub_reddit_name":post.subreddit.display_name,
                        "author":post.author.name if post.author else None
                    }
                )
        except Exception as e:
            logging.error(f"Error extracting posts from {subreddit_name}: {str(e)}")

        return data, data_json
    
    def extract_comments(self, post_id):

        data = []
        data_json = []
        try :
            submissions = self.reddit.submission(id=post_id)
            # print(submissions)
            # submissions.comments.replace_more(list=0) # Avoid "More Comments", loads everything
            for comment in submissions.comments.list():
                time.sleep(self.request_interval)

                comment_data = vars(comment).copy()

                # Serialize non-serializable attributes
                comment_data['author'] = comment.author.name if comment.author else None
                comment_data['post_id'] = post_id
                comment_data = {k: self.json_serializer(v) for k, v in comment_data.items() if not callable(v)}

                data_json.append(comment_data)

                data.append({
                    "id": comment.id,
                    "body": comment.body,
                    "score": comment.score,
                    "parent_id": comment.parent_id,
                    "created_utc": datetime.fromtimestamp(comment.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
                    "author": comment.author.name if comment.author else None,
                    "post_id": post_id
                })
        except Exception as e:
            logging.error(f" Error extracting comments from post {post_id}{ {str(e)}}")
        
        # print(data)
        return data, data_json
    
    
    def save_to_csv(self, data, filename, subreddit_name=None):

        try:
            df = pd.DataFrame(data)
            current_time = datetime.now()
            year = current_time.year
            month = current_time.month

            directory = f"/opt/airflow/data/"
            # os.makedirs(directory, exist_ok=True)

            filename = os.path.join(directory, filename)

            df.to_csv(filename, index=False)
            logging.info(f"Data saved to {filename}")
        except Exception as e:
            logging.error(f"Error saving data to {filename}: {str(e)}")
            raise
    
    def save_to_json(self, data, filename, subreddit_name):

        try:
            current_time = datetime.now()
            year = current_time.year
            month = current_time.month

            directory = f"/opt/airflow/data/"
            # os.makedirs(directory, exist_ok=True)

            filename = os.path.join(directory, filename)

            with open(filename, 'w+') as f:
                json.dump(data, f, indent=4)
            logging.info(f"Data saved to {filename}")
        except Exception as e:
            logging.error(f"Error saving data to {filename}: {str(e)}")
            raise
    
    def upload_csv_to_s3(self, data, filename, subreddit_name=None):

        try:
            df = pd.DataFrame(data)

            current_time = datetime.now()
            year = current_time.year
            month = current_time.month

            s3_key = f"reddit-data-csv/year={year}/month={month}/subreddit={subreddit_name}/{filename}"

            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)

            self.aws.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            logging.info(f"Data uploaded to S3: {s3_key}")
        except Exception as e:
            logging.error(f"Error uploading data to S3: {str(e)}")
            raise
    

    def upload_json_to_s3(self, data, filename, type,subreddit_name=None):

        try:
            s3_key = f"reddit-data-json/{filename}"

            current_time = datetime.now()
            year = current_time.year
            month = current_time.month
            day = current_time.day

            s3_key = f"reddit-data-json/{type}/year={year}/month={month}/day={day}/subreddit={subreddit_name}/{filename}"

            json_data = json.dumps(data, indent=4)
            self.aws.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json'
            )
            logging.info(f"Data uploaded to S3: {s3_key}")
        except Exception as e:
            logging.error(f"Error uploading data to S3: {str(e)}")
            raise
    
        
    def upload_parquet_to_s3(self, data, filename, type, subreddit_name=None):

        try:
            df = pd.DataFrame(data)

            current_time = datetime.now()
            year = current_time.year
            month = current_time.month

            s3_key = f"reddit-data-parquet/{type}/year={year}/month={month}/subreddit={subreddit_name}/{filename}"

            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False)

            self.aws.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=parquet_buffer.getvalue(),
                ContentType='application/x-parquet'
            )
            logging.info(f"Data uploaded to S3: {s3_key}")
        except Exception as e:
            logging.error(f"Error uploading data to S3: {str(e)}")
            raise

def main(args):
    
    CONFIG_FILE = args.config
    if args.subreddit:
        PRODUCT_REVIEW_SUBREDDITS = [args.subreddit]
    else:
        PRODUCT_REVIEW_SUBREDDITS = args.subreddits

    extractor = RedditExtractor(CONFIG_FILE)


    for sub_reddit_name in PRODUCT_REVIEW_SUBREDDITS:

        all_posts, all_posts_json = extractor.extract_posts(sub_reddit_name, post_limit=10)

        if all_posts_json:

            extractor.upload_json_to_s3(all_posts_json, f"reddit_product_reviews_posts_{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.json", "posts", sub_reddit_name)
            new_post_ids = [post["id"] for post in all_posts]
            extractor.update_stored_posts_ids(new_post_ids)
            for post in all_posts:
                comments, comments_json = extractor.extract_comments(post["id"])
                if comments_json:
                    extractor.upload_json_to_s3(comments_json, f"reddit_product_review_comments_{post['id']}_{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.json", "comments", sub_reddit_name)

    logging.info("Reddit extraction completed!")
    
if __name__ == "__main__":
    argparser = argparse.ArgumentParser("Reddit Data Extractor")
    argparser.add_argument("--config", help="Config file path", default='/opt/airflow/env/config.ini')
    argparser.add_argument("--max_req_per_min", help="Max requests per minute", default=60)
    argparser.add_argument("--subreddits", help="Subreddits to extract", nargs='+', default=["ProductReviews", "amazonreviews", "apple", "android", "headphones", "buildapc", "gadgets"])
    argparser.add_argument("--subreddit", help="Single subreddit to extract", default=None)
    args = argparser.parse_args()
    main(args)