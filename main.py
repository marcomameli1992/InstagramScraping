import sys
sys.path.append('./personalized-instagram-scraper/')
from igramscraper.instagram import Instagram
import argparse
import pandas as pd
import threading
from time import sleep
from igramscraper.exception import *


def instagram_login(username: str, password: str):
    """
    This is a function that permit the login
    :param username: the instagram username
    :param password: the instagram password
    :return: the instagram object with logged in information
    """
    instagram = Instagram()
    instagram.with_credentials(username, password, './cache/')
    instagram.login()
    return instagram


def download_image(hashtag, medias):
    """

    :param hashtag: is the hashtag
    :param medias: is the list of media associated to the hashtag
    :return:
    """
    import urllib
    import os
    os.makedirs(os.path.join('.', hashtag), exist_ok=True)  # create the folder for the hastag if not exist
    print('Start download')
    counter = 0
    for media in medias:
        # get the image in the post
        high_resolution_url = media.image_high_resolution_url
        low_resolution_url = media.image_low_resolution_url
        standard_resolution_url = media.image_standard_resolution_url
        identifier = media.identifier  # save the instagram identifier
        short_code = media.short_code  # save the instagram code
        if high_resolution_url is not None:
            open_url = urllib.request.urlopen(high_resolution_url)  # opening the url of the image
            with open(os.path.join('.', hashtag, 'hq-' + identifier + '-' + short_code + '.jpg'), 'wb') as output:
                # opening a binary object and save the image from the url content
                output.write(open_url.read())
        if low_resolution_url is not None:
            open_url = urllib.request.urlopen(low_resolution_url)
            with open(os.path.join('.', hashtag, 'lw-' + identifier + '-' + short_code + '.jpg'), 'wb') as output:
                output.write(open_url.read())
        if standard_resolution_url is not None:
            open_url = urllib.request.urlopen(standard_resolution_url)
            with open(os.path.join('.', hashtag, 'st-' + identifier + '-' + short_code + '.jpg'), 'wb') as output:
                output.write(open_url.read())
        counter += 1
    print(f'Downloaded {counter} images')


def get_posts_by_hashtag(instagram, hashtag, number_of_post=200):
    """
    This function use the list of hashtag to create a list of media and after start download in a parallel processes
    :param instagram: list of instances or a single instance of Instagram with logged in
    :param hashtag: list or single hashtag
    :return: tuple
    """
    total_number_of_post = 0
    thread: list = []
    if isinstance(instagram, list) and isinstance(hashtag, list):
        # There the arguments are lists and can be created different thread to get data
        hashtag_count = 0
        while (hashtag_count < len(hashtag)):
            for index, ig in enumerate(instagram):
                if index + hashtag_count >= len(hashtag):
                    break

                if hashtag_count != 0 and hashtag_count >= len(instagram):
                    print('Index: {}'.format(index + hashtag_count))
                    print(f'Hashtag count: {hashtag_count}')
                    keyword = hashtag[index + hashtag_count]
                else:
                    print(f'Index: {index}')
                    keyword = hashtag[index]
                print('Actual Hashtag: {}'.format(keyword['hashtag']))
                print('Account: {}'.format(ig.session_username))
                try:
                    # Check if the hashtag contains posts
                    # Check the latest data downloaded
                    if keyword['min_time'] == 0 and keyword['max_time'] == 0:
                        medias = ig.get_medias_by_tag(keyword['hashtag'], count=number_of_post)
                    elif keyword['max_time'] == 0:
                        medias = ig.get_medias_by_tag(keyword['hashtag'], count=number_of_post,
                                                      min_timestamp=keyword['min_time'])
                    elif keyword['min_time'] == 0:
                        medias = ig.get_medias_by_tag(keyword['hashtag'], count=number_of_post,
                                                      max_timestamp=keyword['max_time'])
                    else:
                        medias = ig.get_medias_by_tag(keyword['hashtag'], count=number_of_post,
                                                      max_timestamp=keyword['max_time'],
                                                      min_timestamp=keyword['min_time'])

                    if len(medias) == 0:
                        print('Hashtag not found')
                        hashtag_count += 1
                        continue

                    print('Get max_time and min_time for the hashtag')

                    # get the max_timestamp, in the first position of media and get the min_timestamp in the last position of medias
                    last_media = medias[0]
                    old_media = medias[-1]

                    if keyword['max_time'] < last_media.created_time:
                        keyword['max_time'] = last_media.created_time
                    if keyword['min_time'] > old_media.created_time or keyword['min_time'] == 0:
                        keyword['min_time'] = old_media.created_time

                    thread.append(threading.Thread(target=download_image, args=(keyword['hashtag'], medias)))
                    thread[-1].start()
                    total_number_of_post += len(medias)
                    keyword['count'] += len(medias)

                    print('Incremento hashtag_count')
                    hashtag_count += 1

                    sleep(500)  # After a Request of number_of_post sleep for 500 seconds to simulate human interaction
                except InstagramException:
                    # if the hashtag does not contain images or there is a problem with the connection an Instagram Exception is generated
                    print('Instagram Problem')
                    hashtag_count += 1
                    continue
                except:
                    return hashtag, thread, total_number_of_post

            if index + hashtag_count > len(hashtag):
                print('il prossimo index è oltre il numero di hashtag')
                break


    elif isinstance(instagram, Instagram) and isinstance(hashtag, list):
        for tag in hashtag:
            if tag['min_time'] == 0 and tag['max_time'] == 0:
                medias = instagram.get_medias_by_tag(tag['hashtag'], count=number_of_post)
            elif tag['max_time'] == 0:
                medias = instagram.get_medias_by_tag(tag['hashtag'], count=number_of_post,
                                                     min_timestamp=tag['min_time'])
            elif tag['min_time'] == 0:
                medias = instagram.get_medias_by_tag(tag['hashtag'], count=number_of_post,
                                                     max_timestamp=tag['max_time'])
            else:
                medias = instagram.get_medias_by_tag(tag['hashtag'], count=number_of_post,
                                                     max_timestamp=tag['max_time'],
                                                     min_timestamp=tag['min_time'])

            # get the max_timestamp, in the first position of media and get the min_timestamp in the last position of medias
            last_media = medias[0]
            old_media = medias[-1]

            if tag['max_time'] < last_media.created_time:
                tag['max_time'] = last_media.created_time
            if tag['min_time'] > old_media.created_time:
                tag['max_time'] = old_media.created_time

            thread.append(threading.Thread(target=download_image, args=(tag['hashtag'], medias)))
            thread[-1].start()
            total_number_of_post += len(medias)
            sleep(500)

    elif isinstance(instagram, Instagram) and isinstance(hashtag, str):
        medias = instagram.get_medias_by_tag(hashtag, count=number_of_post)
        thread.append(threading.Thread(target=download_image, args=(hashtag, medias)))
        thread[-1].start()
        total_number_of_post += len(medias)

    return hashtag, thread, total_number_of_post


# adding parsing
parser = argparse.ArgumentParser()
parser.add_argument('--account_list', help='the instagram accounts list')
parser.add_argument('--login_username', help='The username to be used for login')
parser.add_argument('--login_password', help='The username password')
parser.add_argument('--hashtag_list', help='the file that contains the list of hashtags')
parser.add_argument('--hashtag', help='The hashtag used to download data')
parser.add_argument('--total_number_of_post', help='The total number of posts to be download')
parser.add_argument('--session_number_of_post', help='The number of posts to get for each request')

args = parser.parse_args()

# Check input:
if (args.account_list is None) and ((args.login_username is None) or (args.login_password is None)):
    raise Exception('To work need accounts list or a single account')
if (args.hashtag_list is None) and (args.hashtag is None):
    raise Exception('To work need hashtags list or a single hashtag')

if __name__ == '__main__':

    if args.account_list is not None:
        try:
            ig_accounts = pd.read_csv(args.account_list, sep=',', names=['ACCOUNT', 'PW'], header=0)
            instagram: list = []
            for index, row in ig_accounts.iterrows():
                try:
                    instagram.append(instagram_login(row['ACCOUNT'], row['PW']))
                except InstagramAuthException:
                    print('Account {} has login problem proceed to next account'.format(row['ACCOUNT']))
                    print(InstagramAuthException)
                    continue
        except FileNotFoundError:
            raise FileNotFoundError('File not found')
        except:
            print('Error')
    else:
        instagram = instagram_login(str(args.login_username), str(args.login_password))

    if args.hashtag_list is not None:
        try:
            ig_hashtag = pd.read_csv(args.hashtag_list, sep=',', names=['hashtag', 'min_time', 'max_time', 'count'],
                                     header=0)
            hashtag: list = []
            for index, row in ig_hashtag.iterrows():
                hashtag.append(
                    {'hashtag': str(row['hashtag']), 'min_time': int(row['min_time']), 'max_time': int(row['max_time']),
                     'count': int(row['count'])})
        except FileNotFoundError:
            raise FileNotFoundError('File not found')
        except:
            print('Error')
    else:
        hashtag = str(args.hashtag)
    total_number_of_post = 0

    """
        Here a session is the single interation of the while that means is the number of downloaded posts for each hashtag
        after one request

        The end of the download is caused by the reached number of the total_number_of_post.
    """

    while (total_number_of_post < int(args.total_number_of_post)):
        update_hashtag, thread, session_number_of_post = get_posts_by_hashtag(instagram, hashtag,
                                                                              number_of_post=
                                                                              int(args.session_number_of_post))

        # update hashtag_file
        if args.hashtag_list is not None and isinstance(update_hashtag, list):
            ig_hashtag = pd.DataFrame.from_dict(update_hashtag, orient='columns')
            ig_hashtag.to_csv(str(args.hashtag_list), sep=',')
        elif isinstance(update_hashtag, list):
            ig_hashtag.to_csv('./hashtag_list.csv', sep=',')

        if len(thread) == 0:
            print('No Download Created')
            continue
        else:
            # wait for download completion
            print('Attendo completamento')
            for th in thread:
                th.join()

        total_number_of_post += session_number_of_post * int(len(hashtag))

        hashtag = update_hashtag  # update hashtag content to continue

        sleep(100)