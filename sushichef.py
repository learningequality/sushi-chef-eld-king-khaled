#!/usr/bin/env python

from bs4 import BeautifulSoup
import codecs
from collections import defaultdict, OrderedDict
import copy
import glob
from le_utils.constants import licenses, content_kinds, file_formats
import hashlib
import json
import logging
import ntpath
import os
from pathlib import Path
import re
import requests
from ricecooker.classes.licenses import get_license
from ricecooker.chefs import JsonTreeChef
from ricecooker.utils import downloader, html_writer
from ricecooker.utils.caching import CacheForeverHeuristic, FileCache, CacheControlAdapter
from ricecooker.utils.jsontrees import write_tree_to_json_tree, SUBTITLES_FILE
import time
from urllib.error import URLError
from urllib.parse import urljoin
from utils import if_dir_exists, get_name_from_url, clone_repo, build_path
from utils import if_file_exists, get_video_resolution_format, remove_links
from utils import get_name_from_url_no_ext, get_node_from_channel, get_level_map
from utils import remove_iframes, get_confirm_token, save_response_content
import youtube_dl


BASE_URL = "https://www.youtube.com/user/kkudl/playlists"

DATA_DIR = "chefdata"
COPYRIGHT_HOLDER = "King Khaled University in Abha, Saudi Arabia"
LICENSE = get_license(licenses.CC_BY, 
        copyright_holder=COPYRIGHT_HOLDER).as_dict()
AUTHOR = "King Khaled University in Abha, Saudi Arabia"

LOGGER = logging.getLogger()
__logging_handler = logging.StreamHandler()
LOGGER.addHandler(__logging_handler)
LOGGER.setLevel(logging.INFO)

DOWNLOAD_VIDEOS = True

sess = requests.Session()
cache = FileCache('.webcache')
basic_adapter = CacheControlAdapter(cache=cache)
forever_adapter = CacheControlAdapter(heuristic=CacheForeverHeuristic(), cache=cache)
sess.mount('http://', basic_adapter)
sess.mount(BASE_URL, forever_adapter)

# Run constants
################################################################################
CHANNEL_NAME = "ELD King Khaled University Learning (العربيّة)"              # Name of channel
CHANNEL_SOURCE_ID = "sushi-chef-eld-k12-ar"    # Channel's unique id
CHANNEL_DOMAIN = "https://www.youtube.com/user/kkudl/playlists"          # Who is providing the content
CHANNEL_LANGUAGE = "ar"      # Language of channel
CHANNEL_DESCRIPTION = None                                  # Description of the channel (optional)
CHANNEL_THUMBNAIL = None                                    # Local path or url to image file (optional)

# Additional constants
################################################################################

def title_patterns(title):
    pattern01 = r"\d+\-\d+"
    match = re.search(pattern01, title)
    if match:
        index = match.span()
        #new_title = title[index[1]+1:]
        numbers = title[index[0]:index[1]]
        number_unit = numbers.split("-")[0]
        return "Unit {}".format(number_unit)
    else:
        return title


class Node(object):
    def __init__(self, title, source_id, lang="en"):
        self.title = title
        self.source_id = source_id
        self.tree_nodes = OrderedDict()
        self.lang = lang
        self.description = None

    def add_node(self, obj):
        node = obj.to_node()
        if node is not None:
            self.tree_nodes[node["source_id"]] = node

    def to_node(self):
        return dict(
            kind=content_kinds.TOPIC,
            source_id=self.title,
            title=self.title,
            description=self.description,
            language=self.lang,
            author=AUTHOR,
            license=LICENSE,
            children=list(self.tree_nodes.values())
        )
    

class Subject(Node):
    def __init__(self, *args, **kwargs):
        super(Subject, self).__init__(*args, **kwargs)
        self.topics = []

    def load(self, filename):
        with open(filename, "r") as f:
            topics = json.load(f)
            for topic in topics:
                topic_obj = Topic(topic["title"], topic["source_id"])
                for unit in topic["units"]:
                    units = Topic.auto_generate_units(unit["source_id"], title=unit["title"])
                    topic_obj.units.extend(units)
                self.topics.append(topic_obj)
                break


class Topic(Node):
    def __init__(self, *args, **kwargs):
        super(Topic, self).__init__(*args, **kwargs)
        self.units = []

    @staticmethod
    def auto_generate_units(url, title=None):
        youtube = YouTubeResource(url)
        units = defaultdict(list)
        if title is not None:
            for url in youtube.playlist_links():
                units[title].append(url)
        else:
            for name, url in youtube.playlist_name_links():
                unit_name_list = name.split("|")
                if len(unit_name_list) > 1:
                    unit = unit_name_list[1]
                    unit_name = unit.strip().split(" ")[0]
                else:
                    unit_name = title_patterns(name)
                units[unit_name].append(url)

        for title, urls in units.items():
            unit = Unit(title, title)
            unit.urls = urls
            yield unit


class Unit(Node):
    def __init__(self, *args, **kwargs):
        super(Unit, self).__init__(*args, **kwargs)
        self.urls = []

    def download(self, download=True, base_path=None):
        for url in self.urls:
            youtube = YouTubeResource(url, lang=self.lang)
            youtube.download(download, base_path)
            self.add_node(youtube)


class YouTubeResource(object):
    def __init__(self, source_id, name=None, type_name="Youtube", lang="ar", 
            embeded=False, section_title=None):
        LOGGER.info("    + Resource Type: {}".format(type_name))
        LOGGER.info("    - URL: {}".format(source_id))
        self.filename = None
        self.type_name = type_name
        self.filepath = None
        self.name = name
        self.section_title = section_title
        if embeded is True:
            self.source_id = YouTubeResource.transform_embed(source_id)
        else:
            self.source_id = self.clean_url(source_id)
        self.file_format = file_formats.MP4
        self.lang = lang
        self.is_valid = False

    def clean_url(self, url):
        if url[-1] == "/":
            url = url[:-1]
        return url.strip()

    @classmethod
    def is_youtube(self, url, get_channel=False):
        youtube = url.find("youtube") != -1 or url.find("youtu.be") != -1
        if get_channel is False:
            youtube = youtube and url.find("user") == -1 and url.find("/c/") == -1
        return youtube

    @classmethod
    def transform_embed(self, url):
        url = "".join(url.split("?")[:1])
        return url.replace("embed/", "watch?v=").strip()

    def playlist_links(self):
        ydl_options = {
                'no_warnings': True,
                'restrictfilenames':True,
                'continuedl': True,
                'quiet': False,
                'format': "bestvideo[height<={maxheight}][ext=mp4]+bestaudio[ext=m4a]/best[height<={maxheight}][ext=mp4]".format(maxheight='480'),
                'noplaylist': False
            }

        playlist_videos_url = []
        with youtube_dl.YoutubeDL(ydl_options) as ydl:
            try:
                ydl.add_default_info_extractors()
                info = ydl.extract_info(self.source_id, download=False)
                for entry in info["entries"]:
                    playlist_videos_url.append(entry["webpage_url"])
            except(youtube_dl.utils.DownloadError, youtube_dl.utils.ContentTooShortError,
                    youtube_dl.utils.ExtractorError) as e:
                LOGGER.info('An error occured ' + str(e))
                LOGGER.info(self.source_id)
            except KeyError as e:
                LOGGER.info(str(e))
        return playlist_videos_url

    def playlist_name_links(self):
        name_url = []
        for url in self.playlist_links():
            youtube = YouTubeResource(url)
            info = youtube.get_video_info(None, False)
            name_url.append((info["title"], url))
        return name_url

    def get_video_info(self, download_to=None, subtitles=True):
        ydl_options = {
                'writesubtitles': subtitles,
                'allsubtitles': subtitles,
                'no_warnings': True,
                'restrictfilenames':True,
                'continuedl': True,
                'quiet': False,
                'format': "bestvideo[height<={maxheight}][ext=mp4]+bestaudio[ext=m4a]/best[height<={maxheight}][ext=mp4]".format(maxheight='480'),
                'outtmpl': '{}/%(id)s'.format(download_to),
                'noplaylist': True
            }

        with youtube_dl.YoutubeDL(ydl_options) as ydl:
            try:
                ydl.add_default_info_extractors()
                info = ydl.extract_info(self.source_id, download=(download_to is not None))
                return info
            except(youtube_dl.utils.DownloadError, youtube_dl.utils.ContentTooShortError,
                    youtube_dl.utils.ExtractorError) as e:
                LOGGER.info('An error occured ' + str(e))
                LOGGER.info(self.source_id)
            except KeyError as e:
                LOGGER.info(str(e))

    def subtitles_dict(self):
        subs = []
        video_info = self.get_video_info()
        if video_info is not None:
            video_id = video_info["id"]
            if 'subtitles' in video_info:
                subtitles_info = video_info["subtitles"]
                for language in subtitles_info.keys():
                    subs.append(dict(file_type=SUBTITLES_FILE, youtube_id=video_id, language=language))
        return subs

    #youtubedl has some troubles downloading videos in youtube,
    #sometimes raises connection error
    #for that I choose pafy for downloading
    def download(self, download=True, base_path=None):
        if not "watch?" in self.source_id or "/user/" in self.source_id or\
            download is False:
            return

        download_to = build_path([base_path, 'videos'])
        for i in range(4):
            try:
                info = self.get_video_info(download_to=download_to, subtitles=False)
                if info is not None:
                    LOGGER.info("    + Video resolution: {}x{}".format(info.get("width", ""), info.get("height", "")))
                    self.filepath = os.path.join(download_to, "{}.mp4".format(info["id"]))
                    self.filename = info["title"]
                    if self.filepath is not None and os.stat(self.filepath).st_size == 0:
                        LOGGER.info("    + Empty file")
                        self.filepath = None
            except (ValueError, IOError, OSError, URLError, ConnectionResetError) as e:
                LOGGER.info(e)
                LOGGER.info("Download retry")
                time.sleep(.8)
            except (youtube_dl.utils.DownloadError, youtube_dl.utils.ContentTooShortError,
                    youtube_dl.utils.ExtractorError, OSError) as e:
                LOGGER.info("     + An error ocurred, may be the video is not available.")
                return
            except OSError:
                return
            else:
                return

    def to_node(self):
        if self.filepath is not None:
            files = [dict(file_type=content_kinds.VIDEO, path=self.filepath)]
            files += self.subtitles_dict()
            node = dict(
                kind=content_kinds.VIDEO,
                source_id=self.source_id,
                title=self.name if self.name is not None else self.filename,
                description='',
                author=AUTHOR,
                files=files,
                language=self.lang,
                license=LICENSE
            )
            return node


def download(source_id):
    tries = 0
    while tries < 4:
        try:
            document = downloader.read(source_id, loadjs=False, session=sess)
        except requests.exceptions.HTTPError as e:
            LOGGER.info("Error: {}".format(e))
        except requests.exceptions.ConnectionError:
            ### this is a weird error, may be it's raised when the webpage
            ### is slow to respond requested resources
            LOGGER.info("Connection error, the resource will be scraped in 5s...")
            time.sleep(3)
        except requests.exceptions.TooManyRedirects as e:
            LOGGER.info("Error: {}".format(e))
        else:
            return document
        tries += 1
    return False


# The chef subclass
################################################################################
class KingKhaledChef(JsonTreeChef):
    HOSTNAME = BASE_URL
    TREES_DATA_DIR = os.path.join(DATA_DIR, 'trees')
    SCRAPING_STAGE_OUTPUT_TPL = 'ricecooker_json_tree.json'
    THUMBNAIL = ""

    def __init__(self):
        build_path([KingKhaledChef.TREES_DATA_DIR])
        self.scrape_stage = os.path.join(KingKhaledChef.TREES_DATA_DIR, 
                                KingKhaledChef.SCRAPING_STAGE_OUTPUT_TPL)
        super(KingKhaledChef, self).__init__()

    def pre_run(self, args, options):
        self.write_tree_to_json(self.scrape(args, options))

    def scrape(self, args, options):
        LANG = 'ar'
        download_video = options.get('--download-video', "1")

        if int(download_video) == 0:
            global DOWNLOAD_VIDEOS
            DOWNLOAD_VIDEOS = False

        global channel_tree
        channel_tree = dict(
                source_domain=KingKhaledChef.HOSTNAME,
                source_id=BASE_URL,
                title=CHANNEL_NAME,
                description="""This channel contains some interactive courses for secondary education learners on the areas of English and Arabic language skills, basic math skills and Islamic studies as well. Videos are produced by a variety of faculty members at King Khaled University."""
[:400], #400 UPPER LIMIT characters allowed 
                thumbnail="https://yt3.ggpht.com/a-/AN66SAz9fwCzHEBXcCczoBEGfXr7xKzhooqj0yqVwQ=s288-mo-c-c0xffffffff-rj-k-no",
                author=AUTHOR,
                language=LANG,
                children=[],
                license=LICENSE,
            )

        base_path = [DATA_DIR] + ["King Khaled University in Abha"]
        base_path = build_path(base_path)

        #subject_01 = Subject(title="English Language Skills اللغة الإنجليزية", 
        #                    source_id="English Language Skills اللغة الإنجليزية")
        #subject_01.load("resources_en_lang_skills.json")
        subject_01 = Subject(title="Arabic Language Skills اللغة العربية", 
                            source_id="Arabic Language Skills اللغة الإنجليزية")
        subject_01.load("resources_ar_lang_skills.json")

        #subject_01 = Subject(title="Islamic Studies الثقافة الإسلامية", 
        #                    source_id="Islamic Studies الثقافة الإسلامية")
        #subject_01.load("resources_ar_islamic_studies.json")

        #subject_01 = Subject(title="Math الرياضيات", 
        #                    source_id="Math الرياضيات")
        #subject_01.load("resources_ar_math.json")

        for subject in [subject_01]:
            for topic in subject.topics:
                for unit in topic.units:
                    unit.download(download=DOWNLOAD_VIDEOS, base_path=base_path)
                    topic.add_node(unit)
                subject.add_node(topic)
            channel_tree["children"].append(subject.to_node())
        
        return channel_tree

    def write_tree_to_json(self, channel_tree):
        write_tree_to_json_tree(self.scrape_stage, channel_tree)


# CLI
################################################################################
if __name__ == '__main__':
    chef = KingKhaledChef()
    chef.main()
