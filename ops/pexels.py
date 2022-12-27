import os
import string
import random
import requests

from typing import List

from pypexels import PyPexels

from dagster import op


PEXELS_API_KEY = 'YOUR_PEXELS_API_KEY'


class PexelsClient:
    """Pexels client to download a random popular video. """

    def __init__(
        self,
        pexels_api_key = PEXELS_API_KEY
    ):
        self.pexels_api_key = pexels_api_key
        self.api = PyPexels(api_key=self.pexels_api_key)


    def random_filename(self, size_: int = 8):
        """Generates a random filename of uppercase letters and digits. """
        chars = string.ascii_uppercase + string.digits
        return ''.join(random.choice(chars) for _ in range(size_)) + '.mp4'


    def download_video(self, root: str):
        """Downloads a random popular video from pexels and saves it. """
        
        popular_videos = self.api.videos_popular(per_page=40)._body['videos']
        video = random.choice(popular_videos)
        video_file = video['video_files'][0]
        video_link = video_file['link']

        video = requests.get(video_link)

        path = os.path.join(root, self.random_filename())
        with open(path, 'wb') as outfile:
            outfile.write(video._content)

        return path


@op
def download_random_video_from_pexels() -> str:
    """Dagster op to download a random pexels video to the current directory.

    Returns:
        The path to the downloaded video.

    """

    client = PexelsClient(pexels_api_key="563492ad6f91700001000001f91f6288a95e4ef0baf9ea78d37d4032")
    path = client.download_video('./')

    return path