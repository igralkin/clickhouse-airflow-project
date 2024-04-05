import requests
import socket
import os


class FileDownloader:
    def __init__(self, source_url, destination_path):
        self.source_url = source_url
        self.destination_path = destination_path

    @staticmethod
    def is_connected():
        try:
            socket.create_connection(("www.google.com", 80))
            return True
        except OSError:
            return False

    def download(self):
        if not self.is_connected():
            print('Failed: No internet connection or google.com is not responding')
            return False

        try:
            response = requests.get(self.source_url)
            response.raise_for_status()
            with open(self.destination_path, "wb") as file:
                file.write(response.content)
            if self.verify_download():
                print(f'Success: File downloaded from {self.source_url} to {self.destination_path}')
                return True
            else:
                print(f'Failed: File from {self.source_url} was not downloaded correctly')
                return False
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError,
                requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
            print(f'Error downloading from {self.source_url}: {e}')
            return False

    def verify_download(self):
        return os.path.isfile(self.destination_path) and os.path.getsize(self.destination_path) > 0
