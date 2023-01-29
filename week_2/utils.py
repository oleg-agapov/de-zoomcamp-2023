import httpx
from io import BytesIO


# TODO: https://www.python-httpx.org/advanced/#monitoring-download-progress
def download_file(url: str):
	downloaded_file = BytesIO()
	with httpx.stream("GET", url) as r:
		for data in r.iter_bytes():
			downloaded_file.write(data)
	downloaded_file.seek(0)
	return downloaded_file
