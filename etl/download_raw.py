# imports
from kaggle.api.kaggle_api_extended import KaggleApi
import pathlib

# setup output path
output_path = pathlib.Path('../data')
output_path.mkdir(exist_ok=True, parents=True)

# import dataset through kaggle api
try:
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files('kyanyoga/sample-sales-data', path=output_path, unzip=True)
    print(f'☑ Data downloaded. Local path: "{output_path}"')
except Exception as e:
    print(f'☐ Data NOT downloaded. Error occured: {e}')
