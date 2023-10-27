### Download the json in this link (http://api.nobelprize.org/v1/laureate.json)
### Then extract this features from the json (id, name(first sur),dob=born,gender,unique_prize_years separated by; ,unique_prize_category separated by ; and country)
### the country has to be extracted from thhis json ("http://api.nobelprize.org/v1/country.json") using the code for the country where it is borned
from typing import List, Dict
import requests
import logging
import pandas as pd
from tqdm import tqdm


logger = logging.getLogger(__name__)


class ExtractJson:
    def __init__(self, _url):
        self.url = _url

    def scrape_json(self):
        request = requests.get(self.url)
        try:
            return request.json()
        except:
            logging.warning(
                f"Error in the request, with code error {request.status_code}"
            )


class TransformJson:
    def __init__(self, _json):
        self._json = _json

    def treate_json(self):
        dataframe_rows = []
        for index, info_list in self._json.items():
            for info_dictionary in tqdm(info_list):
                dataframe_rows.append(self.get_selected_info(info_dictionary))
        return self.create_dataframe(dataframe_rows)

    @staticmethod
    def get_selected_info(dict_info):
        row = {}
        row["id"] = TransformJson.get_id(dict_info)
        row["dob"] = TransformJson.get_dob(dict_info)
        row["gender"] = TransformJson.get_gender(dict_info)
        row["name"] = TransformJson.get_name(dict_info)
        (
            row["unique_prizes_years"],
            row["unique_prizes_category"],
        ) = TransformJson.get_unique_prize_years(dict_info)

        row["name"] = TransformJson.get_name(dict_info)
        row["country"] = TransformJson.get_country_by_code(dict_info)
        return row

    @staticmethod
    def get_id(info_dict: dict) -> str:
        if "id" in info_dict:
            return info_dict["id"]
        else:
            return None

    @staticmethod
    def get_dob(info_dict: dict) -> str:
        if "born" in info_dict:
            return info_dict["born"]
        else:
            return None

    @staticmethod
    def get_gender(info_dict: dict) -> str:
        if "gender" in info_dict:
            return info_dict["gender"]
        else:
            return None

    @staticmethod
    def get_name(info_dict: dict) -> str:
        name = ""
        if "firstname" in info_dict:
            name += info_dict["firstname"]
        if "surname" in info_dict:
            name += info_dict["surname"]
        if name != "":
            return name

    @staticmethod
    def get_unique_prize_years(info_dict: dict) -> str:
        if "prizes" in info_dict:
            prize_years = ""
            prize_category = ""
            for dicts in info_dict["prizes"]:
                x = info_dict["prizes"][-1]
                if "category" in dicts and dicts != x:
                    prize_category += f"{dicts['category']};"
                else:
                    prize_category += dicts["category"]

                if "year" in dicts and dicts != x:
                    prize_years += f"{dicts['year']};"
                else:
                    prize_years += dicts["year"]
            return prize_years, prize_category

    @staticmethod
    def get_country_by_code(dict_info: str) -> str | None:
        if "bornCountry" in dict_info:
            extract = ExtractJson("http://api.nobelprize.org/v1/country.json")
            country_json = extract.scrape_json()
            for dictionaries in country_json["countries"]:
                if (
                    "code" in dictionaries
                    and dictionaries["code"] == dict_info["bornCountryCode"]
                ):
                    return dictionaries["name"]

    @staticmethod
    def create_dataframe(list_dict: List[Dict[str, str]]) -> pd.DataFrame:
        return pd.DataFrame(list_dict)


class Loader:
    @staticmethod
    def create_csv_from_dataframe(dataframe: pd.DataFrame):
        print(dataframe[0:7])
        dataframe.to_csv("testing.csv", sep=",")


if __name__ == "__main__":
    extractor = ExtractJson("http://api.nobelprize.org/v1/laureate.json")
    extracted_json = extractor.scrape_json()
    transformer = TransformJson(extracted_json)
    dataframe = transformer.treate_json()
    Loader.create_csv_from_dataframe(dataframe)
