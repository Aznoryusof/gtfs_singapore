import requests
import json
import traceback
import sys
import time


class DataMallEndpoint:
    def __init__(self, account_key, resource, value_key, force_break_threshold):
        self.data_all = []
        self.account_key = account_key
        self.value_key = value_key
        self.scraped_object = None
        self.domain = "http://datamall2.mytransport.sg"
        self.subdir = "ltaodataservice"
        self.resource = resource
        self.resource_url = f"{self.domain}/{self.subdir}/{resource}"
        self.current_skip_value = 0
        self.next_skip_value = 0
        self.skip_value_interval = 500
        self.force_break_threshold = force_break_threshold


    def _get_data(self, is_has_next=False):
        headers = { 'AccountKey': self.account_key, 'accept' : 'application/json'}

        if is_has_next:
            full_url = self.resource_url + f"?$skip={self.next_skip_value}"
        else:
            full_url = self.resource_url + f"?$skip={self.current_skip_value}"
            print(f"Getting data from API endpoint: {full_url}")
        
        try:
            res = requests.get(
                full_url,
                headers=headers
            )
        except Exception:
            print(f"Can't get from: {full_url}")
            traceback.print_exc()
        if res.status_code == 200:
            res_dict = json.loads(res.text)
        else:
            print(f"Can't get from: {full_url}")
            print(f"Status code: {res.status_code}")
            sys.exit()
       
        return res_dict.get("odata.metadata"), res_dict.get("value") 


    def _pre_run(self):
        metadata, data_list = self._get_data()
        
        return metadata, data_list


    @property
    def _has_next(self):
        self.next_skip_value = self.current_skip_value + self.skip_value_interval
        _, data_list = self._get_data(is_has_next=True)
        if len(data_list) > 0:
            self.current_skip_value = self.next_skip_value
            return True
        else:
            return False


    def scrape(self):
        # Do first run of the data query
        metadata, data_list_pre_run = self._pre_run()
        self.data_all.extend(data_list_pre_run)
                
        count = 1
        while self._has_next:
            if (count % 50) == 0:
                time.sleep(5)

            _, data_list = self._get_data()
            
            # Append data from current query
            self.data_all.extend(data_list)

            # Prevent infinite while loop
            count+=1
            if count > self.force_break_threshold:
                exit_msg = f"Reached infinite while loop when querying API with threshold set at {self.force_break_threshold}. Exit process..."
                sys.exit(exit_msg)

        entities_returned_no = len(set([x.get(self.value_key) for x in self.data_all]))
        print(f"Collected {entities_returned_no} {self.resource}")

        return metadata, self.data_all


if __name__ == "__main__":
    bus_stops_scraper = DataMallEndpoint(
        account_key='u5H3yfW+QkG06YbuaHSLIQ==', resource="BusStops", 
        value_key="BusStopCode", force_break_threshold = 20
    )
    metadata, data_all = bus_stops_scraper.scrape()

    print("pause")