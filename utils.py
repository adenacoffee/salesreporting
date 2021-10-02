import requests
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from requests.models import HTTPBasicAuth
from keys import api_key as token
import json
import os
import datetime

class Data:
    '''
    This class is to host raw data from invoice ninja API. 
    Invoice Ninja utilises RESTful API s.t. the credentials (API token) is passed as a header
    Therefore, when using get() method from requests module, one should NOT pass the TOKEN in the URL, instead using "headers" args from the method
    '''
    def __init__(self):
        self.token = token 

        #initialise headers, populate it with token API
        self.headers = {"X-Ninja-Token": "%s"%(self.token)}

    def assert_return(self):
        try:
            assert(self.response.status_code < 400)
            print("API is called successfully with status code %s"%(self.response.status_code))
        except:
            raise ValueError("Invoice Ninja return status code is %s"%(self.response.status_code))

    def get_num_data(self, module="invoices"):
        '''
        Initial call to determine how many data there are in invoice ninja
        '''
        if module not in ["invoices","clients"]:
            raise ValueError("Module %s not available. Please choose: invoices or clients"%module)

        tmp_url ='https://app.invoiceninja.com/api/v1/%s'%(module)
        tmp_response = requests.get(tmp_url,headers=self.headers)
        return tmp_response.json()["meta"]["pagination"]["total"]

class SalesData(Data):
    def __init__(self):
        super().__init__()
        
    def get_sales(self):
        '''
        This is the main method to generate the invoice, expected to be returned as a CSV
        Since the resuls is paginated, specify result per page as a huge number such that it only calls API one time
        '''

        print("******************************************** Generating all sales data **************************************************")
        #Call the API
        result_per_page = self.get_num_data(module="invoices")

        print("Total number of sales in the database: %s"%(result_per_page))

        self.url = 'https://app.invoiceninja.com/api/v1/invoices?per_page=%s'%(result_per_page)
        self.response = requests.get(self.url, headers=self.headers)           

        #Assert status code
        self.assert_return()

        #Assert that the length of data == result_per_page
        assert(
            len(self.response.json()["data"]) == result_per_page
        )

        #Read as csv
        l = []
        for dct in self.response.json()['data']:
            #Iterate through the list of dictionaries
            l.append(
                pd.DataFrame(dct)
            )

        if not os.path.exists("./data"):
            os.makedirs("data")

        self.df = pd.concat(l)
        print("Successfully fetched all invoice raw data, save it as %s/data/all_inv.csv. Proceed to cleaning............"%(os.getcwd()))

        #Data cleaning part
        self.data_cleaning()
        self.df.to_csv("./data/all_inv.csv",index=False)

        return self.df

    def data_cleaning(self):
        #Create products and quantity in columns
        products = []
        qty = []
        for v in self.df.invoice_items:
            products.append(v["product_key"])
            qty.append(v["qty"])
        
        self.df["product"] = products
        self.df["qty"] = qty

        self.df = self.df.drop(columns=["invoice_items"])

        #Convert invoice date to datetime object
        self.df["invoice_date"] = pd.to_datetime(self.df['invoice_date'], format="%Y-%m-%d")

        print("Cleaning finished................")

        return self.df

class ClientsData(Data):
    def __init__(self):
        super().__init__()

    def get_clients(self):
        '''
        This is the main method to generate clients database
        '''
        print("******************************************** Generating all clients data **************************************************")
        #Call the API
        result_per_page = self.get_num_data(module="clients")

        print("Total number of clients in the database: %s"%(result_per_page))


        self.url = 'https://app.invoiceninja.com/api/v1/clients?per_page=%s'%(result_per_page)
        self.response = requests.get(self.url, headers=self.headers)   

        self.assert_return()    

        #Assert that the length of data == result_per_page
        assert(
            len(self.response.json()["data"]) == result_per_page
        )

        #Read as csv
        l = []
        for dct in self.response.json()['data']:
            #Iterate through the list of dictionaries
            l.append(
                pd.DataFrame(dct)
            )

        if not os.path.exists("./data"):
            os.makedirs("data")

        self.df = pd.concat(l)
        self.df.to_csv("./data/all_clients.csv",index=False)

        print("Successfully fetched all clients raw data, save it as %s/data/all_clt.csv. Proceed to cleaning............"%(os.getcwd()))

        #No cleaning necessary for clients data

        print("Cleaning finished................")       

        return self.df
    

if __name__ == "__main__":
    
    data_sales = SalesData()
    df_inv = data_sales.get_sales()
    print(df_inv["invoice_date"])
    

    data_client = ClientsData()
    df_clt = data_client.get_clients()
    print(
        df_clt[['id','name']]
    )