import utils
import pandas as pd
import numpy as np
import argparse
import datetime
import os
import calendar

'''
Sales analysis master script
'''

def generate_data():
        #Generate data by calling the API
        df_clt = utils.ClientsData().get_clients().rename({'id':'client_id'},axis='columns') #renaming the id to client_id so can be left-joined
        df_sales = utils.SalesData().get_sales()

        #Left join on client_id
        df = df_sales.merge(df_clt, how="left", on = "client_id")

        #Clean the shipping product
        df = df[
                ~df["product"].isin(
                        ["Dakota Cargo", "Shipping JNE", "GoSend", "Shipping", "Go-Box", "Pura-Pura"]
                )
        ]

        #Save the clean data 
        if not os.path.exists("./data"):
                os.makedirs("./data")

        df.to_csv("./data/clean_data.csv", index=False)

        return df

def clients_tracker(df_clt, df_sales):
        pass

def monthly_sales_reporting(df, start_date, end_date):
        '''
        Args:
                start_date = upper limit date
                end_date = bottom limit date

        Slice the SalesData based on the user choice of date. Analyse the monthly sales report
        '''

        #Choose the sales data that are in between the specified date boundaries
        df = df[
                (df['invoice_date'] >= start_date) & 
                (df['invoice_date'] <= end_date)
        ].reset_index()
        
        '''
        Start the analysis
        '''
        #1. Loop over the name of the product and create data frame where X_axis is the product name and Y_axis is month
        product_unique = df["product"].unique().tolist()
        month_start = start_date.month
        month_end = end_date.month
        
        #Array to hold the value 1st column is month, 2-end product names
        V = np.zeros(
                (month_end, len(product_unique)+1)
        )
        print(V.shape)
 
        #Iterate over the product name
        for j,p in enumerate(product_unique):
                #Slash by name p
                df_tmp = df[df['product']== p]

                for i,M in enumerate(
                        np.arange(month_start, month_end+1,1)
                ):
                        #Get the current year and last day of month M
                        year_now = datetime.datetime.now().year
                        last_day = calendar.monthrange(year_now, M)[1]

                        #Form the datetime object of month M 1st and last day
                        ub_date = datetime.datetime.strptime("%s-%s-%s"%(last_day,M,year_now), "%d-%m-%Y")
                        lb_date = datetime.datetime.strptime("%s-%s-%s"%(1,M,year_now), "%d-%m-%Y")

                        #Slash by month
                        df_tmp2 = df_tmp[
                                (df_tmp.invoice_date >= lb_date) & 
                                (df_tmp.invoice_date <= ub_date)
                        ]

                        if len(df_tmp2) > 0:
                                v = df_tmp2.qty.sum()
                        else:
                                v = 0
                        
                        #1st column is months
                        V[i,0] = int(M)
                        V[i,j+1] = v

        product_unique.insert(0,"Month")

        assert(
                V.shape[1] == len(product_unique)
        )

        #Create a dataframe to store V 
        df_historical_sales = pd.DataFrame(
                V, columns = product_unique
        )
        
        #Save the data
        df_historical_sales.to_csv(
                "./data/historical_sales_data.csv",index=False
        )

def client_analyses(df):
        with open("watchlist.txt","w") as f:
                f.write("Watchlist---\n")
        
        now = datetime.datetime.now()

        df_ = df[
                ["display_name","product","invoice_date","qty"]
        ]

        df_ = df_.groupby(["display_name"],group_keys=False).apply(lambda x:x.sort_values(["invoice_date"],ignore_index = True))
        df_.to_csv("test.csv")

        #Start to analyse per client
        for n in df_.display_name.unique():
                #Cut the dataframe and analyse clients one by one
                df_c = df_[df_.display_name == n]
                buying_dates = df_c.invoice_date
                delta_days = []
                
                #X-out clients that are one-time hit
                if not len(buying_dates.unique()) == 1:
                        for i in range(0,len(buying_dates)-1):
                                d1 = buying_dates.iloc[i+1]
                                d2 = buying_dates.iloc[i]
                                delta_days.append(
                                        (d1-d2).days
                                )
                        #If the period between the last order and now is bigger than the average order period
                        last_day = (now - buying_dates.iloc[-1]).days
                        avg_buying_time = np.array(delta_days).mean()
                        
                        if last_day > avg_buying_time:
                                with open("watchlist.txt","a") as f:
                                        f.write("Client: %s\n"%(n))
                                        f.write("Average buying time: %s days\n"%(avg_buying_time))
                                        f.write(
                                                "Days since last PO: %s days, Last PO: %s-%s-%s\n"%(
                                                        last_day, 
                                                        buying_dates.iloc[-1].day, 
                                                        buying_dates.iloc[-1].month,
                                                        buying_dates.iloc[-1].year
                                                )
                                        )
                                        f.write("Items: ")
                                        for idx in range(df_c.shape[0]):
                                                product = df_c["product"].iloc[idx]
                                                qty = df_c["qty"].iloc[idx]
                                                f.write(
                                                        "%s -- %s kgs, "%(product,qty)
                                                )
                                        f.write("\n\n\n")       

if __name__ == "__main__":
        parser = argparse.ArgumentParser()
        parser.add_argument(
                "start_date", 
                type=str,
                help="Parse the upper boundary of date before which equal to the data will be analysed. Format: DD-MM-YYY. E.g. 31-03-2021"
        )

        parser.add_argument(
                "end_date", 
                type=str,
                help="Parse the lower boundary of date beyond which equal to the data will be analysed. Format: DD-MM-YYYY. E.g. 01-03-2021"
        )

        args = parser.parse_args()
        start_date = args.start_date.split("=")[-1]
        end_date = args.end_date.split("=")[-1]

        #Convert string to datetime object
        start_date = datetime.datetime.strptime(start_date, "%d-%m-%Y")
        end_date = datetime.datetime.strptime(end_date, "%d-%m-%Y")
        
        #Final dataframe to be analysed
        df = generate_data()

        #Start analysis
        monthly_sales_reporting(df, start_date, end_date)

        client_analyses(df)