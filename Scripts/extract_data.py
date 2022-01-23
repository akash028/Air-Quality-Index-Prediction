import os
import sys

import requests
from bs4 import BeautifulSoup
import csv
from csv import DictReader

class extract_data:
    def get_html_data(self):
        url = ''
        for year in range(2001, 2022):
            for month in range(1, 13):
                if month < 10:
                    url = 'https://en.tutiempo.net/climate/0{}-{}/ws-702610.html'.format(month, year)
                else:
                    url = 'https://en.tutiempo.net/climate/{}-{}/ws-702610.html'.format(month, year)
                try:
                    htmls = requests.get(url, verify=False)
                    html_utf = htmls.text.encode('utf=8')
                    if not os.path.exists('Data/Html_data/{}'.format(year)):
                        os.makedirs('Data/Html_data/{}'.format(year))
                    with open("Data/Html_data/{}/{}".format(year, month), "wb") as output:
                        output.write(html_utf)
                except:
                    print("No data available for this month")

            sys.stdout.flush()

    def combine_data(self):
        #url = ''
        headers = ['Date', 'Average Temperature', 'Maximum temperature', 'Minimum temperature', 'Atmospheric pressure at sea level', 'Average relative humidity', 'Total rainfall and / or snowmelt', \
                   'Average visibility', 'Average wind speed', 'Maximum sustained wind speed', 'Maximum speed of wind', 'Rain',\
                   'Snow', 'Thunderstorm', 'Fog', 'PM2.5 AQI Value', 'AQI Category']
        table_data = []
        for year in range(2001, 2022):
            with open("Data/pm_data/aqidaily{}.csv".format(year), 'r') as read_obj:
                csv_dict_reader = DictReader(read_obj)
                lst = []
                for row in csv_dict_reader:
                    lst.append(row)
                # print(lst[0]['Date'] + ":" + lst[1]['Date'])
                k = 0
                for month in range(1, 13):
                    try:
                        file_url = open("Data/Html_data/{}/{}".format(year, month), 'rb')




                        text = file_url.read()

                        soup = BeautifulSoup(text, 'html.parser')
                        # print(soup.prettify())

                        try:
                            _div = soup.find("div", attrs={"class": "mt5 minoverflow tablancpy"})
                            _table = _div.find("table", attrs={"class":"medias mensuales numspan"}).findAll("tr")
                                # _table.tbody
                            # print(_table[:10])
                            # break
                            t_headers = _table[0]
                            t_data = _table[1:]
                            for tr in t_data:
                                t_row = {}
                                flag = False
                                for i, td in enumerate(tr.findAll("td")):
                                    if i == 0:
                                        d = td.text.replace('\n', '').strip()
                                        str_date = str(d).zfill(2) + '/' + str(month).zfill(2) + '/' + str(year).zfill(4)
                                        str_date2 = str(month).zfill(2) + '/' + str(d).zfill(2) + '/' + str(year).zfill(4)
                                        t_row[headers[i]] = str_date
                                        if str_date2 == str(lst[k]['Date']):
                                            flag = True
                                            print('Data appended for : ', str_date2)
                                    else:
                                        t_row[headers[i]] = td.text.replace('\n', '').strip()
                                if flag == True:
                                    pm_val = lst[k]['PM2.5 AQI Value']
                                    qual = lst[k]['AQI Category']
                                    t_row['PM2.5 AQI Value'] = pm_val
                                    t_row['AQI Category'] = qual
                                    k+=1
                                table_data.append(t_row)
                            #print(table_data)
                            #break
                        except Exception as e:
                            print(e)
                    except Exception as ex:
                        print(ex)


        with open(f"data.csv", 'w') as out_file:
            writer = csv.DictWriter(out_file, headers)
            writer.writeheader()
            for row in table_data:
                if row:
                    writer.writerow(row)

if __name__ == '__main__':
    objd = extract_data()
    objd.get_html_data()
    objd.combine_data()


