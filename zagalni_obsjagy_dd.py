import pandas as pd
from tableauscraper import TableauScraper as TS
import time
import random

# Зайти на https://public.tableau.com/app/profile/neurc/viz/_16322904158490/1_
# Якщо на дашборді оновилися дані, то порядок дій такий:
# 1. Зайти на https://public.tableau.com/app/profile/neurc/viz/_16322904158490/1_
# 2. На дашборді вибираємо той місяць, що стоїть останнім в url_and_date
# 3. Натискаємо кнопку share (знаходиться під дашбордом) і копіюємо значення "link" (До символа "?") і замінимо ним той що наразі в url_and_date
# 4. На дашборді обираємо наступний місяць натискаємо share і копіюємо значення "link" (До символа "?")
# 5. Додаємо новий запис до url_and_date новий url
url_and_date = [
    ['https://public.tableau.com/shared/YYDDJQD5J', 'January 2023',
     'ОЕС України (синхронізована з ENTSO-E)'],
    ['https://public.tableau.com/shared/ZQDXSWCCR', 'February 2023', 'ОЕС України (синхронізована з ENTSO-E)'],
    ['https://public.tableau.com/shared/HB9DP7HK7', 'March 2023', 'ОЕС України (синхронізована з ENTSO-E)'],
    ['https://public.tableau.com/shared/MSRBRM7GD', 'April 2023', 'ОЕС України (синхронізована з ENTSO-E)'],
    ['https://public.tableau.com/shared/83GB86D77', 'May 2023', 'ОЕС України (синхронізована з ENTSO-E)'],
    ['https://public.tableau.com/views/_16322904158490/1_', 'June 2023', 'ОЕС України (синхронізована з ENTSO-E)'],
]
res_df = []
for url, date, zone in url_and_date:
    ts = TS()
    print(date)
    ts.loads(url)
    workbook = ts.getWorkbook()
    df = workbook.worksheets[0].data
    df['Торгова зона'] = zone
    res_df.append(df)
    # random time sleep
    time.sleep(random.randint(1, 5))
res_df = pd.concat(res_df)
res_df.to_excel('nkrekp.xlsx')

res_df = pd.read_excel('nkrekp.xlsx', engine='openpyxl')

# create new column with date
res_df['date'] = res_df['MY(Дата)-value'].astype(str).str[:4] + '-' + res_df['MY(Дата)-value'].astype(str).str[
                                                                      4:] + '-' + res_df['DAY(Дата)-value'].astype(
    str)
res_df['date'] = pd.to_datetime(res_df['date'])
res_df['Година-value'] = res_df['Година-value'].replace(1, '00:00-01:00').replace(2, '01:00-02:00').replace(3,
                                                                                                            '02:00-03:00').replace(
    4, '03:00-04:00').replace(5, '04:00-05:00').replace(6, '05:00-06:00').replace(7, '06:00-07:00').replace(8,
                                                                                                            '07:00-08:00').replace(
    9, '08:00-09:00').replace(10, '09:00-10:00').replace(11, '10:00-11:00').replace(12, '11:00-12:00').replace(13,
                                                                                                               '12:00-13:00').replace(
    14, '13:00-14:00').replace(15, '14:00-15:00').replace(16, '15:00-16:00').replace(17, '16:00-17:00').replace(18,
                                                                                                                '17:00-18:00').replace(
    19, '18:00-19:00').replace(20, '19:00-20:00').replace(21, '20:00-21:00').replace(22, '21:00-22:00').replace(23,
                                                                                                                '22:00-23:00').replace(
    24, '23:00-00:00')
res_df['SUM(Загальний обсяг електирчної енергії проданої за ДД, МВт*год)-value'] = res_df[
                                                                                       'SUM(Загальний обсяг електирчної енергії проданої за ДД, МВт*год)-value'] / 1000
res_df = res_df.rename(columns={'Година-value': 'Час',
                                'SUM(Загальний обсяг електирчної енергії проданої за ДД, МВт*год)-value': 'Обсяг торгівлі, МВт*год',
                                'date': 'Дата'})
res_df = res_df[['Дата', 'Час', 'Торгова зона', 'Обсяг торгівлі, МВт*год']]

# Отримуємо останню дату з стовбчика 'Дата'
last_date = res_df['Дата'].max()
# В стовбчику 'Дата' знаходимо першу дату first_date
first_date = res_df['Дата'].min()

res_df = res_df.sort_values(
    by=['Дата', 'Час', 'Торгова зона'])  # сортуємо по даті, часу, торговій зоні, митному режиму, країні
res_df.to_excel(f'{path}/scrape_data/{last_date.strftime("%Y_%m_%d")}_zahalna_torhivlia_rdd.xlsx', index=False)
