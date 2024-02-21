import pandas as pd
import re
import time
#Задача 1
df = pd.read_csv('visit_log.csv', encoding='utf-8',sep=';')

def sourceType(type):
    keyword_list = type['traffic_source']
    if (keyword_list == 'yandex') | (keyword_list == 'google'):
        return 'organic'
    elif (keyword_list == 'paid') | (keyword_list == 'email') & (type['region'] == 'Россия'):
        return 'ad'
    elif (keyword_list == 'paid') | (keyword_list == 'email') & (type['region'] != 'Россия'):
        return 'other'
    else:
        return keyword_list

df['source_type'] = df.apply(sourceType,axis=1)
# # print(df.tail())

#Задача 2

df_url = pd.read_csv('URLs.txt', encoding='utf-8')
def type_(x):
    x = re.search(reg_url, x)
    x_string = x.group(0)
    if '/' in x_string:
        new_s = re.sub(r"[/]", "", x_string)
    return new_s
reg_url = '/\w+'
reg ='/\d+-'
df_filter_url = df_url[df_url.url.str.contains(reg)]
df_filter_url['type'] = df_filter_url['url'].apply(type_)
# print(df_filter_url.head())

#Задание 3

start_time = time.time()

df_rating = pd.read_csv('ratings.csv', encoding='utf-8')
df_group = df_rating.groupby(['userId']).agg({'timestamp': ['max','min','count']}).reset_index()
df_group.columns = ['_'.join(col).rstrip('_') for col in df_group.columns.values]

df_filter = df_group.loc[df_group['timestamp_count'] > 100]
df_filter['mean_life_cycle'] = df_filter['timestamp_max'] - df_filter['timestamp_min']
df_result = df_filter.sort_values('mean_life_cycle', ascending=False)
end_time = time.time()  # время окончания выполнения
execution_time = end_time - start_time  # вычисляем время выполнения
# print(df_result.head())
# print(f"Время выполнения программы: {execution_time} секунд")

#Задача 4

rzd = pd.DataFrame(
    {
        'client_id': [111, 112, 113, 114, 115],
        'rzd_revenue': [1093, 2810, 10283, 5774, 981]
    }
)

auto = pd.DataFrame(
    {
        'client_id': [113, 114, 115, 116, 117],
        'auto_revenue': [57483, 83, 912, 4834, 98]
    }
)

air = pd.DataFrame(
    {
        'client_id': [115, 116, 117, 118],
        'air_revenue': [81, 4, 13, 173]
    }
)
client_base = pd.DataFrame(
    {
        'client_id': [111, 112, 113, 114, 115, 116, 117, 118],
        'address': ['Комсомольская 4', 'Энтузиастов 8а', 'Левобережная 1а', 'Мира 14', 'ЗЖБИиДК 1',
                    'Строителей 18', 'Панфиловская 33', 'Мастеркова 4']
    }
)

df_merged = pd.concat([rzd, auto, air]
                         ,axis=0)

purchases_pivot = df_merged.pivot_table(index='client_id', values=['rzd_revenue','auto_revenue','air_revenue'],
                                         aggfunc='sum', fill_value=0)
df_merged_adress = purchases_pivot.merge(client_base, how='left', on = 'client_id')

# df_merged_adress.to_csv('new_file.csv',encoding='utf-8')
# purchases_pivot.to_csv('without_address.csv',encoding='utf-8')
# print(df_merged.head())
# print(df_merged_adress.head())