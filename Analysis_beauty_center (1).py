#!/usr/bin/env python
# coding: utf-8

# <div class="alert alert-info">
# <b>"Сквозная аналитика для Салона красоты"</b> 
# </div>

# In[1]:


# Импортируем необходимые библиотеки для анализа
import pandas as pd 
import numpy as np
from datetime import datetime
from datetime import timedelta


# In[2]:


# Загрузим датасеты для анализа - данные по рекламе
df_ads = pd.read_csv('C:\\Users\\HP\\Desktop\\DataAnalitics\\ads.csv', sep=',', na_values='?', encoding='cp1251')
df_ads.head()


# In[3]:


# Посмотрим основную информацию по датасету.
df_ads.info()


# In[4]:


# Приведем метки к формату обьект.
df_ads['d_utm_campaign'] = df_ads.d_utm_campaign.astype('object')
df_ads['d_utm_content'] = df_ads.d_utm_content.astype('object')


# In[5]:


# Загрузим данные по лидам, сразу меняем пропуски в метках на 'not set' и сортируем по дате
df_leads = pd.read_csv('C:\\Users\\HP\\Desktop\\DataAnalitics\\leads.csv', sep=',', encoding='cp1251').fillna('not set').sort_values(by='lead_created_at', ascending=True)

# Оставим только те данные, где рекламный истоник - yandex
df_leads = df_leads[df_leads['d_lead_utm_source'] == 'yandex']
df_leads.head()


# In[7]:


# Загрузим данные по оплатам, отсортируем по дате оплаты
df_pay = pd.read_csv('C:\\Users\\HP\\Desktop\\DataAnalitics\\purchases.csv', sep=',', na_values='?', encoding='cp1251').sort_values(by='purchase_created_at', ascending=True)
# удалим строки с нулевыми продажами
df_pay = df_pay.loc[df_pay['m_purchase_amount'] != 0.0]
df_pay.head()


# In[8]:


# Подтянем метки к таблице с оплатами
df_pay_utm = df_pay.merge(df_leads, how='left', on=['client_id'])

# Изменим тип данных на дату
df_pay_utm['purchase_created_at'] = pd.to_datetime(df_pay_utm['purchase_created_at'])
df_pay_utm['lead_created_at'] = pd.to_datetime(df_pay_utm['lead_created_at'])

# Добавим новый столбец period, посчитаем период между датой создания заявки и оплаты
df_pay_utm['period'] = df_pay_utm['purchase_created_at'] - df_pay_utm['lead_created_at']

# Оставим только те оплаты, где период не более 15 дней
df_pay_utm = df_pay_utm[df_pay_utm['period'] < '16 days']
df_pay_utm = df_pay_utm[df_pay_utm['period'] > '0 days']

df_pay_utm.head()


# In[9]:


# добавим в эту таблицу новый столбец со сцепкой меток
df_pay_utm['concat'] = df_pay_utm[['lead_created_at', 'd_lead_utm_source', 'd_lead_utm_medium', 'd_lead_utm_campaign']].astype(str).agg('/'.join, axis=1)


# In[10]:


# Сделаем сводную таблицу по оплатам, чтобы объединить покупки по меткам
df_pivot_pay = df_pay_utm.pivot_table(index='concat', 
                                  aggfunc={'purchase_id': 'count', 'm_purchase_amount' : 'sum'}).reset_index()
# переименуем колонки
df_pivot_pay.rename(columns = {'purchase_id' : 'quantity', 'm_purchase_amount' : 'summa'}, inplace = True)
df_pivot_pay.tail()


# In[11]:


# Видим, что под правило 15 дней попадают оплаты только на 2.3 млн руб, 
# остальные оплаты либо были сделаны раньше или позже заявок, либо по другим меткам.
df_pivot_pay.summa.sum()


# In[12]:


# Сделаем сводную таблицу по расходам
df_pivot_cost = df_ads.pivot_table(index=['created_at', 'd_utm_source', 'd_utm_medium', 'd_utm_campaign'], values=['m_clicks', 'm_cost'], aggfunc='sum').round(2).reset_index()

# добавим новый столбец со сцепкой даты и меток
df_pivot_cost['concat'] = df_pivot_cost[['created_at', 'd_utm_source', 'd_utm_medium', 'd_utm_campaign']].astype(str).agg('/'.join, axis=1)
df_pivot_cost.tail()


# In[13]:


# Сделаем сводную таблицу по лидам
df_pivot_lead = df_leads.pivot_table(index=['lead_created_at', 'd_lead_utm_source', 'd_lead_utm_medium', 'd_lead_utm_campaign'], values='lead_id', aggfunc='count').reset_index()

# добавим новый столбец со сцепкой даты и меток
df_pivot_lead['concat'] = df_pivot_lead[['lead_created_at', 'd_lead_utm_source', 'd_lead_utm_medium', 'd_lead_utm_campaign']].astype(str).agg('/'.join, axis=1)
df_pivot_lead.head()


# In[14]:


# Объединим таблицы по расходам и лидам
df_merge = df_pivot_cost.merge(df_pivot_lead[['lead_id', 'concat']], how='left', on=['concat']).fillna(0).sort_values(by='created_at', ascending=True)
df_merge.head()


# In[15]:


# Проверяем, что все данные подтянулись
# Видим, что рекламные кампании yandex - привели на сайт 1582 лида (всего за этот период было 25 тыс лидов)
print(df_merge.m_cost.sum())
print(df_merge.m_clicks.sum())
print(df_merge.lead_id.sum())


# In[16]:


# Подтянем оплаты к объединенной таблице 
df_merge = df_merge.merge(df_pivot_pay[['quantity', 'summa', 'concat']], how='left', on=['concat']).fillna(0)
df_merge.head()


# In[17]:


# Данный рекламный источник принес компании около 2 млн руб.
df_merge.summa.sum()


# In[18]:


# Добавим два столбца с метриками
df_merge['CPL'] = (df_merge['m_cost'] / df_merge['lead_id']).round(2)
df_merge['ROAS'] = (df_merge['summa'] / df_merge['m_cost']).round(2)


# In[19]:


# Приведем таблицу в читабельный вид и выгрузим отчет
df_merge.drop('concat', axis=1, inplace=True)
df_merge.rename(columns = {'created_at' : 'Дата',
                           'd_utm_source' : 'UTM source', 
                           'd_utm_medium' : 'UTM medium', 
                           'd_utm_campaign' : 'UTM campaign',
                           'm_clicks' : 'Количество кликов', 
                           'm_cost' : 'Расходы на рекламу', 
                           'lead_id' : 'Количество лидов', 
                           'quantity' : 'Количество покупок', 
                           'summa' : 'Выручка от продаж'}, inplace = True)
df_merge


# In[20]:


df_merge.fillna(0).to_csv('analysis_test.csv')

