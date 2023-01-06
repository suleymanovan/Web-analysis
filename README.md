# Web-analysis
Сквозной анализ эффективности рекламного источника
"Сквозная аналитика для Салона красоты"

Анализ представлен в файле Analysis_beauty_center.ipynb.

Ссылка на отчет в Google таблице:
https://docs.google.com/spreadsheets/d/1WlNFIAdGMel4tS_2JV_Qlpe3r2tiG6C6H4tQabjzLwE/edit?usp=sharing

Ссылка на дашборд в Data Looker:
https://datastudio.google.com/embed/reporting/89e9fccf-0e77-426c-9f35-66c97f19ab0b/page/6c25C

Проверки на качество данных:
1. Поля с метками в таблице с рекламными кампаниями всегда заполнены:
SELECT created_at 
FROM ads
WHERE d_utm_source IS NULL or d_utm_medium IS NULL or d_utm_campaign IS NULL

2. В таблице с лидами - id лида всегда заполнено:
SELECT lead_created_at 
FROM leads 
WHERE lead_id IS NULL

3. В таблице с оплатами - id клиента всегда заполнено:
SELECT purchase_created_at 
FROM purchases 
WHERE client_id IS NULL

4. Проверить уникальность lead_id, вернет любые повторяющиеся значения:
SELECT
    lead_id,
    SUM(1) AS count
FROM leads
GROUP BY 1
HAVING count > 1
