-- sqlite
-- https://www.jdoodle.com/execute-sql-online/
create table IMPRESSIONS
(
    Product_id string,
    click      boolean,
    'date'     date
);
insert into IMPRESSIONS
values ('1002313003', true, '2018-07-10'),
       ('1002313002', false, '2018-07-10'),
       ('1002313003', false, '2018-07-11');

create table PRODUCTS
(
    Product_id  string,
    Category_id string,
    price       int
);
insert into PRODUCTS
values ('1002313003', '1', 10),
       ('1002313002', '2', 15);

create table PURCHASES
(
    Product_id string,
    User_id    string,
    'date'     date
);
insert into PURCHASES
values ('1002313003', '1003431', '2018-07-10'),
       ('1002313002', '1003432', '2018-07-11');

-- 3.1
select Product_id, strftime('%Y-%m', date), avg(click) as avg_clicks
from IMPRESSIONS
group by Product_id, strftime('%Y-%m', date);

-- 3.2
select PRODUCTS.Category_id, strftime('%Y-%m', IMPRESSIONS.date), avg(IMPRESSIONS.click) as avg_clicks
from PRODUCTS
         join IMPRESSIONS
              on PRODUCTS.Product_id = IMPRESSIONS.Product_id
group by PRODUCTS.Category_id, strftime('%Y-%m', IMPRESSIONS.date)
order by avg_clicks desc
limit 3;

-- 3.3
select price_tiers.price_tier, strftime('%Y-%m', IMPRESSIONS.date), avg(IMPRESSIONS.click) as avg_clicks
from IMPRESSIONS
         join (
    select case
               when price >= 0 and price < 5 then '0-5'
               when price >= 5 and price < 10 then '5-10'
               when price >= 10 and price < 15 then '10-15'
               else '>15'
               end as price_tier,
           Product_id
    from PRODUCTS) price_tiers
              on IMPRESSIONS.Product_id = price_tiers.Product_id
group by price_tiers.price_tier, strftime('%Y-%m', IMPRESSIONS.date)