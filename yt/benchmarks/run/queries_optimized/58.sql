-- NB: Subqueries
$week_seq = (select d_week_seq
                                      from date_dim
                                      where d_date = Date('1998-02-21'));
$ss_items =
 (select item.i_item_id item_id
        ,sum(ss_ext_sales_price) ss_item_rev
 from store_sales
    cross join date_dim
     cross join item
     
 where ss_sold_date_sk   = d_date_sk
   and ss_item_sk = i_item_sk
   and d_date in (select d_date
                  from date_dim
                  where d_week_seq = $week_seq)
   
 group by item.i_item_id);
$cs_items =
 (select item.i_item_id item_id
        ,sum(cs_ext_sales_price) cs_item_rev
  from catalog_sales
      cross join date_dim
      cross join item
      
 where cs_sold_date_sk = d_date_sk 
  and cs_item_sk = i_item_sk
  and  d_date in (select d_date
                  from date_dim
                  where d_week_seq = $week_seq)
  
 group by item.i_item_id);
$ws_items =
 (select item.i_item_id item_id
        ,sum(ws_ext_sales_price) ws_item_rev
  from web_sales
      cross join date_dim
      cross join item
 where ws_sold_date_sk   = d_date_sk 
  and ws_item_sk = i_item_sk
  and  d_date in (select d_date
                  from date_dim
                  where d_week_seq =$week_seq)
  
 group by item.i_item_id);
-- start query 1 in stream 0 using template query58.tpl and seed 1819994127
  select  ss_items.item_id
       ,ss_item_rev
       ,ss_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 ss_dev
       ,cs_item_rev
       ,cs_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 cs_dev
       ,ws_item_rev
       ,ws_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 ws_dev
       ,(ss_item_rev+cs_item_rev+ws_item_rev)/3 average
 from $ss_items ss_items cross join $cs_items cs_items cross join $ws_items ws_items
 where ss_items.item_id=cs_items.item_id
   and ss_items.item_id=ws_items.item_id
   and ss_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
   and ss_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
   and cs_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
   and cs_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
   and ws_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
   and ws_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
 order by item_id
         ,ss_item_rev
 limit 100;

-- end query 1 in stream 0 using template query58.tpl
