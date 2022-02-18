# THIS SCRIPT IS FOR REPORTING BRAND RELAUNCH KPI
# GITHUB: BRAND-RELAUNCH-KPI-TRACKING

# GOAL ----
# 1. CALCULATE AGE DISTRIBUTION FROM CUSTOMERS WHO SIGNED UP TO LOYALTY PROGRAME AFTER BRAND RELAUNCH
# 2. CALCULATE SPEND OVER TIME, AVG TRANSACTION NUMBER, AVG TRANSACTIONAL VALUE, CATEGORY PENETRATION FOR MORE THAN 1 CATEGORIES

# BRIEF FROM ANDREW POWELL
# Hi Andrew,
# 
# As discussed during the catch-up, I would recommend not to list acquisition of 25-35 as part of the KPI as the metric is affected by 
# 1) low birthday acquired from customers (between Feb-Apr, tag rate for FY19-20 was 10%, but is now 4% ), 
# 2) no existing tactics to drive this. As such, we have agreed on using % penetration instead of acquisitions on core consumers.
# 
# Here are the metrics and baselines for our core consumers (25-34 yo) between Feb-Apr 2021:
#   
# Avg transactional value: $117.3 NZD
# Avg transactions: 1.40
# Category penetration for more than 1 categories: 28%
# 
# The metrics will be reviewed at the beginning of Aug for the 3m check-in (brand launch was on the 10/05), and I will compare the metrics in 2 parts, 1) pre vs post, which will compare Feb-Apr21 to May - Jul21, and 2) comparison on last year, which is May - Jul21 vs May - Jul20 to understand seasonal impact.
# 
# Cheers,
# 
# Arthur
# 

# Construct report ----------------------------------------------------
library(tidyverse)
library(DBI)
library(keyring)
library(lubridate)
library(kmdr)

# Set date variables ----------------------------------------------------------
reporting_period_end <- as.Date("2021-12-12")
reporting_period_start <- floor_date(reporting_period_end, "week", week_start = 1)

# Connect to database -----------------------------------------------------
con <- DBI::dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "BIDW", 
                      Database = "data_warehouse", UID = keyring::key_get("email_address"), 
                      PWD = keyring::key_get("kmd_password"), Trusted_Connection = "TRUE", 
                      Port = 1433)


# Collect New Members -----------------------------------------------------
collect_new_members_age <- function(.connection, 
                                    .period_start, 
                                    .period_end) {
  bq <- base_member_query(connection = .connection) %>% 
    dplyr::filter(country %in% c("Australia", "New Zealand", "United Kingdom", "United States")) %>% 
    dplyr::filter(dplyr::between(date_joined, .period_start, .period_end)) %>% 
    select(customer_number, customer_type, contact_by_email, join_cal_month_start, join_fin_year,
           country, state_province, city_town, date_of_birth, date_joined) %>% 
    dplyr::collect() %>% 
    dplyr::mutate(member_age = lubridate::as.period(lubridate::interval(start = date_of_birth, 
                                                                        end = date_joined))$year) %>% 
    dplyr::mutate(age_bracket = dplyr::case_when(
      member_age < 17 ~ "0-17",
      member_age < 25 ~ "18-24",
      member_age < 35 ~ "25-34", 
      member_age < 40 ~ "35-39", 
      member_age < 55 ~ "40-54", 
      member_age < 65 ~ "55-64", 
      TRUE ~ "65+" 
    ))
  
  return(bq)
}

acquisition_monthly_tbl <- collect_new_members_age(.connection   = con, 
                                                   .period_start = reporting_period_start,
                                                   .period_end   = reporting_period_end) %>% 
  recode_dates() %>% 
  rename(period_start = join_cal_month_start) %>% 
  group_by(period_start, join_fin_year, country, age_bracket) %>% 
  summarise(n = n()) %>% 
  ungroup()



# Collect Master Data for calculating KPI ----------------------------------------------------

collect_sales_metrics_modified <- function(.connection, .period_start, .period_end){
  
  # this is a copy of base_txn_query with DOB and date_joined added
  tbls <- set_source_tables(connection = con)
  total_kmd_sales_tbl <- tbls$fact_sales_trans %>% 
    dplyr::select(dim_date_key,dim_customer_key, dim_country_key, dim_location_key, dim_gift_voucher_key, sale_transaction, sale_amount_incl_gst, 
                  sale_amount_excl_gst, sale_qty, dim_product_key, dim_gift_voucher_key) %>% 
    dplyr::filter(dim_gift_voucher_key == -1) %>% 
    dplyr::left_join(dplyr::select(tbls$dim_customer, dim_customer_key, customer_number, customer_type, contact_by_email, date_of_birth, date_joined), by = c("dim_customer_key")) %>% 
    dplyr::left_join(dplyr::select(tbls$dim_date, dim_date_key, full_date, cal_month_start, fin_year, acc_week_of_year, week_start), by = c("dim_date_key")) %>% 
    dplyr::filter(dplyr::between(full_date, period_start, period_end)) %>% 
    dplyr::left_join(dplyr::select(tbls$dim_location, dim_location_key, location_code), by = c("dim_location_key")) %>% 
    dplyr::left_join(dplyr::select(tbls$dim_product, dim_product_key, product_group, item_group), by = c("dim_product_key")) %>% 
    encode_sales_country() %>% 
    encode_sales_channel() %>% 
    encode_sales_location() %>% 
    dplyr::select(full_date, cal_month_start, fin_year, acc_week_of_year, week_start, 
                  sales_country, sales_channel, sales_location, dim_customer_key, 
                  customer_number, customer_type, sale_transaction, sale_amount_incl_gst, 
                  sale_amount_excl_gst, sale_qty, dim_product_key, dim_gift_voucher_key, 
                  date_of_birth, date_joined, product_group, item_group)
    return(total_kmd_sales_tbl)
}

# Collect spend trend and segment by New and Existing Members----
customer_age_spend_profile_metrics_tbl <- collect_sales_metrics_modified(.connection   = con, 
                                                                   .period_start = reporting_period_start,
                                                                   .period_end   = reporting_period_end) %>%
  dplyr::mutate(customer_type = dplyr::case_when(customer_type == "Summit Club" ~ "Summit Club", 
                                                           TRUE ~ "Non Member"),
                member_status = dplyr::case_when(date_joined < period_start ~ "Existing Members",
                                                 TRUE ~ "New Members")) %>% 
  dplyr::filter(customer_type %in% c("Summit Club")) %>%
  dplyr::collect() %>% 
  # member age is set to when they sign up to the program
  dplyr::mutate(member_age     = lubridate::as.period(lubridate::interval(start = date_of_birth, 
                                                                      end = date_joined))$year,
                period_start   = cal_month_start) %>%
  dplyr::mutate(age_bracket = dplyr::case_when(
    member_age < 17 ~ "0-17",
    member_age < 25 ~ "18-24",
    member_age < 35 ~ "25-34", 
    member_age < 40 ~ "35-39", 
    member_age < 55 ~ "40-54", 
    member_age < 65 ~ "55-64", 
    TRUE ~ "65+" 
  )) %>% 
  dplyr::group_by(period_start, sales_country, member_status, age_bracket) %>% 
  dplyr::summarise(sum_sales_incl_gst = sum(sale_amount_incl_gst, na.rm = TRUE), 
                   sum_txns = dplyr::n_distinct(sale_transaction), 
                   sum_shoppers = dplyr::n_distinct(customer_number), 
                   sum_units_sold = sum(sale_qty, na.rm = TRUE)) %>% 
  dplyr::ungroup() %>% 
  dplyr::mutate(mean_txn_sale_amount = (sum_sales_incl_gst/sum_txns), 
                mean_member_rev = (sum_sales_incl_gst/sum_shoppers), 
                mean_visit_freq = (sum_txns/sum_shoppers), mean_basket_size = (sum_units_sold/sum_txns))
   
# below is for creating different product categories

Jackets & vests 
Puffer/insulated Jacket or Vest
Rain Jacket
Fleece/Soft Shell Jacket

Footwear
Hiking Footwear
Walking / running footwear
Other footwear for climbing, camping, travel

Outdoor Clothing
Merino clothing & accessories
Shirt/T-shirt/Top
Pants/ Shorts
Leggings
Thermals
Dresses/ Skirts
Hoodie/ Pullover
Clothing accessories (hats, socks, gloves etc)
Kids wear

Activity Specific Gear
Tents / Shelters
Camping items (lighting, kitchen, furniture etc.)
Sleeping bags / mats
Travel Accessories
Climbing equipment
Hiking accessories
Drink Bottles

Bags
Hiking
Backpack
Trolley / hybrid


Others


