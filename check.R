# 校验未返回数据 ----
tb_stock_codes_null <- tb_stock_codes %>%
  anti_join(tb_mc, by = join_by(stock_codes == stockCode))

tb_query_params_null <- tb_stock_codes_null %>%
  mutate(
    endpoint = lxr_hk_company_candlestick(),
    stock_code = stock_codes,
    type = "ex_rights",
    start_date = "2016-01-01"
  ) %>%
  select(endpoint, stock_code, type, start_date)

list_mc_null_candlestick <-
  pmap(tb_query_params_null, safe_lxr_query)

# error 查询需为 0，否则扩大上文 K 线查询范围
tb_mc_null_candlestick_error <- list_mc_null_candlestick %>%
  keep(~ !is.null(.x$error))

tb_mc_null_candlestick <- list_mc_null_candlestick %>%
  keep(~ is.null(.x$error)) %>%
  rlang::set_names(tb_stock_codes_null$stock_codes) %>%
  bind_rows(.id = "stock_code") %>%
  unnest(cols = c(result))

# 补充查询 ----
tb_tb_mc_null_additional_query <- tb_mc_null_candlestick %>%
  mutate(date = ymd_hms(date)) %>%
  group_by(stock_code) %>%
  summarise(date_latest = max(date)) %>%
  filter(date_latest > "2023-12-31")

tb_query_params_additional_query <- tb_query_params %>%
  semi_join(
    tb_tb_mc_null_additional_query,
    by = join_by(stock_codes == stock_code)
  )

list_mc_additional_query <-
  pmap(tb_query_params_additional_query, safe_lxr_query)

list_mc_additional_query %>%
  keep(~ !is.null(.x$error))