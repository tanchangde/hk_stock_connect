# 计算港股累计市值覆盖率

library(conflicted)
library(lixingr)
library(tidyverse)
library(magrittr)
library(vroom)
library(lubridate)
library(tidyr)
library(writexl)
conflicts_prefer(
  dplyr::filter(),
  dplyr::lag()
)

times_tamp <- format(now(), "%Y%m%d%H%M")

# 获取所有上市股票代码及财报类型 ----
list_stock_codes <- lxr_query(lxr_hk_company(), flatten = FALSE)

# 获取市值查询参数 ----
tb_stock_codes <- list_stock_codes %>%
  extract2("data") %>%
  map(~ list(
    stock_codes = .x$stockCode,
    fsTableType = .x$fsTableType,
    listingStatus = .x$listingStatus
  )) %>%
  bind_rows() %>%
  filter(listingStatus != "delisted")

tb_query_params <- tb_stock_codes %>%
  mutate(
    endpoint = case_match(
      fsTableType,
      "non_financial" ~ lxr_hk_company_fundamental_non_financial(),
      "bank" ~ lxr_hk_company_fundamental_bank(),
      "security" ~ lxr_hk_company_fundamental_security(),
      "insurance" ~ lxr_hk_company_fundamental_insurance(),
      "reit" ~ lxr_hk_company_fundamental_reit(),
      "other_financial" ~ lxr_hk_company_fundamental_other_financial()
    ),
    start_date = "2024-01-01",
    end_date = "2024-10-02",
    metrics_list = "mc"
  ) %>%
  select(-fsTableType, -listingStatus)

safe_lxr_query <- safely(lxr_query)

# 批量获取市值 ----
# API 每分钟最大请求次数为 1000 次
chunk_size <- 999
total_rows <- nrow(tb_query_params)
num_chunks <- ceiling(total_rows / chunk_size)
chunk_indices <- map(1:num_chunks, ~ {
  start <- (.x - 1) * chunk_size + 1
  end <- min(.x * chunk_size, total_rows)
  c(start, end)
})

tb_mc <- map(chunk_indices, ~ {
  Sys.sleep(60)
  pmap(tb_query_params %>% slice(.x[1]:.x[2]), safe_lxr_query) %>%
    keep(~ is.null(.x$error)) %>%
    map("result") %>%
    bind_rows()
}) %>%
  bind_rows()

# 计算累计市值覆盖率 ----
tb_cum_mc_pct <- tb_mc %>%
  group_by(stockCode) %>%
  summarise(avg_mc = sum(mc, na.rm = TRUE) / n()) %>%
  arrange(desc(avg_mc)) %>%
  mutate(
    pct = avg_mc / sum(avg_mc, na.rm = TRUE),
    cum_pct = cumsum(pct)
  )

# 输出数据 ----
saveRDS(tb_mc, file = paste0("hk_stock_market_cap_2024_", times_tamp, ".rds"))

tb_cum_mc_pct %>%
  rename(
    股票代码 = "stockCode",
    平均市值 = "avg_mc",
    平均市值占比 = "pct",
    累计市值覆盖率 = "cum_pct"
  ) %>%
  write_xlsx(
    path = paste0("港股_2024_累计市值覆盖率_", times_tamp, ".xlsx")
  )
