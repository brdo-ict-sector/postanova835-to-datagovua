library(tidyverse)
library(readxl)
library(ckanr)
library(httr)

setwd("C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/projects/open_data/postanova_835/clean data")

# це розпорядники і набори з постанови

req_datasets_oct24 <- 
  read_xlsx("835 - organizations and required datasets_Oct24.xlsx") |>
  filter(!str_detect(Розпорядник, '^Усі')) 

req_datasets_oct24 |> filter(str_detect(Розпорядник, '(М|м)ісц')) |> 
  count(Розпорядник)

req_datasets_oct24 <- 
  req_datasets_oct24 |> filter(!str_detect(`Набір даних`, '\\{')) |>
  rename(org = Розпорядник, dataset = `Набір даних`)

req_datasets_oct24 <- 
  req_datasets_oct24 |> 
  filter(!str_detect(dataset, 'водний кадастр за розділами:'))

req_datasets_oct24 <- req_datasets_oct24 |> 
  filter(!str_detect(org, '(М|м)ісц')) # має бути всього 914 наборів

req_datasets_jul25 <- read_xlsx("835 - organizations and required datasets_Jul25.xlsx",
                                sheet = "835_Jul25") |>
  mutate(across(everything(), ~ str_trim(.))) 

req_datasets_jul25 <- req_datasets_jul25 |> 
  filter(!is.na(dataset)) |> 
  fill(org, .direction = 'down') |>
  filter(!str_detect(dataset, '\\{')) |>
  distinct() # має бути всього 901 набір 
# Державний реєстр атестованих судових експертів двічі згадується у Мін'юста 

req_datasets_jul25 |> summarise(
  n = n(),
  .by = org
) 

length(unique(req_datasets_jul25$org)) # у Постанові згадані 83 розпорядники

# звідси id організацій, прив'язані до назв розпорядників на Порталі
req_orgs_id_df <- read_xlsx('aux_org_835_to_classify_Jul25.xlsx') 

req_orgs_absent <- req_orgs_id_df |> filter(is.na(org_id)) # цих взагалі немає на порталі

req_orgs_id_df <- req_orgs_id_df |> filter(!is.na(org_id))

nrow(req_orgs_id_df) # є 79 організацій на Порталі

# тут усі доступні набори від необхідних розпорядників (включно з необов'язковими)
packages_data <- read_xlsx("portal_data_packages_and_resources - 2025-07-04.xlsx",
                           sheet = 'packages_data')

# набори з порталу усі мають унікальні ід
packages_data|> distinct(dataset_id) |> nrow() == nrow(packages_data)

# фактично є 74 розпорядники, бо є також пусті акаунти
packages_data |> count(org_id) |> nrow() 

factually_absent_org <- 
  setdiff(
    req_orgs_id_df$org_id,
    unique(packages_data$org_id)
  )

# оце створені, але без жодного набору:
req_orgs_id_df |> filter(org_id %in% factually_absent_org) 

# тут відповідні фактичним наборам ресурси - 33.4 тис
resources_data <- read_xlsx("portal_data_packages_and_resources - 2025-07-04.xlsx",
                            sheet = 'resources_data')


# тепер по назвам розпорядників з'єдную розпорядників з постанови з id розпорядників

req_orgs_id_df2 <- # мав би бути 901 опублікований набір
  left_join(
    req_datasets_jul25 |> rename(required_dataset = dataset),
    req_orgs_id_df,
    join_by(org)
  ) |> 
  left_join(packages_data |> 
              select(org_id, org_name_portal = org_title) |> distinct(),
            join_by(org_id))

# частини розпорядників взагалі немає, у частини пусті профілі:
req_orgs_id_df3 <- req_orgs_id_df2 |> filter(!is.na(org_id)) |>
  filter(!org_id %in% factually_absent_org) # шукаю 861 набір



setwd("C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/projects/open_data/postanova_835/clean data/antons_data")

list.files()

# це набір Антона, де він вручну присвоював наборам з Постанови id з Порталу
antons_data <- read_xlsx("10.dataset_manual_processing_anton_corrected - 2025-07-23.xlsx", 
                         sheet = 'processed') |> 
  # перепровірив, знайшов 8 неправильно атрибутованих id
  filter(!str_detect(required_dataset, '^ур'))

unique(antons_data$org_name_portal)

# перевіряю, чи є задвоєння
req_orgs_id_df3 |> count(org_name_portal, required_dataset) |> filter(n > 1)
antons_data |> count(org_name_portal, required_dataset) |> filter(n > 1)

# 8 наборів, де дублюється id - це коли ті самі id відповідають фактично кільком 
  # різним наборам, що вимагаються постановою
antons_data |> count(dataset_id) |> filter(n > 1 & !is.na(dataset_id)) 
antons_data |> count(required_dataset, dataset_id) |> filter(n > 1)

# це точні збіги наборів/розпорядників - роблю, щоб знайти id, які Антон вже присвоїв
antons_july25_combined <- 
  left_join(
    req_orgs_id_df3,
    antons_data,
    join_by(org_name_portal, required_dataset)
  )

antons_july25_combined |> distinct(dataset_id) |> nrow() # об'єдналося 489
antons_data |> filter(!is.na(dataset_id)) |> distinct(dataset_id) |> nrow() # у Антона було 498

anti_join(
  antons_data,
  antons_july25_combined,
  join_by(dataset_id)
) # це ті 11, хто не поєднався - додам їх нижче

needed_packages_ids <- antons_july25_combined |> 
  filter(!is.na(dataset_id)) |> 
  select(dataset_id) |> as_vector() # у 495 є (488 без дублів)

unique(needed_packages_ids)

# про всяк випадок перевірю ті у яких є присвоєні id з актуальними наборами з порталу
ckanr_setup(url = "https://data.gov.ua/")

# df_urls <- map(needed_packages_ids, 
#                ~ paste0("https://data.gov.ua/api/3/action/package_show?id=", 
#                         .x))
# 
# responses <- map(df_urls, ~ GET(.x))
# 
# text_content <- map(responses, content)
# 
# result <- map(text_content, ~ .x$result)
# 
# df_packages <- 
#   map_df(result, ~ tibble(
#     title = .x$title,
#     maintainer = .x$maintainer,
#     maintainer_email = .x$maintainer_email,
#     author = .x$author,
#     author_email = .x$author_email,
#     purpose = .x$purpose_of_collecting_information,
#     tag_string = .x$tag_string,
#     num_resources = .x$num_resources,
#     update_frequency = .x$update_frequency,
#     metadata_created = .x$metadata_created,
#     metadata_modified = .x$metadata_modified,
#     state = .x$state,
#     dataset_id = .x$id,
#     is_datapackage = .x$is_datapackage,
#     owner_org = .x$owner_org,
#     org_description = .x$organization$description,
#     org_created = .x$organization$created,
#     org_title = .x$organization$title,
#     org_name = .x$organization$name,
#     org_state = .x$organization$state,
#     org_id = .x$organization$id,
#     openness_score = .x$qa$openness_score
#   )
#   ) 

# writexl::write_xlsx(
#   list('actual_packages_name_check' = df_packages),
#   paste0('actual_packages_name_check - ', Sys.Date(), '.xlsx')
# )

setwd("C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/projects/open_data/postanova_835/clean data")

df_packages <- read_xlsx('actual_packages_name_check - 2025-07-15.xlsx')

df_packages_actual <- df_packages |>
  select(title, dataset_id, org_title) |>
  distinct()

df_packages_actual |> count(dataset_id) |> filter(n > 1) |> arrange(desc(n))

checked_ids <- 
  left_join(
    df_packages_actual |> rename(actual_title = title) |> distinct(),
    antons_july25_combined |> select(dataset_id, required_dataset, org_id, org) |>
      filter(!is.na(dataset_id)),
    join_by(dataset_id)
  ) |> relocate(required_dataset, .before = actual_title) |> 
    mutate(is_equal = required_dataset == actual_title) 

# лишаю дублі по id, бо вони вказують на різні набори, які вимагає постанова
checked_ids |> count(dataset_id) |> filter(n > 1) |> arrange(desc(n))
checked_ids |> count(required_dataset, dataset_id) |> filter(n > 1) 

checked_ids |> filter(is_equal == FALSE) # ці очима перевіряю

# ці сам співставляю з даними з Порталу
ids_to_check <- 
  anti_join(
    antons_july25_combined,
    checked_ids |> select(dataset_id),
    join_by(dataset_id)
  ) |> arrange(org_name_portal)

setwd("C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/projects/open_data/postanova_835/clean data")

# writexl::write_xlsx(
#   list('ids_to_check' = ids_to_check),
#   paste0('ids_to_check - ', Sys.Date(), '.xlsx')
# )

ids_manually_checked <- read_xlsx('ids_to_check - 2025-07-14_processed.xlsx')

ids_manually_checked |> filter(str_detect(org, 'прокуро'))

# aux_ids_left_to_check <- 
# anti_join(
#   ids_to_check,
#   ids_manually_checked,
#   join_by(org_id, required_dataset)
# ) 
# 
# writexl::write_xlsx(
#   list('ids_to_check' = aux_ids_left_to_check),
#   paste0('aux_ids_left_to_check - ', Sys.Date(), '.xlsx')
# )

# у мене теж є кілька навмисних дублів по id: 
# це 1-Т від НКЕК і реєстр оброблення відходів разом з суб'єктами господарювання
ids_manually_checked |> count(dataset_id) |> filter(n > 1) |> arrange(desc(n))
checked_ids |> count(required_dataset, dataset_id) |> filter(n > 1)

ids_manually_checked <- ids_manually_checked |> 
  mutate(status = if_else(!is.na(dataset_id), 'наявний', 'відсутній'))

names(ids_manually_checked)
names(checked_ids)

checked_ids <- 
  checked_ids |> 
    select(-is_equal) |> 
    rename(available_dataset = actual_title,
           org_name_portal = org_title) |>
    mutate(status = 'наявний')



required_packs_checked <- rbind(checked_ids, ids_manually_checked) |>
  filter(!org_id %in% factually_absent_org) |> 
  rename(org_names_postanova = org)

required_packs_checked <- required_packs_checked |> filter(!is.na(org_id))

length(unique(required_packs_checked$org_id))

# required_packs_checked <- 
#   required_packs_checked |>
#     mutate(
#       org_name_portal = case_when(
#         org_id == '89fac924-f4c6-4217-8470-1fc03a704828' ~ 'Рахункова палата',
#         org_id == '3befeb6e-efce-48e6-9eef-cb750fbc9ec0' ~ 'Державна служба України з питань безпечності харчових продуктів та захисту споживачів',
#         org_id == '0392dfd5-54c4-4207-afb6-3ecfc856d9c0' ~ 'Аудиторська палата України',
#         org_id == 'a5100a21-f0ea-4fcd-b923-3464391fdb5f' ~ 'Національне агенство із забезпечення якості вищої освіти',
#         org_id == 'b9090ba7-2863-475c-b26d-018db47b644d' ~ 'Національна акціонерна компанія "Нафтогаз України"',
#         org_id == 'af6994ff-883a-444b-acaa-af8617ab24ae' ~ 'ПАТ Укрзалізниця',
#         .default = org_name_portal
#       )
#     ) 



# можна унікалізувати отак:
required_packs_checked |> filter(is.na(dataset_id)) |> 
  count(required_dataset, org_name_portal) |> filter(n > 1)

required_packs_checked_no_id <- required_packs_checked |> filter(is.na(dataset_id))
required_packs_checked_with_id <- required_packs_checked |> filter(!is.na(dataset_id))


packages_data_selected <-
  packages_data |>
  select(available_dataset = title, dataset_id,
         num_resources, update_frequency,
         org_name_portal = org_title, org_id)

required_packs_checked_with_id2 <- 
  left_join( 
    required_packs_checked_with_id,
    packages_data_selected,
    join_by(dataset_id, available_dataset, org_id, org_name_portal)
  ) 

names(required_packs_checked_with_id2)
names(required_packs_checked_no_id)

required_packs_checked_no_id2 <- 
  required_packs_checked_no_id |> 
    mutate(
      num_resources = 0,
      update_frequency = NA
    )

required_packs_clean <- 
  rbind(required_packs_checked_with_id2, required_packs_checked_no_id2) |> 
    as_tibble()

extra_packs <- 
  anti_join(
    packages_data_selected,
    required_packs_clean |> filter(status != 'відсутній'),
    join_by(dataset_id)
  ) |> mutate(
    status = 'додатковий'
  )

names(required_packs_clean)
names(extra_packs)

extra_packs <- 
  extra_packs |> 
    mutate(
      required_dataset = NA
    ) |>
  left_join(required_packs_clean |> distinct(org_id, org_names_postanova),
            join_by(org_id))

length(unique(required_packs_clean$org_id))

required_orgs_all_packs_clean <- 
  rbind(required_packs_clean, extra_packs) |> 
  as_tibble() # це усі набори розпорядників, яких визначає Постанова і які є додаткові на Порталі


# це завантаження ресурсів

setwd("C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/projects/open_data/postanova_835/clean data")

downloaded_id_data <- read.csv("ukraine_resources_progress_2025-07-02_234246.csv")

downloaded_id_data <- downloaded_id_data |>
  mutate(views_summary = as.integer(views_summary))

downloaded_vps_fun <- function(dotcsv) { 
  read.csv(dotcsv) |>
  as_tibble() |>
  select(-response_size) |>
  rename(timestamp = scraped_at) |>
  mutate(views_summary = str_extract(views_summary, '(?<=: ")\\d+'),
         views_summary = as.integer(views_summary)) 
}

vps1 <- downloaded_vps_fun('vps1_success - ukrainian_data_views_summary_20250709_082910.csv')
vps2 <- downloaded_vps_fun('vps2 - backup_results_6500.csv')
vps3 <- downloaded_vps_fun('vsp3_successful_views_summary_20250710_001942.csv')

# це набір із завантаженнями до рівня ресурсу
downloads_data <- rbind(vps1, downloaded_id_data, vps2, vps3) |>
  as_tibble() |>
  arrange(timestamp)

downloads_data |> distinct(resource_id) |> nrow()

downloads_data[downloads_data$resource_id == '3884be82-4f3a-4b44-8503-056ebdf475db', 
               'views_summary'] <- 0
downloads_data[downloads_data$resource_id == '3884be82-4f3a-4b44-8503-056ebdf475db', 
               'status'] <- 'success'

# це набір із завантаженнями до рівня набору
downloads_data_per_package <- 
  downloads_data |> 
  summarise(
    sum_views = sum(views_summary),
    .by = dataset_id
  ) |>
  arrange(desc(sum_views))


      # звожу кількість завантажень з основним + додаю перегляди

pack_views <- read_xlsx("packages_views_scrapted - 2025-07-02.xlsx") |>
  select(-c(status, url, timestamp)) # це перегляди

# з даних про ресурси добуваю 
pack_last_modified <- 
  resources_data |>
  summarise(
    last_update = max(last_modified),
    created = min(created),
    .by = dataset_id
  ) |> arrange(dataset_id)


fin1 <- 
required_orgs_all_packs_clean |>
  filter(status != 'відсутній') |>
  left_join(downloads_data_per_package, join_by(dataset_id)) |>
  left_join(pack_views, join_by(dataset_id)) |>
  left_join(pack_last_modified, join_by(dataset_id))

fin1 <- fin1 |>
  rbind(required_orgs_all_packs_clean |>
          filter(status == 'відсутній') |>
          mutate(sum_views = NA,
                 views_summary = NA,
                 last_update = NA,
                 created = NA)
  ) |> rename(n_downloads = sum_views, n_views = views_summary)

fin1 <- 
  fin1 |>
    mutate(last_update = as.Date(str_extract(last_update, '^.{10}')),
           created = as.Date(str_extract(created, '^.{10}')),
           last_update = if_else(!is.na(created) & is.na(last_update),
                                 created, last_update),
           date_collected = '2025-07-02') 

fin1 <- 
  fin1 |>
    mutate(updated_since_feb2022 = case_when(
      last_update >= '2022-02-24' ~ 'так',
      last_update < '2022-02-24' ~ 'ні',
      is.na(last_update) ~ NA
           ))

unique(fin1$update_frequency)

fin1 <- 
  fin1 |>
    mutate(
      days_since_last_update = as.integer(as.Date('2025-07-02') - last_update)
      )

fin1 <- 
  fin1 |>
    mutate(
      update_frequency_in_days = case_when(
        update_frequency == "immediately after making changes" ~ NA,
        update_frequency == "once a day" ~ 1,
        update_frequency == "once a month" ~ 31,
        update_frequency == "once a week" ~ 7,
        update_frequency == "once a year" ~ 364,
        update_frequency == "once a quarter" ~ 91,
        update_frequency == "once a half year" ~ 182,
        update_frequency == "no longer updated" ~ NA,
        update_frequency == "more than once a day" ~ 1,
        update_frequency == "immediately after making changes”" ~ NA,
        .default = NA
        )
    )

fin1 <- 
fin1 |>
  mutate(
    on_time_update = case_when(
      update_frequency_in_days < days_since_last_update ~ 'невчасно',
      update_frequency_in_days >= days_since_last_update ~ 'вчасно',
      str_detect(update_frequency, 'immediately after making') ~ 'оновлення після зміни даних',
      str_detect(update_frequency, 'no longer updated') ~ 'не оновлюється',
      status == 'відсутній' ~ 'набір відсутній'
    )
  )

# прибрати неспецифічні набори.

common <- read_xlsx('835 - organizations and required datasets_Oct24.xlsx') |>
  filter(str_detect(Розпорядник, '^Усі'))

fin1 |> nrow()

non_specific_to_remove <- 
  fin1 |>
    filter(status == 'додатковий')|> 
    filter(str_detect(available_dataset, 'інформац.+аудит') |
             str_detect(available_dataset, 'овідник.+організац') |
             str_detect(available_dataset, 'організаційн.+структур') |
             str_detect(available_dataset, 'стандарт.+технічн.+регламент') |
             str_detect(available_dataset, 'задоволен.+запит.+на.+інфо') |
             str_detect(available_dataset, 'набор.+дан.+володін') |
             str_detect(available_dataset, 'дміністративні.+дан') |
             str_detect(available_dataset, 'ерелік.+нормативн') |
             str_detect(available_dataset, 'ерелік.+регуляторн') |
             str_detect(available_dataset, 'лан.+підготовк.+проект.+регуляторн') |
             str_detect(available_dataset, '^(І|і)нформаці.+нормативн.+засад') |
             str_detect(available_dataset, 'інансов.+звітн.+суб.+господарюван') |
             str_detect(available_dataset, 'ічн.+звед.+фінансов.+показник') |
             str_detect(available_dataset, 'триман.+майно.+міжнар') |
             str_detect(available_dataset, 'нформац.+системи облік.+публічн.+інформац') |
             str_detect(available_dataset, 'нформац.+аудиту')
             ) |> distinct(dataset_id) |> as_vector()


# і треба додати обов'язкові набори тих, кого немає на порталі / 
  # у кого фактично немає публікацій там

fin1_needed_names <- names(fin1)

to_append <- 
req_datasets_jul25 |>
  filter(org %in% c('ТОВ “Оператор газотранспортної системи України”',
                    'АТ “Українська залізниця”',
                    'Національна академія наук',
                    'Рада міністрів Автономної Республіки Крим',
                    'Верховна Рада України',
                    'Рахункова палата',
                    'Держпродспоживслужба',
                    'Національне агентство із забезпечення якості вищої освіти',
                    'АТ “Національна акціонерна компанія “Нафтогаз України”',
                    'Аудиторська палата',
                    'Моторне (транспортне) страхове бюро'
  )) 

tibble_to_append <- matrix(ncol = length(fin1_needed_names), nrow = nrow(to_append))
colnames(tibble_to_append) <- fin1_needed_names
tibble_to_append <- as_tibble(tibble_to_append)

tibble_to_append$required_dataset <- to_append$dataset
tibble_to_append$org_names_postanova <- to_append$org
tibble_to_append$status <- 'відсутній'
tibble_to_append$date_collected <- '2025-07-02'

tibble_to_append <- tibble_to_append |> 
  select(-org_id) |>
  left_join(
  req_orgs_id_df |> filter(org_id %in% factually_absent_org),
  join_by(org_names_postanova == org)
)

fin1 <- rbind(fin1, tibble_to_append) |> as_tibble()

                # comparing 2024 Q4 data with 2025 Q2

q2_25 <- fin1 |> filter(!dataset_id %in% non_specific_to_remove)

setwd("C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/projects/open_data/postanova_835/clean data/antons_data")

excel_sheets("10.dataset_manual_processing_anton.xlsx")

q4_24 <- read_xlsx("full_dataset_manual_anton_corrected - 2025-07-23.xlsx", 
                       sheet = "10.6. Всі набори, дата, перег") 

q4_24 <- 
  q4_24 |> select(
    org_name_portal = "Назва розпорядника",
    required_dataset = "Назва обов'язкового датасету",
    available_dataset = "Назва доступного датасету",
    status = Статус,
    is_specific = "Специфічний чи обов'язковий для всіх",
    dataset_id,
    last_update = "Час останнього оновлення на 9 грудня",
    n_views = "Кількість переглядів",
    updated_since_feb2022 ="Оновлювали під час війни",
    num_resources,
    update_frequency,
    update_frequency_in_days = "Строк оновлення",
    days_since_last_update = "Днів з часу оновлення до 9 грудня 2024 року",
    on_time_update = "Чи вчасно оновили",
    n_downloads ="Кількість завантажень"
  )

q4_24 <- 
  q4_24 |>
    mutate(
      status = if_else(status == 'наявний' & is_specific == 'додатковий',
                       'додатковий',
                       status),
      is_specific = if_else(is_specific == 'додатковий',
                       'специфічний',
                       is_specific),
    )

# розпорядники центрального рівня з Постанови
length(unique(req_datasets_oct24$org))
length(unique(req_datasets_jul25$org)) # з'явився ДСНС

 # 1. як змінилася Постанова у частині вимог до публікації обов'язкових наборів
  
req_datasets_oct24 |> nrow() # 905 наборів
req_datasets_jul25 |> nrow() # 901 набір

req_datasets_oct24 |> count(dataset, org) |> filter(n > 1) # двічі державний реєстр судових експертів

req_datasets_oct24 <- req_datasets_oct24 |> distinct()

req_datasets_oct24 |> nrow() # 904 наборів

full_match <- 
  left_join(
    req_datasets_oct24,
    req_datasets_jul25 |> mutate(n = 1),
    join_by(dataset, org)
  ) |> 
    filter(!is.na(n)) |>
    select(-n)

check_diff <- 
  left_join(
    setdiff(
      req_datasets_oct24,
      req_datasets_jul25
    ), # це які були у 24 і немає у 25
    setdiff(
      req_datasets_jul25,
      req_datasets_oct24
    ),
    join_by(dataset)
  ) |>
  rename(org_jul25 = org.y, org_oct24 = org.x)

check_diff2 <- 
  left_join(
    setdiff(
      req_datasets_jul25,
      req_datasets_oct24
    ),
    setdiff(
      req_datasets_oct24,
      req_datasets_jul25
    ), # це які були у 24 і немає у 25
    join_by(dataset)
  ) |>
  rename(org_jul25 = org.x, org_oct24 = org.y)

org_change_only <- 
left_join(
  check_diff |> filter(!is.na(org_jul25)) |> select(-org_jul25), # 67
  check_diff2 |> filter(!is.na(org_oct24)) |> select(-org_oct24), # 67
  join_by(dataset)
)

absent_in_jul25 <- check_diff |> filter(is.na(org_jul25))
absent_in_oct24 <- check_diff2 |> filter(is.na(org_oct24))


setwd("C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/projects/open_data/postanova_835/clean data")
# тут треба передивитися
# writexl::write_xlsx(list(
#   'absent_in_jul25' = absent_in_jul25,
#   'absent_in_oct24' = absent_in_oct24,
#   'org_change_only' = org_change_only,
#   'full_match' = full_match
# ),
# paste0('changes_in_postanova_jul25_to_oct25 - ', Sys.Date(), '.xlsx'))


clean_req_compared <- read_xlsx('changes_in_postanova_jul25_to_oct25 - 2025-07-23_checked2.xlsx',
                                sheet = 'processed')

clean_req_compared |> count(type)

# опрацьовані дані
q4_24 |> filter(status != 'додатковий') |> nrow() # фактично є 904, як і має бути за Постановою
# причина - деякі фактичні набори розбиті на кілька
q4_24 |> filter(status != 'додатковий') |> 
  distinct(org_name_portal, required_dataset) |> nrow()
q4_24 |> filter(is.na(org_name_portal))

q4_24 |> filter(status != 'додатковий') |> arrange(org_name_portal, required_dataset)

q4_24 |> filter(status != 'додатковий') |> 
  count(org_name_portal, required_dataset) |> filter(n > 1) 
 
unique(q4_24$org_name_portal)

q2_25 |> filter(status != 'додатковий') |> nrow() # фактично є 894 /  має бути 901


# req_datasets_jul25 |>
#   left_join(
#     q2_25 |> filter(status != 'додатковий'),
#     join_by(dataset == required_dataset, org == org_names_postanova)
#   ) |> 
#   filter(is.na(status)) |>
#   writexl::write_xlsx('to_clean - 2025-07-23.xlsx')

to_append_q2_25 <- read_xlsx('to_clean - 2025-07-23_cleaned.xlsx')

str(q2_25)

q2_25 <- rbind(q2_25, to_append_q2_25) |> as_tibble()

q2_25 |> filter(status != 'додатковий') |> nrow() #  є 901

q2_25 |> distinct(org_name_portal, org_id) |> 
  arrange(org_name_portal) |> print(n = 73)


# ситуації, коли той самий id вказує на різні набори
q4_24 |> count(dataset_id) |> filter(n > 1 & !is.na(dataset_id)) # 17 id
q2_25 |> count(dataset_id) |> filter(n > 1 & !is.na(dataset_id)) # 16 id   


# перевірка унікалізація за id + назва набору
q4_24 |> count(dataset_id, required_dataset) |> filter(n > 1 & !is.na(dataset_id))
q2_25 |> count(dataset_id, required_dataset) |> filter(n > 1 & !is.na(dataset_id))

# це, де id асоційований з різними наборами
q2_25 |> count(dataset_id) |> arrange(desc(n)) |> filter(n > 1) |>
  filter(!is.na(dataset_id)) |>
  select(-n) |>
  left_join(q2_25, join_by(dataset_id))

req_orgs_absent # цих взагалі немає на Порталі
req_orgs_id_df |> filter(org_id %in% factually_absent_org) # ці створені, але без наборів 


# порівнюю динаміку по наборам
q4_24 |> 
  filter(status != 'додатковий') |> 
  count(org_name_portal) |> 
  arrange(desc(n)) |> print(n = 15)

q2_25 |> 
  filter(status != 'додатковий') |> 
  count(org_names_postanova) |> 
  arrange(desc(n)) |> print(n = 15)

length(unique(q2_25$org_name_portal))
length(unique(q2_25$org_names_postanova))
length(unique(q4_24$org_name_portal))




setwd("C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/projects/open_data/postanova_835/clean data")

# writexl::write_xlsx(
#   list(
#     'q4_24' = q4_24 |> 
#       filter(status != 'додатковий') |> 
#       count(org_name_portal) |> 
#       arrange(desc(n)),
#     'q2_25' = q2_25 |> 
#       filter(status != 'додатковий') |> 
#       count(org_names_postanova, org_name_portal, org_id) |> 
#       arrange(desc(n)) 
#   ), paste0('comparing orgs data - oct24 to jul25 - ', Sys.Date(), '.xlsx')
# )

aligned_names <- read_xlsx('comparing orgs data - oct24 to jul25 - 2025-07-18_processed.xlsx',
                           sheet = 'merged')

aligned_names |> names()

aligned_names <- 
  aligned_names |>
    mutate(
      aux = if_else(is.na(org_name_portal_jul25),
                    org_names_postanova_jul25,
                    org_name_portal_jul25)
    )


# з обов'язковими наборами розібрався, тепер треба з фактичними

q2_25 |> filter(status == 'наявний')
q4_24 |> filter(status == 'наявний')

q2_25[q2_25$org_names_postanova == 
        'Моторне (транспортне) страхове бюро', 'org_id'] <- "temp1"
q2_25[q2_25$org_names_postanova == 
        'Національна академія наук', 'org_id'] <- "temp2"
q2_25[q2_25$org_names_postanova == 
        'Рада міністрів Автономної Республіки Крим', 'org_id'] <- "temp3"
q2_25[q2_25$org_names_postanova == 
        'ТОВ “Оператор газотранспортної системи України”', 'org_id'] <- "temp4"
q2_25[q2_25$org_names_postanova == 
        'Верховна Рада України', 'org_id'] <- "temp5"

q2_25 <- 
  q2_25 |>
    left_join(
      aligned_names,
      join_by(org_id)
    ) |> select(-c(org_name_oct24, org_name_portal_jul25, 
                   org_names_postanova_jul25))


# Антон загубив ДФС, виправив в екселі

q4_24 |> filter(status == 'наявний')
q2_25 |> filter(status == 'наявний')

# тепер обидва з org_id
q4_24 <- 
  q4_24 |>
    left_join(
      aligned_names |> select(aux, org_id),
      join_by(org_name_portal == aux)
    )

q4_24 |> filter(status == 'наявний')
q2_25 |> filter(status != 'додатковий') |> arrange(org_name_portal, required_dataset)

q2_25 |> filter(status != 'додатковий') |> filter(str_detect(org_name_portal, 'трансп'))

q4_24 |> count(status, is_specific)

present_in_oct_24 <- 
  anti_join(
    q4_24 |> filter(status == 'наявний'),
    q2_25 |> filter(status == 'наявний'),
    join_by(org_id, dataset_id)
  ) 

names(present_in_oct_24)

present_in_jul_25 <- 
  anti_join(
    q2_25 |> filter(status == 'наявний'),
    q4_24 |> filter(status == 'наявний'),
    join_by(org_id, dataset_id)
  ) 
# 
# writexl::write_xlsx(
#   list(
#     'present_in_oct_24' = present_in_oct_24,
#     'present_in_jul_25' = present_in_jul_25
#   ), 'compare_req_factual_datasets_two_periods - 2025-07-24.xlsx'
# )

excel_sheets("compare_req_factual_datasets_two_periods - 2025-07-24_compared.xlsx")

act_present_in_oct_24 <- read_xlsx("compare_req_factual_datasets_two_periods - 2025-07-24_compared.xlsx",
                                   sheet = 'present_in_oct_24') 

act_present_in_oct_24 |> count(org_name_portal, required_dataset) |> filter(n > 1)

q4_24 |> nrow() - nrow(act_present_in_oct_24)

sort(names(act_present_in_oct_24))
sort(names(q4_24))

q4_24 <- 
  anti_join(
    q4_24,
    act_present_in_oct_24 |> select(org_name_portal, required_dataset),
    join_by(org_name_portal, required_dataset)
  ) |> 
    rbind(act_present_in_oct_24) |>
  as_tibble()

# inner_join(
#   q4_24,
#   act_present_in_oct_24 |> select(org_name_portal, required_dataset),
#   join_by(org_name_portal, required_dataset)
# ) |> count(org_name_portal, required_dataset) |> filter(n > 1)

act_present_in_jul_25 <- read_xlsx("compare_req_factual_datasets_two_periods - 2025-07-24_compared.xlsx",
                                   sheet = 'present_in_jul_25')

act_present_in_jul_25 |> count(org_name_portal, required_dataset) |> filter(n > 1)

q2_25 |> nrow() - nrow(act_present_in_jul_25)

q2_25 <- 
  anti_join(
    q2_25,
    act_present_in_jul_25 |> select(org_name_portal, required_dataset),
    join_by(org_name_portal, required_dataset)
  ) |> 
    rbind(act_present_in_jul_25) |>
    as_tibble()


q2_25 |> filter(status != 'додатковий') |> count(status)
nrow(q2_25[q2_25$status == 'відсутній',]) + nrow(q2_25[q2_25$status == 'наявний',])

q4_24 |> filter(status != 'додатковий') |> count(status) 
nrow(q4_24[q4_24$status == 'відсутній',]) + nrow(q4_24[q4_24$status == 'наявний',])

q4_24 <- 
  q4_24 |>
    mutate(
      org_id = if_else(required_dataset == 'Реєстр складських документів на зерно та зерна, прийнятого на зберігання',
                       'c9edbe3d-0a05-4d00-b23b-7c1fe5810ca4',
                       org_id)
    )

# останні перевірки
q2_25 |> filter(status != 'додатковий') |> 
  distinct(org_name_portal, required_dataset) |> nrow()

q4_24 |> filter(status != 'додатковий') |> 
  distinct(org_name_portal, required_dataset) |> nrow()


q2_25 |> filter(status != 'додатковий') |> 
  distinct(required_dataset, org_name_portal) |> nrow()

q4_24 |> filter(status != 'додатковий') |> 
  distinct(required_dataset, org_name_portal) |> nrow()

req_datasets_oct24 |> count(dataset) |> filter(n > 1)


q4_24 |> filter(status != 'додатковий') |>
  count(org_id) |>
  rename(n_oct24 = n)

compared_by_amount_all_packages <- 
left_join(
  q2_25 |> filter(status != 'додатковий') |>
    count(org_id) |>
    rename(n_jul25 = n),
  q4_24 |> filter(status != 'додатковий') |>
    count(org_id) |>
    rename(n_oct24 = n),
  join_by(org_id)
) |>
  mutate(across(starts_with('n_'), ~ replace_na(.x, 0))) |>
  left_join(aligned_names |> select(org_id, org_names_postanova_jul25), 
            join_by(org_id))

compared_by_amount_all_packages |>
  mutate(if_match = n_jul25 == n_oct24) |>
  filter(if_match == FALSE) |> 
  select(-c(if_match, org_id)) |>
  relocate(org_names_postanova_jul25, .before = everything()) |> 
  mutate(delta = (n_jul25 - n_oct24))

compared_by_status <- 
left_join(
  aligned_names,
  q4_24 |> 
    filter(status != 'додатковий') |> 
    count(org_name_portal, status) |>
    rename(n_oct24 = n),
  join_by(aux == org_name_portal)) |> 
  left_join(
    q2_25 |> 
      filter(status != 'додатковий') |> 
      count(org_names_postanova, status) |>
      rename(n_jul25 = n),
    join_by(org_names_postanova_jul25 == org_names_postanova, status))

compared_by_status |> 
  mutate(across(where(is.numeric), ~ replace_na(.x, 0))) |>
  summarise(across(where(is.numeric), sum))

compared_by_status |>
  select(org_names_postanova_jul25, status, starts_with('n')) |> 
  arrange(org_names_postanova_jul25, status) 

anti_join(
  q2_25 |> filter(status != 'додатковий'),
  q4_24 |> filter(status != 'додатковий'),
  join_by(required_dataset)
)


# preparing data for the dashboard
setwd("C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/projects/open_data/postanova_835/clean data/antons_data")



haluzi <- read_xlsx('full_dataset_manual_anton_corrected - 2025-07-23.xlsx',
                    sheet = 'галузі') |>
  rename(org_name_portal = 1, sector = 2)


q2_25 <- 
  q2_25 |>
    left_join(aligned_names |> select(org_id, aux),
              join_by(org_id)) |> 
    select(-aux.x) |> rename(aux = aux.y) 

names(q2_25)

# q2_25_prep <- 
#   q2_25 |>
#   select(
#     "Дата останнього оновлення" = last_update,
#     "Назва доступного набору" = available_dataset,
#     "Назва обов'язкового набору" = required_dataset,
#     dataset_id,
#     "Назва розпорядника" = org_names_postanova,
#     "Статус" = status,
#     "Чи вчасно оновили" = on_time_update,
#     "Кількість завантажень" = n_downloads 
#   )

prep_names <- c('org_name_portal', 'org_id', 
                'required_dataset', 'available_dataset', 'dataset_id', 
                'status', 'num_resources', 'n_views', 'created', 'update_frequency',
                'last_update', 'on_time_update', 'updated_since_feb2022',
                'n_downloads', 'date_collected')

q2_25 <- q2_25 |> 
  select(-org_name_portal) |>
  rename(org_name_portal = aux)

q4_24 <- 
  q4_24 |> 
    mutate(date_collected = '2024-12-09',
           created = NA)

q4_24_prep <- q4_24 |> select(all_of(prep_names)) 

q2_25_prep <- q2_25 |> select(all_of(prep_names))

table(names(q4_24_prep) == names(q2_25_prep))


q4_24_prep |> distinct(org_name_portal, org_id) |>
  count(org_id) |> filter(n > 1)

q4_24_prep |> filter(org_id == 'c9edbe3d-0a05-4d00-b23b-7c1fe5810ca4') |> 
  distinct(org_name_portal) |> pluck(1) |> pluck(2) ==
q2_25 |> filter(org_id == 'c9edbe3d-0a05-4d00-b23b-7c1fe5810ca4') |> 
  distinct(org_name_portal) |>
  pluck(1)

q4_24_prep <- 
  q4_24_prep |> 
  mutate(org_name_portal = if_else(
    org_name_portal == 
      q4_24_prep |> filter(org_id == 'c9edbe3d-0a05-4d00-b23b-7c1fe5810ca4') |> 
      distinct(org_name_portal) |> pluck(1) |> pluck(2),
    q4_24_prep |> filter(org_id == 'c9edbe3d-0a05-4d00-b23b-7c1fe5810ca4') |> 
      distinct(org_name_portal) |> pluck(1) |> pluck(1),
    org_name_portal))

unique(q4_24_prep$org_name_portal)

sort(unique(q2_25_prep$org_name_portal)) == sort(c(unique(q4_24_prep$org_name_portal),
                                                   "Державна служба України з надзвичайних ситуацій"))

dataset_id_to_fill_with_update_data <- 
  q2_25_prep |> filter(is.na(on_time_update)) |> 
  filter(status != 'відсутній') |>
  select(dataset_id) |> as_vector()


fin1_extra_data <- 
  fin1 |>
    filter(
      dataset_id %in% dataset_id_to_fill_with_update_data
    ) |>
    filter(!is.na(on_time_update))


names(q2_25_prep)

str(q2_25_prep)
str(fin1_extra_data)

q2_25_prep2 <- 
q2_25_prep |>
  left_join(fin1_extra_data, by = "dataset_id", suffix = c("", "_small")) |>
  mutate(
    last_update = as.Date(ifelse(is.na(last_update) | last_update == "", 
                         last_update_small, last_update)),
    created = as.Date(ifelse(is.na(created), 
                     created_small, created)),
    on_time_update = ifelse(is.na(on_time_update), 
                            on_time_update_small, on_time_update),
    updated_since_feb2022 = ifelse(is.na(updated_since_feb2022), 
                                   updated_since_feb2022_small, on_time_update)
  ) |>
  select(-c(ends_with("_small"), org_names_postanova,
            days_since_last_update, update_frequency_in_days))

str(q2_25_prep2)

q2_25_prep |> filter(status != 'додатковий') |> nrow()

q2_25_prep <- 
  q2_25_prep |> mutate(
    on_time_update = if_else(status == 'відсутній',
                             'набір відсутній',
                             on_time_update)
  )

setwd("C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/projects/open_data/postanova_835/clean data")


q2_25_prep |>
  filter(status != 'додатковий') |>
  count(org_name_portal, status) |>
  count(org_name_portal) |>
  filter(n == 1) |>
  left_join(
    q2_25_prep |>
      filter(status != 'додатковий') |>
      count(org_name_portal, status),
    join_by(org_name_portal)
  ) |>
  filter(status == 'наявний')

q2_25_prep |>
  filter(status != 'додатковий') |>
  count(org_name_portal, on_time_update) |> 
  count(org_name_portal) |>
  filter(n < 2) |>
  left_join(
    q2_25_prep |>
      filter(status != 'додатковий') |>
      filter(on_time_update == 'вчасно') |>
      count(org_name_portal, on_time_update),
    join_by(org_name_portal)
  ) |> filter(!is.na(on_time_update))

q2_25_prep |>
  filter(status != 'додатковий') |>
  count(org_name_portal) |>
  arrange(desc(n))


q2_25_prep |>
  filter(status != 'додатковий') |>
  filter(on_time_update == 'вчасно') |>
  count(org_name_portal) |>
  arrange(desc(n)) 



  
# writexl::write_xlsx(
#   list(
#     'q2_25_prep' = q2_25_prep,
#     'q4_24_prep' = q4_24_prep,
#     'sectors' = haluzi
#   ), paste0('q2_25_and_q4_24_prepared - ', Sys.Date(), '.xlsx')
# )





