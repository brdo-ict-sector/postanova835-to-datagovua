library(rvest)
library(httr)
library(dplyr)
library(purrr)
library(tibble)

# Function to scrape views_summary from a single dataset page
scrape_views_summary <- function(dataset_id, base_url = 'https://data.gov.ua/dataset/') {
  
  # Construct full URL
  full_url <- paste0(base_url, dataset_id)
  
  # Add delay to be respectful to the server
  Sys.sleep(1)
  
  # Try to scrape the page
  tryCatch({
    # Read the HTML page
    page <- read_html(full_url)
    
    # Extract views_summary using the ID selector
    views_summary <- page %>%
      html_element("#views_summary") %>%
      html_text(trim = TRUE)
    
    # If views_summary is NULL or empty, try alternative extraction
    if (is.na(views_summary) || views_summary == "") {
      # Try to find by text pattern (as backup)
      views_text <- page %>%
        html_elements("p") %>%
        html_text() %>%
        str_extract("\\d+") %>%
        na.omit() %>%
        .[1]  # Take first number found
      
      views_summary <- ifelse(length(views_text) > 0, views_text, NA)
    }
    
    # Return result
    return(data.frame(
      dataset_id = dataset_id,
      url = full_url,
      views_summary = views_summary,
      status = "success",
      timestamp = Sys.time(),
      stringsAsFactors = FALSE
    ))
    
  }, error = function(e) {
    # Return error information
    return(data.frame(
      dataset_id = dataset_id,
      url = full_url,
      views_summary = NA,
      status = paste("error:", e$message),
      timestamp = Sys.time(),
      stringsAsFactors = FALSE
    ))
  })
}

# Function to scrape multiple datasets with progress tracking
scrape_multiple_datasets <- function(dataset_ids, base_url = 'https://data.gov.ua/dataset/', 
                                     batch_size = 50, save_progress = TRUE) {
  
  total_datasets <- length(dataset_ids)
  all_results <- list()
  
  cat("Starting to scrape", total_datasets, "datasets...\n")
  
  # Process in batches
  for (i in seq(1, total_datasets, by = batch_size)) {
    
    # Determine batch range
    end_idx <- min(i + batch_size - 1, total_datasets)
    batch_ids <- dataset_ids[i:end_idx]
    
    cat("Processing batch", ceiling(i/batch_size), "- datasets", i, "to", end_idx, "\n")
    
    # Scrape current batch
    batch_results <- map_dfr(batch_ids, ~scrape_views_summary(.x, base_url))
    
    # Store results
    all_results[[length(all_results) + 1]] <- batch_results
    
    # Save progress if requested
    if (save_progress) {
      temp_results <- bind_rows(all_results)
      write.csv(temp_results, paste0("scraping_progress_", Sys.Date(), ".csv"), row.names = FALSE)
    }
    
    # Print progress
    cat("Completed", end_idx, "out of", total_datasets, "datasets\n")
    cat("Success rate so far:", 
        round(sum(temp_results$status == "success") / nrow(temp_results) * 100, 1), "%\n\n")
  }
  
  # Combine all results
  final_results <- bind_rows(all_results)
  
  return(final_results)
}

# For testing with a small sample first:
 test_ids <- c("8b06c10f-d542-452a-a88d-45e6315b1bfc", "1c13f2f1-8bc4-41d4-81dc-ea4527fa73de")
 test_results <- scrape_multiple_datasets(test_ids, batch_size = 2)

# For your full dataset:
 
 # !!!! ids are from full scale deployment preps.R file
 
scrapte_results <- scrape_multiple_datasets(needed_packages_ids, batch_size = 50)

scrapte_results <- scrapte_results |> 
  mutate(views_summary = as.numeric(views_summary)) |> arrange(desc(views_summary)) 

setwd("C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/projects/open_data/postanova_835/clean data")

# writexl::write_xlsx(list('packages_views_scrapted' = scrapte_results),
#                     paste0("packages_views_scrapted - ", Sys.Date(), ".xlsx"))


# Analyze results
analyze_results <- function(results) {
  cat("=== SCRAPING RESULTS SUMMARY ===\n")
  cat("Total datasets processed:", nrow(results), "\n")
  cat("Successful scrapes:", sum(results$status == "success"), "\n")
  cat("Failed scrapes:", sum(results$status != "success"), "\n")
  cat("Success rate:", round(sum(results$status == "success") / nrow(results) * 100, 1), "%\n\n")
  
  # Show sample of successful results
  successful_results <- results[results$status == "success" & !is.na(results$views_summary), ]
  if (nrow(successful_results) > 0) {
    cat("Sample of successful results:\n")
    print(head(successful_results[, c("dataset_id", "views_summary")], 10))
  }
  
  # Show error summary
  error_results <- results[results$status != "success", ]
  if (nrow(error_results) > 0) {
    cat("\nError summary:\n")
    error_summary <- table(error_results$status)
    print(error_summary)
  }
}

# Helper function to cl