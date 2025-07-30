# Load required libraries
library(rvest)
library(httr)
library(dplyr)
library(purrr)
library(tibble)
library(stringr)

# Function to scrape views_summary from a single resource page
scrape_resource_views_summary <- function(dataset_id, resource_id, base_url = 'https://data.gov.ua/dataset/') {
  
  # Construct full URL for resource page
  full_url <- paste0(base_url, dataset_id, "/resource/", resource_id)
  
  # Add delay to be respectful to the server (reduced for large dataset)
  Sys.sleep(0.5)
  
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
      resource_id = resource_id,
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
      resource_id = resource_id,
      url = full_url,
      views_summary = NA,
      status = paste("error:", e$message),
      timestamp = Sys.time(),
      stringsAsFactors = FALSE
    ))
  })
}

# Function to scrape multiple resources with progress tracking
scrape_multiple_resources <- function(dataset_ids, resource_ids, base_url = 'https://data.gov.ua/dataset/', 
                                      batch_size = 100, save_progress = TRUE, 
                                      progress_file = NULL) {
  
  # Validate input vectors have same length
  if (length(dataset_ids) != length(resource_ids)) {
    stop("dataset_ids and resource_ids must have the same length")
  }
  
  total_resources <- length(dataset_ids)
  all_results <- list()
  
  # Set up progress file name
  if (is.null(progress_file)) {
    progress_file <- paste0("resource_scraping_progress_", Sys.Date(), "_", 
                            format(Sys.time(), "%H%M%S"), ".csv")
  }
  
  cat("Starting to scrape", total_resources, "resources...\n")
  cat("Progress will be saved to:", progress_file, "\n\n")
  
  # Process in batches
  for (i in seq(1, total_resources, by = batch_size)) {
    
    # Determine batch range
    end_idx <- min(i + batch_size - 1, total_resources)
    batch_dataset_ids <- dataset_ids[i:end_idx]
    batch_resource_ids <- resource_ids[i:end_idx]
    
    cat("Processing batch", ceiling(i/batch_size), "- resources", i, "to", end_idx, "\n")
    start_time <- Sys.time()
    
    # Scrape current batch using map2 for paired iteration
    batch_results <- map2_dfr(batch_dataset_ids, batch_resource_ids, 
                              ~scrape_resource_views_summary(.x, .y, base_url))
    
    # Store results
    all_results[[length(all_results) + 1]] <- batch_results
    
    # Combine results so far
    temp_results <- bind_rows(all_results)
    
    # Save progress if requested
    if (save_progress) {
      write.csv(temp_results, progress_file, row.names = FALSE)
    }
    
    # Calculate timing
    end_time <- Sys.time()
    batch_duration <- as.numeric(end_time - start_time, units = "secs")
    
    # Print progress
    cat("Completed", end_idx, "out of", total_resources, "resources\n")
    success_count <- sum(temp_results$status == "success")
    cat("Success rate so far:", 
        round(success_count / nrow(temp_results) * 100, 1), "%\n")
    cat("Batch duration:", round(batch_duration, 1), "seconds\n")
    
    # Estimate remaining time
    if (end_idx < total_resources) {
      remaining_batches <- ceiling((total_resources - end_idx) / batch_size)
      estimated_time <- remaining_batches * batch_duration / 60
      cat("Estimated remaining time:", round(estimated_time, 1), "minutes\n")
    }
    cat("\n")
    
    # Optional: pause between large batches to be extra respectful
    if (batch_size >= 100 && end_idx < total_resources) {
      cat("Pausing 10 seconds between large batches...\n")
      Sys.sleep(10)
    }
  }
  
  # Combine all results
  final_results <- bind_rows(all_results)
  
  return(final_results)
}

# Function to create paired vectors from data frame (if you have data in tabular format)
prepare_id_vectors <- function(df, dataset_col = "dataset_id", resource_col = "resource_id") {
  return(list(
    dataset_ids = df[[dataset_col]],
    resource_ids = df[[resource_col]]
  ))
}

# Function to analyze results specifically for resources
analyze_resource_results <- function(results) {
  cat("=== RESOURCE SCRAPING RESULTS SUMMARY ===\n")
  cat("Total resources processed:", nrow(results), "\n")
  cat("Successful scrapes:", sum(results$status == "success"), "\n")
  cat("Failed scrapes:", sum(results$status != "success"), "\n")
  cat("Success rate:", round(sum(results$status == "success") / nrow(results) * 100, 1), "%\n\n")
  
  # Show sample of successful results
  successful_results <- results[results$status == "success" & !is.na(results$views_summary), ]
  if (nrow(successful_results) > 0) {
    cat("Sample of successful results:\n")
    print(head(successful_results[, c("dataset_id", "resource_id", "views_summary")], 10))
    
    # Show views statistics
    views_numeric <- as.numeric(gsub("[^0-9]", "", successful_results$views_summary))
    views_numeric <- views_numeric[!is.na(views_numeric)]
    
    if (length(views_numeric) > 0) {
      cat("\nViews statistics:\n")
      cat("Min views:", min(views_numeric), "\n")
      cat("Max views:", max(views_numeric), "\n")
      cat("Mean views:", round(mean(views_numeric), 1), "\n")
      cat("Median views:", median(views_numeric), "\n")
    }
  }
  
  # Show unique datasets covered
  unique_datasets <- length(unique(results$dataset_id))
  cat("Unique datasets covered:", unique_datasets, "\n")
  
  # Show error summary
  error_results <- results[results$status != "success", ]
  if (nrow(error_results) > 0) {
    cat("\nError summary:\n")
    error_summary <- table(error_results$status)
    print(error_summary)
  }
}

# Helper function to clean and convert views to numeric
clean_resource_views_data <- function(results) {
  results$views_numeric <- as.numeric(gsub("[^0-9]", "", results$views_summary))
  return(results)
}

# Function to resume scraping from a saved progress file
resume_scraping <- function(dataset_ids, resource_ids, progress_file, batch_size = 100) {
  
  # Load existing progress
  if (file.exists(progress_file)) {
    existing_results <- read.csv(progress_file, stringsAsFactors = FALSE)
    cat("Found existing progress file with", nrow(existing_results), "records\n")
    
    # Find which resources still need to be processed
    completed_pairs <- paste(existing_results$dataset_id, existing_results$resource_id, sep = "_")
    all_pairs <- paste(dataset_ids, resource_ids, sep = "_")
    
    remaining_indices <- which(!all_pairs %in% completed_pairs)
    
    if (length(remaining_indices) == 0) {
      cat("All resources already processed!\n")
      return(existing_results)
    }
    
    cat("Found", length(remaining_indices), "remaining resources to process\n")
    
    # Continue with remaining resources
    remaining_dataset_ids <- dataset_ids[remaining_indices]
    remaining_resource_ids <- resource_ids[remaining_indices]
    
    new_results <- scrape_multiple_resources(remaining_dataset_ids, remaining_resource_ids, 
                                             batch_size = batch_size, 
                                             progress_file = progress_file)
    
    # Combine with existing results
    final_results <- rbind(existing_results, new_results)
    return(final_results)
    
  } else {
    cat("No existing progress file found, starting fresh\n")
    return(scrape_multiple_resources(dataset_ids, resource_ids, batch_size = batch_size, 
                                     progress_file = progress_file))
  }
}

# Main function to run the complete resource scraping process
run_complete_resource_scraping <- function(dataset_ids, resource_ids, test_size = 10) {
  
  cat("Starting complete resource scraping process...\n")
  cat("Total resources to process:", length(dataset_ids), "\n\n")
  
  # Test with first few resources
  cat("Testing with first", test_size, "resources...\n")
  test_results <- scrape_multiple_resources(dataset_ids[1:test_size], resource_ids[1:test_size], 
                                            batch_size = test_size, save_progress = FALSE)
  analyze_resource_results(test_results)
  
  # If test looks good, proceed with full scraping
  success_rate <- sum(test_results$status == "success") / nrow(test_results)
  
  if (success_rate >= 0.6) {  # At least 60% success rate
    cat("\nTest successful (", round(success_rate * 100, 1), "% success rate), proceeding with full scraping...\n")
    
    # Create progress file name
    progress_file <- paste0("ukraine_resources_progress_", Sys.Date(), "_", 
                            format(Sys.time(), "%H%M%S"), ".csv")
    
    full_results <- scrape_multiple_resources(dataset_ids, resource_ids, 
                                              batch_size = 100, 
                                              progress_file = progress_file)
    
    # Clean and analyze results
    full_results <- clean_resource_views_data(full_results)
    analyze_resource_results(full_results)
    
    # Save final results
    final_filename <- paste0("ukraine_resources_views_final_", Sys.Date(), "_", 
                             format(Sys.time(), "%H%M%S"), ".csv")
    write.csv(full_results, final_filename, row.names = FALSE)
    cat("Final results saved to:", final_filename, "\n")
    
    return(full_results)
  } else {
    cat("Test failed (", round(success_rate * 100, 1), "% success rate), please check your data and try again.\n")
    return(test_results)
  }
}


resources_data


dataset_ids <- as_vector(df_packages_resources$dataset_id)
resource_ids <- as_vector(df_packages_resources$resource_id)

setwd("C:/Users/MykolaKuzin/OneDrive - NGO 'BETTER REGULATION DELIVERY OFFICE'/Desktop/projects/open_data/postanova_835/clean data")

# Run the complete process:
results <- run_complete_resource_scraping(dataset_ids, resource_ids)

# Or if you want to resume from a previous attempt:
results <- resume_scraping(dataset_ids, resource_ids, "existing_progress_file.csv")
