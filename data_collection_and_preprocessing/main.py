from data_pipeline import DataPipeline

# Create an object to handle all data st
data_pipeline = DataPipeline()

# # Step 1: Get data from the TSE website, about the candidates, and the election results
data_pipeline.collect_data(2022, "candidates", redownload=False)
data_pipeline.collect_data(2022, "social_media", redownload=False)
data_pipeline.collect_data(2022, "voting_section", redownload=False)

# # Step 2: Data exploration with SQL, clean data (remove useless columns),
# merge data into one database, with each row representing a candidate
data_pipeline.transform_to_sql_tables(done=True)

# scrap social media in order to more data for each candidate 
# (like candidate social media engagement, google trends data about the candidate, etc.)

#  Data exploration (again) with SQL, clean data (remove useless columns),
# merge data into one database, with each row representing a candidate



#--------------------------------------------------------------------------------------


# # Step 3: Model training
# model = model_training.train_model()

# # Step 4: Model evaluation
# model_evaluation.evaluate_model(model)

# # Step 5: Prediction
# prediction.make_prediction(model)


# to-do: turn .csv files into pandas or python objects
# to-do: remove accents from names
# to-do: join data and exclude useless columns
# to-do: do web scrapping (use beatiful soup)
# to-do: use selenium to automate the web scrapping
# to-do: use the Twitter API to get the tweets
# to-do: use the Facebook API to get the posts
# to-do: use the Instagram API to get the posts
# to-do: preprocess the data
# to-do: feature engineering


# download multiple stack clipboards