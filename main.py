from data_pipeline import DataPipeline

# Create an object to handle all data st
data_pipeline = DataPipeline()

# # Step 1: Data collection
data_pipeline.collect_data(2022, "candidates", redownload=False)
data_pipeline.collect_data(2022, "social_media", redownload=False)

# # Step 2: Data preprocessing
data_pipeline.transform_data()

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


# to-do: visualize data with Tableau, PowerBI or Microsoft Excel
# download multiple stack clipboards