from data_collection import get_candidates, get_social_media

# import data_preprocessing
# import model_training
# import model_evaluation
# import prediction

# # Step 1: Data collection
# data_collection.collect_data()

# # Step 2: Data preprocessing
# data_preprocessing.preprocess_data()

# # Step 3: Model training
# model = model_training.train_model()

# # Step 4: Model evaluation
# model_evaluation.evaluate_model(model)

# # Step 5: Prediction
# prediction.make_prediction(model)

get_candidates(2022, redownload=True)
get_social_media(2022, redownload=True)