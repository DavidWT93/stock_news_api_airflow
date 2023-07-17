# -*- coding: utf-8 -*-
"""
Created on Sat Nov 19 15:42:46 2022

@author: david
"""
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import transformers

tokenizer = AutoTokenizer.from_pretrained("assemblyai/distilbert-base-uncased-sst2")
model = AutoModelForSequenceClassification.from_pretrained("assemblyai/distilbert-base-uncased-sst2")


def calculate_sentiment_using_bert(text):
    """
    return:
    sentiment of text
    """
    tokenized_segments = tokenizer([f'{text}'], return_tensors="pt", padding=True, truncation=True)
    tokenized_segments_input_ids, tokenized_segments_attention_mask = tokenized_segments.input_ids, tokenized_segments.attention_mask
    model_predictions = F.softmax(
        model(input_ids=tokenized_segments_input_ids, attention_mask=tokenized_segments_attention_mask)['logits'],
        dim=1)

    return model_predictions[0][1].item() * 100


def add_column_with_sentiment_to_df(DF, nameOfColumnWithText, newColumnWithSentimentName="sentiment",
                                    startAtColumnIfCrashed=0, saveToPklFileAsSafety="dontSaveProgress"):
    DF[newColumnWithSentimentName] = " "
    DFLength = len(DF)
    for i in range(startAtColumnIfCrashed, DFLength):
        text = DF[nameOfColumnWithText].iloc[i]
        print(calculate_sentiment_using_bert(text))
        DF[newColumnWithSentimentName].iloc[i] = calculate_sentiment_using_bert(text)
        print(i, " of ", DFLength, " sentiments calculated")
        # if wanted the progress can be saved to a pickle file for every 10000 calculations incase the programm crashes
        if saveToPklFileAsSafety != "dontSaveProgress":
            if i % 10000 == 0:
                print("restart at ", i, " if crashed")
                DF.to_pickle(saveToPklFileAsSafety)
    return DF


