from transformers import AutoTokenizer, AutoModel
import torch
import torch.nn.functional as F
import numpy as np
import flask
import json
from flask import Flask, request, jsonify, make_response

app = Flask(__name__)
port = 4242
tokenizer = AutoTokenizer.from_pretrained('sentence-transformers/all-MiniLM-L6-v2')
model = AutoModel.from_pretrained('sentence-transformers/all-MiniLM-L6-v2')

#Mean Pooling - Take attention mask into account for correct averaging
def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[0] #First element of model_output contains all token embeddings
    input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(input_mask_expanded.sum(1), min=1e-9)

def generate_embeddings(sentence):
	# Tokenize sentence
	encoded_input = tokenizer(sentence, padding=True, truncation=True, return_tensors='pt')

	# Compute token embeddings
	with torch.no_grad():
		model_output = model(**encoded_input)

	# Perform pooling
	sentence_embeddings = mean_pooling(model_output, encoded_input['attention_mask'])

	# Normalize embeddings
	sentence_embeddings = F.normalize(sentence_embeddings, p=2, dim=1)
	return sentence_embeddings.tolist(), sentence_embeddings.shape

@app.route("/api/v1/embeddings", methods=["POST"])
def fetch_embedding():
    data = request.get_json()
    sentence = data['sentence']
    if isinstance(sentence, str):
        sentence = [sentence]
    embedding, shape = generate_embeddings(sentence)
    return jsonify({"embeddings": embedding[0], "dim": shape[1]})

if __name__ == "__main__":
	app.debug = True
	app.run('0.0.0.0', port = port, threaded=True)
