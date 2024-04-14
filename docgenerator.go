package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/jaswdr/faker"
)

type embeddings struct {
	Vectors []float32 `json:"embeddings"`
	Dim     int       `json:"dim"`
}

type document struct {
	Car          string
	Model        string
	Fuel         string
	Type         string
	Year         int
	Transmission string
	Color        string
	Vector       []float32
	Dim          int
}

var mu sync.Mutex

func fetchEmbeddings(sentence string) ([]float32, int, error) {
	httpClient := http.Client{}
	content := map[string]interface{}{
		"sentence": sentence,
	}
	buf, err := json.Marshal(content)
	if err != nil {
		return nil, -1, err
	}
	bufReader := bytes.NewReader(buf)

	req, err := http.NewRequest("POST", "http://localhost:4242/api/v1/embeddings", bufReader)
	if err != nil {
		return nil, -1, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, -1, err
	}
	defer resp.Body.Close()
	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, -1, err
	}
	var vectors embeddings
	err = json.Unmarshal(respBytes, &vectors)
	if err != nil {
		return nil, -1, err
	}

	return vectors.Vectors, vectors.Dim, nil
}

func generateDocument() document {

	var fake = faker.New()
	var goFakeIt = gofakeit.New(0)

	var doc document
	doc.Car = goFakeIt.Car().Brand
	doc.Model = goFakeIt.Car().Model
	doc.Fuel = goFakeIt.Car().Fuel
	doc.Type = goFakeIt.Car().Type
	doc.Year = goFakeIt.Car().Year
	doc.Transmission = goFakeIt.Car().Transmission
	doc.Color = fake.Color().SafeColorName()
	var vec []float32
	var dim int
	var err error
	for retry := 0; retry < 5; retry++ {
		vec, dim, err = fetchEmbeddings(doc.Color)
		if err != nil {
			time.Sleep(2 * time.Second)
		} else {
			err = nil
			break
		}
	}
	if err != nil {
		fmt.Printf("Error retrieving vector embeddings %v\n", err)
	}

	doc.Vector = vec
	doc.Dim = dim
	return doc
}

func buildVectors(documents *[]document, wg *sync.WaitGroup) {
	docObj := generateDocument()
	mu.Lock()
	defer mu.Unlock()

	*documents = append(*documents, docObj)
	wg.Done()
}

func getDocuments(batchSize int) []document {
	var documents []document
	var wg sync.WaitGroup

	for i := 0; i < batchSize; i++ {
		wg.Add(1)
		go buildVectors(&documents, &wg)
	}
	wg.Wait()

	return documents
}
