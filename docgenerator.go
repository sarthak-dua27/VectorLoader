package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/jaswdr/faker"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sync"
)

type embeddings struct {
	Vectors []float32 `json:"embeddings"`
	Dim     int       `json:"dim"`
}

type document struct {
	ID           string
	Car          string
	Model        string
	Fuel         string
	Type         string
	Rating       int
	Year         int
	Availability bool
	Transmission string
	Price        int
	Description  string
	Color        string
	RGB          [3]uint8 `json:"color_rgb_vector"`
	Hex          string   `json:"color_hex"`
	//Dim          int 	`json:"description_vector_dim"`
	//Vector       []float32 	`json:"description_vector"`
}
type Store struct {
	ID            string
	StoreName     string
	AvailableCars []string
	Address       string
	Contact       string
}

type returnType struct {
	CarDocument   *[]document
	StoreDocument *[]Store
}

type Color struct {
	Name string `json:"color"`
	Hex  string `json:"hex"`
}

type Colors struct {
	Colors []Color `json:"colors"`
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

func generateCarDocument() document {

	var goFakeIt = gofakeit.New(0)

	var doc document
	doc.ID = generateRandomID(10)
	doc.Car = get_car(1)[0]
	doc.Model = goFakeIt.Car().Model
	doc.Fuel = goFakeIt.Car().Fuel
	doc.Rating = rand.Intn(6)
	doc.Type = goFakeIt.Car().Type
	doc.Year = goFakeIt.Car().Year
	doc.Availability = rand.Intn(1) == 1
	doc.Transmission = goFakeIt.Car().Transmission
	doc.Description = fmt.Sprintf("This car is a %s on %s Transmission. Manifactured in %d year this is one of the best car to drive", doc.Car, doc.Transmission, doc.Year)
	doc.Price = rand.Intn(10000) + 100000

	data, e := ioutil.ReadFile("./colors.json")
	if e != nil {
		return doc
	}
	var colorsobj Colors
	e = json.Unmarshal(data, &colorsobj)
	if e != nil {
		fmt.Printf("Error unmarshaling json data %v", e)
	}

	randomIndex := rand.Intn(len(colorsobj.Colors))
	randomColor := colorsobj.Colors[randomIndex]

	doc.Color = randomColor.Name
	doc.Hex = randomColor.Hex
	doc.RGB, e = get_rgb_from_hex(doc.Hex)
	if e != nil {
		fmt.Printf("Error retrieving RGB embeddings %v\n", e)
	}
	//var vec []float32
	//var dim int
	//var err error
	//for retry := 0; retry < 5; retry++ {
	//	vec, dim, err = fetchEmbeddings(doc.Description)
	//	if err != nil {
	//		time.Sleep(2 * time.Second)
	//	} else {
	//		err = nil
	//		break
	//	}
	//}
	//if err != nil {
	//	fmt.Printf("Error retrieving vector embeddings %v\n", err)
	//}
	//
	//doc.Vector = vec
	//doc.Dim = dim
	return doc
}

func generateStoreDocument() Store {
	fake := faker.New()
	store := Store{}
	store.ID = generateRandomID(10)
	store.StoreName = fake.Company().Name() + " " + fake.Company().Suffix()
	store.AvailableCars = get_car(rand.Intn(5))
	store.Address = fake.Address().SecondaryAddress() + ", " + fake.Address().City() + ", " + fake.Address().PostCode()
	store.Contact = fake.Person().Contact().Phone

	return store
}

func buildVectors(documents *[]document, storedocument *[]Store, wg *sync.WaitGroup, dataset string) {

	mu.Lock()
	defer mu.Unlock()

	if dataset == "car" {
		*documents = append(*documents, generateCarDocument())
	} else {
		*storedocument = append(*storedocument, generateStoreDocument())
	}

	wg.Done()
}

func getDocuments(batchSize int, dataset string) returnType {

	var documents []document
	var storeDocuments []Store
	returnObj := returnType{&documents, &storeDocuments}

	var wg sync.WaitGroup

	for i := 0; i < batchSize; i++ {
		wg.Add(1)
		go buildVectors(&documents, &storeDocuments, &wg, dataset)
	}
	wg.Wait()

	return returnObj
}

func generateRandomID(length int) string {
	randomBytes := make([]byte, length)
	_, err := rand.Read(randomBytes)
	if err != nil {
		panic(err)
	}
	return base64.URLEncoding.EncodeToString(randomBytes)[:length]
}
