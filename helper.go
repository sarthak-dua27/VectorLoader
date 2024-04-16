package main

import (
	"fmt"
	"github.com/crazy3lf/colorconv"
	"math/rand"
)

func get_rgb_from_hex(hex string) ([3]uint8, error) {
	r, g, b, err := colorconv.HexToRGB(hex)
	if err != nil {
		fmt.Printf("Error generating RGB embedding %v", err)
		return [3]uint8{}, err
	}
	return [3]uint8{r, g, b}, nil
}

var cars = []string{"Bentley", "Rover", "Maserati", "Porsche", "Audi", "Bentley", "Suzuki", "Porsche", "Chevrolet", "Porsche", "Volvo", "Ferrari", "Hyundai", "Rolls-Royce", "Citroen", "De Lorean", "Saab", "McLaren", "Renault", "Fiat", "Ford", "Daihatsu", "Honda"}

// to avoid race condition either put a lock here or
//func get_car(num int) []string {
//	rand.Shuffle(len(cars), func(i, j int) {
//		cars[i], cars[j] = cars[j], cars[i]
//	})
//	return cars[:num]
//}

func get_car(num int) []string {
	carsCopy := make([]string, len(cars))
	copy(carsCopy, cars)

	rand.Shuffle(len(carsCopy), func(i, j int) {
		carsCopy[i], carsCopy[j] = carsCopy[j], carsCopy[i]
	})
	if num > len(carsCopy) {
		num = len(carsCopy)
	}
	return carsCopy[:num]
}
