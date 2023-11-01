package main

import (
	"encoding/csv"
	"github.com/google/uuid"
	"log"
	"math/rand"
	"os"
	"time"
)

type UserEntry struct {
	Key   string
	Value string
	Meta  map[string]string
}

const (
	PathDataset1KB = "/home/roma/dev/itmo/tahbd/spark-lab-2/orc-parquet/datasets/ds1kb.csv"
	PathDataset1MB = "/home/roma/dev/itmo/tahbd/spark-lab-2/orc-parquet/datasets/ds1mb.csv"
	PathDataset1GB = "/home/roma/dev/itmo/tahbd/spark-lab-2/orc-parquet/datasets/ds1gb.csv"
	PathDataset2GB = "/home/roma/dev/itmo/tahbd/spark-lab-2/orc-parquet/datasets/ds2gb.csv"
	PathDataset3GB = "/home/roma/dev/itmo/tahbd/spark-lab-2/orc-parquet/datasets/ds3gb.csv"
)

func main() {
	// for 1 KB
	entries := genDatasetEntries(14)
	convertEntriesToCSV(entries, PathDataset1KB)

	entries = genDatasetEntries(14000)
	convertEntriesToCSV(entries, PathDataset1MB)

	entries = genDatasetEntries(14000000)
	convertEntriesToCSV(entries, PathDataset1GB)

	entries = genDatasetEntries(28000000)
	convertEntriesToCSV(entries, PathDataset2GB)

	entries = genDatasetEntries(42000000)
	convertEntriesToCSV(entries, PathDataset3GB)
}

func genDatasetEntries(cap int) []UserEntry {
	entries := make([]UserEntry, cap)

	for i := range entries {
		entries[i] = UserEntry{
			Key:   uuid.New().String(),
			Value: "TestJustValue",
			Meta: map[string]string{
				"Tank": "TestValueTank",
				"Comp": "TestValueComp",
			},
		}
	}

	return entries
}

func convertEntriesToCSV(entries []UserEntry, path string) {
	// Создание CSV файла
	file, err := os.Create(path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Создание писателя CSV
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Запись заголовков
	headers := []string{"Key", "Value"}
	for key := range entries[0].Meta {
		headers = append(headers, key)
	}
	if err := writer.Write(headers); err != nil {
		log.Fatalln("Error writing headers to csv:", err)
	}

	// Запись данных для каждого пользователя
	for _, entry := range entries {
		row := []string{entry.Key, entry.Value}
		for _, value := range entry.Meta {
			row = append(row, value)
		}
		if err := writer.Write(row); err != nil {
			log.Fatalln("Error writing record to csv:", err)
		}
	}
}

func generateRandomWord() string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyz")

	rand.Seed(time.Now().UnixNano())

	wordLength := rand.Intn(5) + 6

	word := make([]rune, wordLength)
	for i := range word {
		word[i] = letters[rand.Intn(len(letters))]
	}

	return string(word)
}
