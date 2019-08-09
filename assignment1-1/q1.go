package cos418_hw1_1

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
)

// Find the top K most common words in a text document.
// 	path: location of the document
//	numWords: number of words to return (i.e. k)
//	charThreshold: character threshold for whether a token qualifies as a word,
//		e.g. charThreshold = 5 means "apple" is a word but "pear" is not.
// Matching is case insensitive, e.g. "Orange" and "orange" is considered the same word.
// A word comprises alphanumeric characters only. All punctuations and other characters
// are removed, e.g. "don't" becomes "dont".
// You should use `checkError` to handle potential errors.
func topWords(path string, numWords int, charThreshold int) []WordCount {
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	buf := bufio.NewReader(f)
	pat, err := regexp.Compile("[^0-9a-zA-Z]")
	checkError(err)

	wordCounts := make(map[string]int)
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			break
		}
		words := strings.Fields(line)
		for _, word := range words {
			word = strings.ToLower(word)
			word = pat.ReplaceAllLiteralString(word, "")
			if len(word) >= charThreshold {
				wordCounts[word]++
			}
		}
	}
	if err != io.EOF {
		checkError(err)
	}
	var wcs []WordCount
	for k, v := range wordCounts {
		wc := WordCount{Word: k, Count: v}
		wcs = append(wcs, wc)
	}

	sortWordCounts(wcs)
	last := numWords
	if len(wcs) < numWords {
		last = len(wcs)
	}
	return wcs[:last]
}

// A struct that represents how many times a word is observed in a document
type WordCount struct {
	Word  string
	Count int
}

func (wc WordCount) String() string {
	return fmt.Sprintf("%v: %v", wc.Word, wc.Count)
}

// Helper function to sort a list of word counts in place.
// This sorts by the count in decreasing order, breaking ties using the word.
// DO NOT MODIFY THIS FUNCTION!
func sortWordCounts(wordCounts []WordCount) {
	sort.Slice(wordCounts, func(i, j int) bool {
		wc1 := wordCounts[i]
		wc2 := wordCounts[j]
		if wc1.Count == wc2.Count {
			return wc1.Word < wc2.Word
		}
		return wc1.Count > wc2.Count
	})
}
