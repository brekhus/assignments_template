package cos418_hw1_1

import (
	"bufio"
	"io"
	"os"
	"strconv"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	var sum int
	for v := range nums {
		sum += v
	}
	out <- sum
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	nums := make(chan int)
	out := make(chan int)
	for i := 0; i < num; i++ {
		go sumWorker(nums, out)
	}
	f, err := os.Open(fileName)
	checkError(err)
	defer f.Close()
	vals, err := readInts(f)
	checkError(err)
	for _, v := range vals {
		nums <- v
	}
	close(nums)
	var sum int
	for i := 0; i < num; i++ {
		sum += <-out
	}
	// TODO: implement me
	// HINT: use `readInts` and `sumWorkers`
	// HINT: used buffered channels for splitting numbers between workers
	return sum
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		text := scanner.Text()
		val, err := strconv.Atoi(text)
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
