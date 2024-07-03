package bigcsv

import "strconv"

type FilterEvenAges struct{}

func (f FilterEvenAges) Execute(input [][]string) ([][]string, error) {
	var result [][]string
	for _, row := range input {
		if len(row) >= 5 {
			age, err := strconv.Atoi(row[4])
			if err == nil && age%2 == 0 {
				result = append(result, row)
			}
		}
	}
	return result, nil
}
