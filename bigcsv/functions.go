package bigcsv

import (
	"math"
	"strconv"
)

type FilterRows struct {
	Condition func(record []string) bool
}

func (fr FilterRows) Execute(input Table) (Table, error) {
	var result Table
	for _, row := range input {
		if fr.Condition(row) {
			result = append(result, row)
		}
	}
	return result, nil
}

type GetColumns struct {
	ColumnIndices []int
}

func (gc GetColumns) Execute(input Table) (Table, error) {
	var result Table
	for _, row := range input {
		newRow := make([]string, 0, len(gc.ColumnIndices))
		for _, index := range gc.ColumnIndices {
			if index < len(row) {
				newRow = append(newRow, row[index])
			} else {
				// You might want to handle this case differently depending on your requirements
				newRow = append(newRow, "")
			}
		}
		result = append(result, newRow)
	}
	return result, nil
}

type Ceil struct {
	ColumnIndices []int
}

func (c Ceil) Execute(input Table) (Table, error) {
	result := make(Table, len(input))
	for i, row := range input {
		newRow := make([]string, len(row))
		copy(newRow, row)
		for _, index := range c.ColumnIndices {
			if index < len(row) {
				if val, err := strconv.ParseFloat(row[index], 64); err == nil {
					newRow[index] = strconv.FormatFloat(math.Ceil(val), 'f', 0, 64)
				}
			}
		}
		result[i] = newRow
	}
	return result, nil
}
