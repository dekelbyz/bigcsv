package bigcsv

type Table [][]string

type Operation interface {
	Execute(input Table) (Table, error)
}

