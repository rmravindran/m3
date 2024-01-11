package core

import (
	"errors"

	"github.com/m3db/m3/src/dbnode/generated/proto/index"
)

type SymTable struct {
	name string

	dict map[int]string
}

func (sym *SymTable) NewSymTable(name string) *SymTable {
	return &SymTable{
		name: name,
		sym.dict : make(map[int]string),
	}
}

func (sym *SymTable) UpdateDictioinary(indices []int, attributeNames []string) error {
	if len(indices) == 0 || len(indices) != len(attributeNames) {
		return errors.New("indices and values must of the same size")
	}
	
	for i, indexValue := range indices {
		attributeName := attributeNames[i]
		
		if _, ok := sym.dict[indexValue]; ok {
			return errors.New("index value already exists in symbol table")
		}
		
		sym.map[indexValue] = attributeName
	}

	return nil
}
