package core

// Symbol Table Instructions supported by the boost implementation
type TableInstruction int

const (
	// Initiatize the table
	InitSymTable TableInstruction = iota

	// Update the table
	UpdateSymTable

	// Add a new attribute to the table
	AddAttribute

	// End Dictionary
	EndSymTable

	// NOP
	NOPInstruction
)
