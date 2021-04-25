package cache

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

type FieldType int

const (
	FieldTypeUnknown = iota
	FieldTypeString
	FieldTypeInteger
	FieldTypeFloat
)

func fieldTypeString(t FieldType) string {
	switch t {
	case FieldTypeString:
		return "FIELD_TYPE_STRING"
	case FieldTypeInteger:
		return "FIELD_TYPE_INTEGER"
	case FieldTypeFloat:
		return "FIELD_TYPE_FLOAT"
	}

	return "FIELD_TYPE_UNKNOWN"
}


type DataSetCache struct {
	FieldNames []string
	FieldTypes []FieldType
	FieldData [][]string
}

func New() *DataSetCache {
	var c DataSetCache

	// This allocates the top level slice; not the inner slices. They are allocated as we read in the data.
	c.FieldData = make([][]string, 0, 1000)

	return &c
}

func (c *DataSetCache) Import(URI string) error {
	log.Println("importing", URI)

	file, err := os.Open(URI)
	if err != nil {
		return fmt.Errorf("could not open file %v: %w", URI, err)
	}

	reader := bufio.NewReader(file)
	csvReader := csv.NewReader(reader)

	// Read the header of the file first
	record, err := csvReader.Read()
	if err == io.EOF {
		return fmt.Errorf("unexpected end of file while reading CSV file header from %v", URI)
	}
	if err != nil {
		return fmt.Errorf("could not parse CSV file %v: %w", URI, err)
	}
	c.FieldNames = record

	// Initialize FieldTypes to integer first; if we see a . in the data it switches to float
	// if we see anything else it switches to string.

	c.FieldTypes = make([]FieldType, len(c.FieldNames))
	for fieldIndex := range c.FieldNames {
		c.FieldTypes[fieldIndex] = FieldTypeInteger
	}

	// When we first read the dataset, we don't know what the types are initially, so we store them as Strings as
	// we read them into memory. As we read the data we adjust the type stored in the cache for that field until
	// we have read all the rows. If all we see are integers, then the field will be an integer field, and so on.

	for {
		record, err = csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("could not parse CSV file %v: %w", URI, err)
		}

		fieldData := make([]string, len(record)) // allocates a slice of strings that we'll store the CSV data in
		for column, stringValue := range record {
			switch c.FieldTypes[column] {
			case FieldTypeUnknown:
				// See what this field parsed as and use that as a starting point.
				c.FieldTypes[column] = inferProbableTypeFromString(&stringValue)
			case FieldTypeString:
				// if this column is already defined as a string, we just take whatever was passed in.
				// String is the most permissive type; we can't change this column to an integer or a float because
				// there have been values unable to be parsed as such already seen for this column.
				//
				// This case is left empty for readability.
			case FieldTypeInteger:
				// if it's an integer, it stays an integer; if it's a float or a string
				// then we change to that.
				c.FieldTypes[column] = inferProbableTypeFromString(&stringValue)
			case FieldTypeFloat:
				// If the field is a float and the current row has a value that can be parsed as an integer, the
				// column stays a float. However, if it is a string, then we change the column to a string. So,
				// we only care if this is parsed as a string. The other results don't change the
				if inferProbableTypeFromString(&stringValue) == FieldTypeString {
					c.FieldTypes[column] = FieldTypeString
				}
			}
			fieldData[column] = stringValue // store the column with the string value.
		}
		c.FieldData = append(c.FieldData, fieldData)
	}

	log.Printf("finished processing CSV, %v rows processed", len(c.FieldData))


	log.Println("fields: ")
	for i, v := range c.FieldNames {
		log.Printf("   %v: %v", v, fieldTypeString(c.FieldTypes[i]))
	}

	return nil
}

// This function tries to figure out what the passed in string could be converted to; the intent is that for
// some columns that only contains integers we will want to set the column type to integer. This will allow
// us to query on integer values using some math.
//
// We take a reference to a string to avoid unnecessary allocations here. Hopefully the compiler is
// smart enough to inline this code? Probably not.
func inferProbableTypeFromString(s *string) FieldType {
	// Try parsing as an Integer
	_, err := strconv.ParseInt(*s, 10, 64)
	if err != nil {
		// That didn't work, try parsing as a Float
		_, err = strconv.ParseFloat(*s, 10)
		if err != nil {
			// Give up; treat it as a string.
			return FieldTypeString
		} else {
			// parsed successfully as a float
			return FieldTypeFloat
		}
	} else {
		// This parsed as an integer.
		return FieldTypeInteger
	}
}