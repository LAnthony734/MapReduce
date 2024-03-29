//
// DoReduce.go
//
// This file contains functionality for a 'reduce' worker.
//
// The MIT License (MIT)
//
// Copyright (c) 2023 Luke Andrews.  All Rights Reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this
// software and associated documentation files (the "Software"), to deal in the Software
// without restriction, including without limitation the rights to use, copy, modify, merge,
// publish, distribute, sub-license, and/or sell copies of the Software, and to permit persons
// to whom the Software is furnished to do so, subject to the following conditions:
//
// * The above copyright notice and this permission notice shall be included in all copies or
// substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, 
// INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
// PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
// FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR 
// OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.
//
import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
)

//
// doReduce
//
// This function does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
//
// 		jobName          - the name of the MapReduce job
//      reduceTaskNumber - the unique number assigned to this Reduce task
//      nMap			 - the number of Map tasks that were run
//      reduceFunc       - the user-defined Reduce function
//
func doReduce(
	jobName          string,
	reduceTaskNumber int,
	nMap             int,
	reduceFunc       func(key string, values []string) string,
) {
	var status int   = 0
	var err    error = nil

	//
	// Decode files:
	//
	var keyValues []KeyValue = nil

	if status == 0 {
		for i := 0; i < nMap; i++ {
			fileName := reduceName(jobName, i, reduceTaskNumber)

			_, tempErr := os.Stat(fileName)

			if tempErr != nil {
				if errors.Is(tempErr, fs.ErrNotExist) {
					// File does not exist
					// *NOTE* Currently not treating this as an error
				} else {
					// Some other error acquiring file stats			
					status = -1
					err    = tempErr
					break
				}
			} else {
				// No error: file exists
				var file* os.File = nil

				file, tempErr = os.Open(fileName)

				if tempErr != nil {
					// Error opening file
					status = -1
					err    = tempErr
					break
				}
	
				decoder := json.NewDecoder(file)

				var tempKV KeyValue

				for decoder.More() {
					tempErr = decoder.Decode(&tempKV)

					if tempErr != nil {
						// Error decoding
						status = -1
						err    = tempErr
						file.Close()	
						break
					}

					keyValues = append(keyValues, tempKV)
				}

				file.Close()
			}
		}
	}

	//
	// Create key-to-values map for Reduce function input:
	//
	var keyValuesMap map[string][]string = nil

	if status == 0 {
		keyValuesMap = make(map[string][]string)

		for _, kv := range keyValues {
			keyValuesMap[kv.Key] = append(keyValuesMap[kv.Key], kv.Value)
		}
	}

	//
	// Create new KeyValue array with Reduce function results:
	//
	var newKeyValues []KeyValue = nil

	if status == 0 {
		var newValue string = ""

		for key, value := range keyValuesMap {
			
			newValue = reduceFunc(key, value)

			if newValue == "error" {
				status = -1
				err    = errors.New("Reduce Function Error")
				break
			}

			newKeyValues = append(newKeyValues, KeyValue{key, newValue})
		}
	}

	//
	// Encode to JSON:
	//
	var encodingString string = ""

	if status == 0 {
		buffer  := new(bytes.Buffer)
		encoder := json.NewEncoder(buffer)

		var tempErr error

		for _, kv := range newKeyValues {
			tempErr = encoder.Encode(&kv)

			if tempErr != nil {
				// Error encoding
				status = -1
				err    = tempErr
				break
			}
		}

		if status == 0 {
			encodingString = buffer.String()
		}
	}

	//
	// Create and write encoding to new Merge file:
	//
	var outFile *os.File = nil

	if status == 0 {
		fileName := mergeName(jobName, reduceTaskNumber)
		
		//
		// Remove file if it already exists:
		// *NOTE* Currently not treating this as an error
		//
		_, tempErr := os.Stat(fileName)

		if tempErr == nil {
			// No error: file exists
			tempErr = os.Remove(fileName)

			if tempErr != nil {
				// Error removing file
				status = -1
				err    = tempErr
			}
		} else if !errors.Is(tempErr, fs.ErrNotExist) {
			// Unexpected error acquiring file stats
			status = -1
			err    = tempErr
		}

		//
		// Create new file and write:
		//
		if status == 0 {
			outFile, tempErr = os.Create(fileName)

			if tempErr != nil {
				// Error creating file
				status = -1
				err    = tempErr
			} else {
				outFile.WriteString(encodingString)
				outFile.Close()
			}
		}
	}

	//
	// Handle any error, and return:
	//
	if status != 0 {
		//
		// Remove intermediate file if created:
		//
		if outFile != nil {
			fileInfo, tempErr := outFile.Stat()

			if tempErr != nil {
				// Error acquiring file stats
				status = -1
				err    = tempErr
			} else {
				outFile.Close()
				tempErr = os.Remove(fileInfo.Name())

				if tempErr != nil {
					// Error removing file
					status = -1
					err    = tempErr
				}
			}
		}

		fmt.Printf("Function error [DoReduce.doReduce]: %s\n", err.Error())
	}
}