//
// DoMap.go
//
// This file contains functionality for a 'map' worker.
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
	"hash/fnv"
	"io/fs"
	"os"
)

//
// doMap
//
// This function does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function for that file's contents
// (see mapFunc in main/wc.go), and partitions the output into nReduce intermediate files.
//
// 		jobName       - the name of the MapReduce job
//      mapTaskNumber - the unique number assigned to this Map task
//      inFile        - the name of the input file
//      nReduce       - the number of Reduce tasks that will be run
//      mapFunc		  - the user-defined Map function
//
func doMap(
	jobName       string,
	mapTaskNumber int,
	inFile        string,
	nReduce       int,
	mapFunc       func(file string, contents string) []KeyValue,
) {
	var status int   = 0
	var err    error = nil

	//
	// Open and read the contents of the file:
	//
	var content string
	
	if status == 0 {
		contentBytes, tempErr := os.ReadFile(inFile)

		if tempErr != nil {
			// Error reading file
			status = -1
			err    = tempErr
		} else {
			content = string(contentBytes)
		}
	}

	//
	// Contruct KeyValue pairs from file content, and encode them to JSON in partitions:
	//
	encodingStrings := make([]string, nReduce)

	if status == 0 {
		keyValues := mapFunc(inFile, content)

		//
		// Create JSON encoder for each new Reduce file:
		//
		buffers  := make([]*bytes.Buffer, nReduce)
		encoders := make([]*json.Encoder, nReduce)

		for i := 0; i < len(encoders); i++ {
			buffers[i]   = new(bytes.Buffer)
			encoders[i] = json.NewEncoder(buffers[i])
		}

		//
		// For each KeyValue pair, determine respective encoder and encode.
		//
		var encIndex uint32
		var tempErr  error

		for _, kv := range keyValues {
			encIndex = ihash(kv.Key) % uint32(nReduce) // Why not use round robin?
			tempErr  = encoders[encIndex].Encode(&kv)

			if tempErr != nil {
				// Error encoding KeyValue
				status = -1
				err    = tempErr
				break
			}
		}

		if status == 0 {
			for i := 0; i < len(encodingStrings); i++ {
				encodingStrings[i] = buffers[i].String()
			}
		}
	}

	//
	// Creates Reduce files to store encodings:
	//
	var outFiles[] *os.File

	if status == 0 {
		outFiles = make([]*os.File, len(encodingStrings))

		var tempErr error

		for i := 0; i < len(encodingStrings); i++ {
			fileName := reduceName(jobName, mapTaskNumber, i)

			//
			// Remove file if it already exists:
			// *NOTE* Currently not treating this as an error
			//
			_, tempErr = os.Stat(fileName)

			if tempErr == nil {
				// No error: file exists
				tempErr = os.Remove(fileName)

				if tempErr != nil {
					// Error removing file
					status = -1
					err    = tempErr
					break
				}
			} else if !errors.Is(tempErr, fs.ErrNotExist) {
				// Unexpected error acquiring file stats
				status = -1
				err = tempErr
				break
			}

			//
			// Create new file and write:
			//
			if status == 0 {
				outFiles[i], tempErr = os.Create(fileName)

				if tempErr != nil {
					// Error creating file
					status = -1
					err    = tempErr
					break
				}

				outFiles[i].WriteString(encodingStrings[i])
				outFiles[i].Close()
			}
		}
	}

	//
	// Handle any error, and return:
	//
	if status != 0 {
		//
		// Remove any created intermediate file:
		//
		for i := 0; i < len(outFiles); i++ {
			if outFiles[i] != nil {
				fileInfo, tempErr := outFiles[i].Stat()

				if tempErr != nil {
					// Error acquiring file stats
					status = -1
					err    = tempErr
				} else {
					outFiles[i].Close()
					tempErr = os.Remove(fileInfo.Name())

					if tempErr != nil {
						// Error removing file
						status = -1
						err    = tempErr
					}
				}
			}
		} 

		fmt.Printf("Function error [DoMap.doMap]: %s\n", err.Error())
	}
}

//
// ihash
//
// Hashes a given string to a 32-bit integer value.
//
// 		s - the string value to be hashed
//
// Returns the 32-bit integer hash value.
//
func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
