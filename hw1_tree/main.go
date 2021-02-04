package main

import (
	"fmt"
	"io"
	"os"
	"sort"
)

func main() {
	out := os.Stdout
	if !(len(os.Args) == 2 || len(os.Args) == 3) {
		panic("usage go run main.go . [-f]")
	}
	path := os.Args[1]
	printFiles := len(os.Args) == 3 && os.Args[2] == "-f"
	err := dirTree(out, path, printFiles)
	if err != nil {
		panic(err.Error())
	}
}

func dirTree(out io.Writer, path string, printFiles bool) error {
	return recTree(out, path, printFiles, "")
}

func recTree(out io.Writer, path string, printFiles bool, parentPrefix string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}

	fileInfos, err := file.Readdir(0)
	if err != nil {
		return err
	}

	fileInfos = sortAndFilter(fileInfos, printFiles)
	for index, fileInfo := range fileInfos {
		fileSymbol := "├───"
		currentPrefix := "│\t"
		if index == len(fileInfos)-1 {
			fileSymbol = "└───"
			currentPrefix = "\t"
		}

		fmt.Fprintln(out, formatName(fileInfo, parentPrefix+fileSymbol))

		if fileInfo.IsDir() {
			if err := recTree(
				out,
				path+string(os.PathSeparator)+fileInfo.Name(),
				printFiles,
				parentPrefix+currentPrefix,
			); err != nil {
				return err
			}
		}
	}

	return nil
}

func formatName(fInfo os.FileInfo, prefix string) string {
	var postfix string

	if !fInfo.IsDir() {
		postfix = " (empty)"
		if fInfo.Size() > 0 {
			postfix = fmt.Sprintf(" (%db)", fInfo.Size())
		}
	}

	return prefix + fInfo.Name() + postfix
}

func sortAndFilter(fileInfos []os.FileInfo, printFiles bool) []os.FileInfo {
	sort.SliceStable(
		fileInfos,
		func(i, j int) bool { return fileInfos[i].Name() < fileInfos[j].Name() },
	)

	n := len(fileInfos)
	if !printFiles {
		n = 0
		for _, fInfo := range fileInfos {
			if fInfo.IsDir() {
				fileInfos[n] = fInfo
				n++
			}
		}
	}

	return fileInfos[:n]
}
