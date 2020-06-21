package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
)

func main() {
	re := regexp.MustCompile(`(?i)(\B|^)(anoda|comot|dem|dey|di|dia|don\s|doti|everitin|im|na|pickin|pickins|pikin|pesin|waka|wetin|wen|wan|wella|wey|wuna|sey)(\B|$)`)
	buf := bufio.NewReader(os.Stdin)
	for line, err := buf.ReadString('\n'); err == nil; line, err = buf.ReadString('\n') {
		if re.MatchString(line) {
			fmt.Println("__label__naija 0.95")
		} else {
			fmt.Println("__label__naija 0.38")
		}
	}
}
