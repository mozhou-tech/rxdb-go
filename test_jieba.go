package main

import (
	"fmt"

	"github.com/yanyiwu/gojieba"
)

func main() {
	j := gojieba.NewJieba()
	defer j.Free()

	text := "React、Vue 和 Angular 是目前最流行的前端框架。本文将从性能、学习曲线和生态系统等方面进行详细对比。"
	words := j.Cut(text, true)
	fmt.Println("Cut:", words)

	words2 := j.CutForSearch(text, true)
	fmt.Println("CutForSearch:", words2)

	query := "系统"
	queryWords := j.Cut(query, true)
	fmt.Println("Query '系统' Cut:", queryWords)
}
