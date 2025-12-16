package main

import (
	"fmt"

	"github.com/yanyiwu/gojieba"
)

func main() {
	j := gojieba.NewJieba()
	defer j.Free()

	// article-004 的内容
	text := "React、Vue 和 Angular 是目前最流行的前端框架。本文将从性能、学习曲线和生态系统等方面进行详细对比。"
	words := j.Cut(text, true)
	fmt.Println("Article-004 Cut:", words)

	// 检查是否包含"系统"这个词
	for _, word := range words {
		if word == "系统" {
			fmt.Println("Found '系统' in article-004")
		}
	}

	// article-006 的内容
	text2 := "Rust 系统编程入门 Rust 系统编程入门 Rust 是一种系统编程语言，专注于安全性、速度和并发性。它通过所有权系统实现内存安全。 周七"
	words2 := j.Cut(text2, true)
	fmt.Println("\nArticle-006 Cut:", words2)

	// 检查是否包含"系统"这个词
	for _, word := range words2 {
		if word == "系统" {
			fmt.Println("Found '系统' in article-006")
		}
	}
}
