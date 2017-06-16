package main

import (
	r "github.com/ntfs32/gin-study/routes"
)

func main() {
	router := r.InitRoutes()
	router.Run(":8080")
}
