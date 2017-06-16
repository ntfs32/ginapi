package routes

import (
	Action "github.com/ntfs32/gin-study/actions"

	"github.com/gin-gonic/gin"
)

func InitRoutes() *gin.Engine {
	route := gin.Default()
	route.GET("/", Action.Root)
	route.GET("/name", Action.Name)
	route.POST("/name", Action.Name)
	return route
}
