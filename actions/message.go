package actions

import (
	"github.com/gin-gonic/gin"
)

func Message(c *gin.Context) {

	c.JSON(200, gin.H{"name": 12})

}
