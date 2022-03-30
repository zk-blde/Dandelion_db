package main

import (
	"net/http"
	"orchid_db/internal"
	"orchid_db/mapEngine"

	"github.com/gin-gonic/gin"
)


func main()  {
	r := gin.Default()
	var internalMap mapEngine.BaseEngine
	internalMap=&mapEngine.InnerDBOperation{
		Db: map[string]string{},
	}
	db:=internal.OrchidDB{
		DirPath: internal.DirPath{
			Dirpath: "./test",
		},

		InternalMap: internalMap,
	}





	r.POST("/set",func(c *gin.Context){
		msg := db.Set(c.Query("key") ,c.Query("value") )
		db.Ctx_object()
		c.JSON(http.StatusOK, gin.H{
			"msg":msg,
		})
	})
	r.GET("/get", func(c *gin.Context){
		value := db.Get(c.Query("key"))
		c.JSON(http.StatusOK, gin.H{
			"msg":value,
		})
	})
	r.PUT("/put", func(c *gin.Context){
		ok := db.Put(c.Query("key"), c.Query("key"))
		db.Ctx_object()
		if ok {
			c.JSON(http.StatusOK, gin.H{
				"msg":ok,
			})
		}else{
			c.JSON(http.StatusOK, gin.H{
				"msg":ok,
			})
		}

	})
	r.DELETE("/delete", func(c *gin.Context){
		ok := db.Delete(c.Query("key"))
		db.Ctx_object()
		if ok {
			c.JSON(http.StatusOK, gin.H{
				"msg":ok,
			})
		}else{
			c.JSON(http.StatusOK, gin.H{
				"msg":ok,
			})
		}
	})
	r.Run(":8080")

}