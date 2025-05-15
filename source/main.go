package main

import (
	"database_sync/source/utils"
	"fmt"
	"log"
	"time"
)

func main() {
	utils.LoadEnvVariables()

	if err := SyncUsers(); err != nil {
		log.Printf("Error synchronizing users: %v", err)
	} else {
		fmt.Println("Initial user synchronization completed successfully")
	}

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		fmt.Println("Running scheduled user synchronization...")
		if err := SyncUsers(); err != nil {
			log.Printf("Error synchronizing users: %v", err)
		} else {
			fmt.Println("User synchronization completed successfully")
		}
	}
}
