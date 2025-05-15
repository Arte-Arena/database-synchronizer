package main

import (
	"database_sync/source/utils"
	"fmt"
	"log"
	"sync"
	"time"
)

func main() {
	utils.LoadEnvVariables()

	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := SyncUsers(); err != nil {
			log.Printf("Error synchronizing users: %v", err)
		} else {
			fmt.Println("Initial user synchronization completed successfully")
		}
	}()

	go func() {
		defer wg.Done()
		if err := SyncLeads(); err != nil {
			log.Printf("Error synchronizing leads: %v", err)
		} else {
			fmt.Println("Initial leads synchronization completed successfully")
		}
	}()

	wg.Wait()

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		wg.Add(2)

		go func() {
			defer wg.Done()
			fmt.Println("Running scheduled user synchronization...")
			if err := SyncUsers(); err != nil {
				log.Printf("Error synchronizing users: %v", err)
			} else {
				fmt.Println("User synchronization completed successfully")
			}
		}()

		go func() {
			defer wg.Done()
			fmt.Println("Running scheduled leads synchronization...")
			if err := SyncLeads(); err != nil {
				log.Printf("Error synchronizing leads: %v", err)
			} else {
				fmt.Println("Leads synchronization completed successfully")
			}
		}()

		wg.Wait()
	}
}
